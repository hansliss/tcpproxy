#include<signal.h>
#include<stdio.h>
#include<stdarg.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<netinet/in.h>
#include<string.h>
#include<sys/types.h>
#include<stdlib.h>
#include<unistd.h>
#include<errno.h>
#include<netdb.h>
#include<fcntl.h>
#include<time.h>
#include<sys/wait.h>
#include<limits.h>
#include<getopt.h>

#include "daemon_common.h"
#include "observer.h"

#define CONTEXT_LENGTH 256

/*
 * Error logging is routed through a global context string so we can tag every
 * message with the component that triggered it (listener, connection, etc.).
 */
static char current_log_context[CONTEXT_LENGTH] = "proxy";

static void handle_exit_signal(int signo);

struct proxy_config {
  char *local_address;
  char *remote_address;
  char *logdir;
  char *pidfile_path;
  char *observer_config;
  char *config_path;
};

static void proxy_set_string(char **dest, const char *value) {
  if (!dest) {
    return;
  }
  free(*dest);
  *dest = value ? daemon_dup_string(value) : NULL;
}

static void normalise_logdir(char *path) {
  if (!path) {
    return;
  }
  size_t len = strlen(path);
  while (len > 1 && path[len - 1] == '/') {
    path[--len] = '\0';
  }
}

static int apply_config_key(struct proxy_config *cfg, const char *key, const char *value) {
  if (!cfg || !key) {
    return -1;
  }
  if (!value) {
    value = "";
  }
  if (strcmp(key, "local") == 0 || strcmp(key, "local-address") == 0) {
    proxy_set_string(&cfg->local_address, value);
  } else if (strcmp(key, "remote") == 0 || strcmp(key, "remote-address") == 0) {
    proxy_set_string(&cfg->remote_address, value);
  } else if (strcmp(key, "log-dir") == 0 || strcmp(key, "logdir") == 0) {
    proxy_set_string(&cfg->logdir, value);
    normalise_logdir(cfg->logdir);
  } else if (strcmp(key, "pidfile") == 0 || strcmp(key, "pid-file") == 0) {
    proxy_set_string(&cfg->pidfile_path, value);
  } else if (strcmp(key, "observer") == 0 || strcmp(key, "observer-config") == 0) {
    proxy_set_string(&cfg->observer_config, value);
  } else {
    return -1;
  }
  return 0;
}

static void free_proxy_config(struct proxy_config *cfg) {
  if (!cfg) {
    return;
  }
  free(cfg->local_address);
  free(cfg->remote_address);
  free(cfg->logdir);
  free(cfg->pidfile_path);
  free(cfg->observer_config);
  free(cfg->config_path);
  memset(cfg, 0, sizeof(*cfg));
}

static void config_log_message(int level, int configured_level, const char *fmt, ...) {
  (void)level;
  (void)configured_level;
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
  fputc('\n', stderr);
}

static int parse_arguments(int argc, char **argv, struct proxy_config *cfg);

static void log_set_context(const char *context) {
  if (!context || !*context) {
    snprintf(current_log_context, sizeof(current_log_context), "%s", "proxy");
    return;
  }
  snprintf(current_log_context, sizeof(current_log_context), "%s", context);
}

static const char *log_get_context(void) {
  return current_log_context;
}

#define ERR_SOCKFAIL -1
#define ERR_BINDFAIL -2
#define ERR_LISTENFAIL -3
#define ERR_ACCEPTFAIL -4

#define BUFSIZE 131072

/*
 * Append an error message to errors.log, including errno details and the
 * active logging context. Falls back to stderr if the log cannot be opened.
 */
void logerror(char *message, char *logdir) {
  static char time_buf[20], filenamebuf[BUFSIZE];
  time_t t;
  struct tm* tm_info;
  int saved_errno = errno;
  t = time(NULL);
  tm_info = localtime(&t);
  strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
  snprintf(filenamebuf, sizeof(filenamebuf), "%s/errors.log", logdir);
  FILE *logfile = fopen(filenamebuf, "a");
  if (!logfile) {
    fprintf(stderr, "%s\t%s\t%s (errno=%d: %s)\n",
            time_buf,
            log_get_context(),
            message,
            saved_errno,
            saved_errno ? strerror(saved_errno) : "no errno");
    errno = saved_errno;
    return;
  }
  fprintf(logfile, "%s\t%s\t%s",
          time_buf,
          log_get_context(),
          message);
  if (saved_errno) {
    fprintf(logfile, " (errno=%d: %s)", saved_errno, strerror(saved_errno));
  } else {
    fprintf(logfile, " (no errno)");
  }
  fprintf(logfile, "\n");
  fclose(logfile);
  errno = saved_errno;
}

typedef struct childproc_s {
  int pid;
  struct childproc_s *next;
} *childproc;

void add_childproc(childproc *list, int pid) {
  struct childproc_s *newchild = (struct childproc_s *)malloc(sizeof(struct childproc_s));
  newchild->pid = pid;
  newchild->next = *list;
  *list = newchild;
}

void wait_for_children(childproc *list, char *logdir) {
  int status;
  while(*list) {
    int res = waitpid((*list)->pid, &status, WNOHANG);
    if (res == -1) {
      logerror("waitpid()", logdir);
      list = &((*list)->next);
    } else if (res > 0) {
      childproc thisproc = *list;
      *list = (*list)->next;
      free(thisproc);
    } else {
      list = &((*list)->next);
    }
  }
}

void usage(char *progname) {
  fprintf(stderr,
          "Usage: %s -l <local addr:service> -r <remote addr:service> -o <log directory> [-p <pid file>] [-O <observer config>] [--config PATH]\n"
          "  -O file=/path/to/events.log   Append observer output to a text logfile\n"
          "  -O amqp=<amqp URI>            Publish JSON events to RabbitMQ\n"
          "  --config PATH                 Optional configuration file; CLI overrides file values\n"
          "  (only one -O value may be supplied; choose either file or amqp)\n",
          progname);
}

static int parse_arguments(int argc, char **argv, struct proxy_config *cfg) {
  if (!cfg) {
    return -1;
  }

  optind = 1;
  static struct option options[] = {
      {"local", required_argument, NULL, 'l'},
      {"remote", required_argument, NULL, 'r'},
      {"log-dir", required_argument, NULL, 'o'},
      {"pidfile", required_argument, NULL, 'p'},
      {"observer", required_argument, NULL, 'O'},
      {"config", required_argument, NULL, 1000},
      {0, 0, 0, 0}};

  int c;
  while ((c = getopt_long(argc, argv, "l:r:o:p:O:", options, NULL)) != -1) {
    switch (c) {
    case 'l':
      proxy_set_string(&cfg->local_address, optarg);
      break;
    case 'r':
      proxy_set_string(&cfg->remote_address, optarg);
      break;
    case 'o':
      proxy_set_string(&cfg->logdir, optarg);
      normalise_logdir(cfg->logdir);
      break;
    case 'p':
      proxy_set_string(&cfg->pidfile_path, optarg);
      break;
    case 'O':
      proxy_set_string(&cfg->observer_config, optarg);
      break;
    case 1000:
      proxy_set_string(&cfg->config_path, optarg);
      break;
    default:
      usage(argv[0]);
      return -1;
    }
  }

  if (!cfg->local_address || !cfg->remote_address || !cfg->logdir) {
    usage(argv[0]);
    return -1;
  }
  normalise_logdir(cfg->logdir);
  return 0;
}

/*
 * Pump bytes from one socket to the other while mirroring the payload to the
 * connection log and optional observer. Returns the number of bytes forwarded
 * or zero on failure/EOF.
 */
int copy_message(int fromfd,
                 int tofd,
                 char *source,
                 FILE *logfile,
                 char *logdir,
                 struct observer_instance *observer,
                 enum observer_direction direction) {
  static char buf[BUFSIZE + 1];
  static char time_buf[20];
  time_t t;
  struct tm* tm_info;
  t = time(NULL);
  tm_info = localtime(&t);
  strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
  int res = recv(fromfd, buf, BUFSIZE, 0);
  if (res == 0) {
    if (direction == OBSERVER_DIR_CLIENT) {
      logerror("Broken connection (client side)", logdir);
    } else {
      logerror("Broken connection (server side)", logdir);
    }
    return 0;
  } else if (res < 0) {
    if (errno != ECONNRESET) {
      logerror("recv()", logdir);
    }
    return 0;
  } else {
    buf[res] = '\0';
    fprintf(logfile, "%s\t%s\t", time_buf, source);
    fwrite(buf, 1, res, logfile);
    fprintf(logfile, "\n");
    fflush(logfile);
    observer_instance_record(observer, direction, time_buf, buf, res);
    int total_sent = 0;
    while (total_sent < res) {
      int sres = send(tofd, buf + total_sent, res - total_sent, 0);
      if (sres > 0) {
        total_sent += sres;
        continue;
      }
      if (sres == 0) {
        logerror("send()", logdir);
        return 0;
      }
      if (sres == -1 && errno == EINTR) {
        continue;
      }
      logerror("send()", logdir);
      return 0;
    }
  }
  return res;
}

// Convert a struct sockaddr address to a string, IPv4 and IPv6:

/*
 * Render a sockaddr as a printable IP (IPv4/IPv6). Returns NULL on failure.
 */
char *get_ip_str(const struct sockaddr *sa, char *s, size_t maxlen) {
  switch(sa->sa_family) {
  case AF_INET:
    inet_ntop(AF_INET, &(((struct sockaddr_in *)sa)->sin_addr),
	      s, maxlen);
    break;
    
  case AF_INET6:
    inet_ntop(AF_INET6, &(((struct sockaddr_in6 *)sa)->sin6_addr),
	      s, maxlen);
    break;
    
  default:
    strncpy(s, "Unknown AF", maxlen);
    return NULL;
  }
  
  return s;
}

/*
 * Child process entry point: proxy a single client connection to the remote
 * server using select(2) and mirror traffic to disk/observer.
 */
void handle_connection(int client_fd,
		       struct sockaddr *client_address,
		       socklen_t client_addrlen,
		       struct sockaddr *remote_address,
		       socklen_t remote_addrlen,
		       char *logdir,
		       struct observer_global *observer_global) {
  static char log_filename[BUFSIZE];
  static char time_buf[20], addr_buf[BUFSIZE], remote_buf[BUFSIZE];
  char context[CONTEXT_LENGTH];
  time_t t;
  struct tm* tm_info;
  t = time(NULL);
  tm_info = localtime(&t);
  strftime(time_buf, sizeof(time_buf), "%Y-%m-%d_%H%M%S", tm_info);
  const char *client_ip = get_ip_str(client_address, addr_buf, BUFSIZE);
  const char *remote_ip = get_ip_str(remote_address, remote_buf, BUFSIZE);
  int client_port = 0;
  int remote_port = 0;
  if (client_address->sa_family == AF_INET) {
    client_port = ntohs(((struct sockaddr_in *)client_address)->sin_port);
  } else if (client_address->sa_family == AF_INET6) {
    client_port = ntohs(((struct sockaddr_in6 *)client_address)->sin6_port);
  }
  if (remote_address->sa_family == AF_INET) {
    remote_port = ntohs(((struct sockaddr_in *)remote_address)->sin_port);
  } else if (remote_address->sa_family == AF_INET6) {
    remote_port = ntohs(((struct sockaddr_in6 *)remote_address)->sin6_port);
  }
  snprintf(log_filename, sizeof(log_filename), "%s/%s_%s.log", logdir, time_buf, client_ip ? client_ip : "unknown");
  if (remote_ip && remote_port) {
    snprintf(context, sizeof(context), "conn=%s client=%s:%d remote=%s:%d",
             time_buf,
             client_ip ? client_ip : "unknown",
             client_port,
             remote_ip,
             remote_port);
  } else {
    snprintf(context, sizeof(context), "conn=%s client=%s:%d",
             time_buf,
             client_ip ? client_ip : "unknown",
             client_port);
  }
  log_set_context(context);
  FILE *logfile = fopen(log_filename, "a");
  struct observer_instance *observer = NULL;

  int remote_fd;
  remote_fd = socket(remote_address->sa_family, SOCK_STREAM, 0);
  if (remote_fd == -1) {
    logerror("socket()", logdir);
    return;
  }

  if(connect(remote_fd, remote_address, remote_addrlen)==-1) {
    logerror("connect()", logdir);
    fclose(logfile);
    close(client_fd);
    close(remote_fd);
    return;
  }
  observer = observer_instance_create(observer_global, time_buf, addr_buf);
  if (observer_global && !observer) {
    logerror("observer_instance_create()", logdir);
  }
  fd_set my_set;
  fd_set wk_set;
  
  FD_ZERO(&my_set);
  int client_open = 1;
  int remote_open = 1;
  int max_fd = client_fd > remote_fd ? client_fd : remote_fd;
  while (client_open || remote_open) {
    FD_ZERO(&my_set);
    if (client_open) {
      FD_SET(client_fd, &my_set);
    }
    if (remote_open) {
      FD_SET(remote_fd, &my_set);
    }
    memcpy(&wk_set, &my_set, sizeof(fd_set));

    if (select(max_fd + 1, &wk_set, NULL, NULL, NULL) == -1) {
      if ( errno == EINTR )
	continue;
      logerror("select()", logdir);
      fclose(logfile);
      close(client_fd);
      close(remote_fd);
      observer_instance_close(observer);
      return;
    }
    if (client_open && FD_ISSET(client_fd, &wk_set)) {
      if (!copy_message(client_fd, remote_fd, "client", logfile, logdir, observer, OBSERVER_DIR_CLIENT)) {
        client_open = 0;
        if (shutdown(remote_fd, SHUT_WR) == -1 && errno != ENOTCONN) {
          logerror("shutdown()", logdir);
        }
      }
    }
    if (remote_open && FD_ISSET(remote_fd, &wk_set)) {
      if (!copy_message(remote_fd, client_fd, "server", logfile, logdir, observer, OBSERVER_DIR_SERVER)) {
        remote_open = 0;
        if (shutdown(client_fd, SHUT_WR) == -1 && errno != ENOTCONN) {
          logerror("shutdown()", logdir);
        }
      }
    }
  }
  fclose(logfile);
  close(client_fd);
  close(remote_fd);
  observer_instance_close(observer);
  return;
}

/*
 * Parent process loop: accept new connections and fork children to handle
 * each session while reaping completed children opportunistically.
 */
int dolisten(struct sockaddr *local_address,
	     socklen_t local_addrlen,
	     struct sockaddr *remote_address,
	     socklen_t remote_addrlen,
	     char *logdir,
	     struct observer_global *observer_global) {
  int sockfd, connfd;
  struct sockaddr_storage client_address;
  childproc children = NULL;
  char listener_context[CONTEXT_LENGTH];
  char local_buf[BUFSIZE];
  char remote_buf[BUFSIZE];
  const char *local_ip = get_ip_str(local_address, local_buf, BUFSIZE);
  const char *remote_ip = get_ip_str(remote_address, remote_buf, BUFSIZE);
  int local_port = 0;
  int remote_port = 0;
  if (local_address->sa_family == AF_INET) {
    local_port = ntohs(((struct sockaddr_in *)local_address)->sin_port);
  } else if (local_address->sa_family == AF_INET6) {
    local_port = ntohs(((struct sockaddr_in6 *)local_address)->sin6_port);
  }
  if (remote_address->sa_family == AF_INET) {
    remote_port = ntohs(((struct sockaddr_in *)remote_address)->sin_port);
  } else if (remote_address->sa_family == AF_INET6) {
    remote_port = ntohs(((struct sockaddr_in6 *)remote_address)->sin6_port);
  }
  if (remote_ip && remote_port) {
    snprintf(listener_context, sizeof(listener_context), "listener local=%s:%d remote=%s:%d",
             local_ip ? local_ip : "unknown",
             local_port,
             remote_ip,
             remote_port);
  } else {
    snprintf(listener_context, sizeof(listener_context), "listener local=%s:%d",
             local_ip ? local_ip : "unknown",
             local_port);
  }
  log_set_context(listener_context);
  
  // socket create and verification
  sockfd = socket(local_address->sa_family, SOCK_STREAM, 0);
  if (sockfd == -1) {
    printf("socket creation failed...\n");
    return ERR_SOCKFAIL;
  }

  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0) {
    logerror("SO_REUSEADDR", logdir);
  }

  // Binding newly created socket to given IP and verification
  if ((bind(sockfd, local_address, local_addrlen)) != 0) {
    logerror("bind()", logdir);
    return ERR_BINDFAIL;
  }
  
  // Now server is ready to listen and verification
  if ((listen(sockfd, 5)) != 0) {
    logerror("listen()", logdir);
    return ERR_LISTENFAIL;
  }

  
  fd_set my_set;
  fd_set wk_set;
  
  FD_ZERO(&my_set);         /* initialize  fd_set */
  FD_SET(sockfd, &my_set);  /* put listener into fd_set */
  int max_fd = sockfd;
  int select_status;
  
  struct timeval timeout;
  
  while (1) {
    wait_for_children(&children, logdir);
    socklen_t len = sizeof(client_address);
    int pid;
    
    memcpy(&wk_set, &my_set, sizeof(my_set));
    memset(&timeout, 0, sizeof(timeout));
    timeout.tv_sec  = 5;
    timeout.tv_usec = 0;
    
    if ((select_status = select(max_fd + 1, &wk_set, NULL, NULL, &timeout)) < 0) {
      logerror("select()", logdir);
      break;
    }

    if (select_status > 0 && FD_ISSET(sockfd, &wk_set)) {

      // Accept the data packet from client and verification
      connfd = accept(sockfd, (struct sockaddr*)&client_address, &len);
      if (connfd < 0) {
	logerror("accept()", logdir);
      } else {
	if ((pid = fork()) == -1) {
	  logerror("fork()", logdir);
	  close(connfd);
	  continue;
	} else if (pid > 0) {
	  add_childproc(&children, pid);
	  close(connfd);
	  continue;
	} else if (pid == 0) {
	  handle_connection(connfd,
			  (struct sockaddr *)&client_address,
			  len,
			  remote_address,
			  remote_addrlen,
			  logdir,
			  observer_global);
	  exit(0);
	}
      }
    }
  }
  close(sockfd);
  return 0;
}

/*
 * Notice: Destroys its first argument, and terminates on error.
 */
void parseaddr(char *addrdesc, struct addrinfo **res) {
  char *p=addrdesc;
  char *addr = NULL;
  char *service = NULL;
  char *e=addrdesc + strlen(addrdesc);
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  int r;
  while(p < e && *p != ':') {
    p++;
  }
  if (p == e) {
    fprintf(stderr, "Fatal: address spec needs to be <address>:<port> : %s\n", addrdesc);
    exit(-97);
  } else {
    addr = addrdesc;
    *(p++) = '\0';
    service = p;
  }
  r = getaddrinfo(addr, service, &hints, res);
  if (r) {
    if (addr) {
      fprintf(stderr, "Fatal: %s:%s : %s\n", addr, service, gai_strerror(r));
    } else {
      fprintf(stderr, "Fatal: %s : %s\n", service, gai_strerror(r));
    }
    exit(-99);
  }
}

int main(int argc, char *argv[]) {
  struct addrinfo *remote_address, *local_address;
  struct observer_global *observer_global = NULL;
  struct proxy_config cfg;
  memset(&cfg, 0, sizeof(cfg));

  log_set_context("main");

  char *config_path = daemon_find_config_path(argc, argv);
  if (config_path) {
    cfg.config_path = config_path;
    if (daemon_parse_config_file(cfg.config_path,
                                 (daemon_kv_handler)apply_config_key,
                                 &cfg,
                                 (daemon_log_fn)config_log_message,
                                 0,
                                 "tcpproxy") != 0) {
      /* continue even if the config file could not be parsed */
    }
  }

  if (parse_arguments(argc, argv, &cfg) != 0) {
    free_proxy_config(&cfg);
    return -1;
  }

  parseaddr(cfg.local_address, &local_address);
  parseaddr(cfg.remote_address, &remote_address);
  if (observer_global_init(cfg.observer_config, &observer_global) != 0) {
    fprintf(stderr, "Unable to initialize observer configuration.\n");
    free_proxy_config(&cfg);
    return -1;
  }
  if (cfg.pidfile_path) {
    if (daemon_pidfile_create(cfg.pidfile_path, NULL, 0, "tcpproxy") != 0) {
      observer_global_free(observer_global);
      free_proxy_config(&cfg);
      return -1;
    }
    atexit(daemon_pidfile_cleanup);
    signal(SIGTERM, handle_exit_signal);
    signal(SIGINT, handle_exit_signal);
  }
  dolisten(local_address->ai_addr,
	   local_address->ai_addrlen,
	   remote_address->ai_addr,
	   remote_address->ai_addrlen,
	   cfg.logdir,
	   observer_global);
  observer_global_free(observer_global);
  daemon_pidfile_cleanup();
  free_proxy_config(&cfg);
}

static void handle_exit_signal(int signo) {
  (void)signo;
  daemon_pidfile_cleanup();
  _exit(0);
}
