#include<signal.h>
#include<stdio.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<string.h>
#include<sys/types.h>
#include<stdlib.h>
#include<unistd.h>
#include<errno.h>
#include<netdb.h>
#include<fcntl.h>
#include<time.h>
#include<sys/wait.h>

#include "observer.h"

#define ERR_SOCKFAIL -1
#define ERR_BINDFAIL -2
#define ERR_LISTENFAIL -3
#define ERR_ACCEPTFAIL -4

#define BUFSIZE 131072

void logerror(char *message, char *logdir) {
  static char time_buf[20], filenamebuf[BUFSIZE];
  time_t t;
  struct tm* tm_info;
  t = time(NULL);
  tm_info = localtime(&t);
  strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
  sprintf(filenamebuf, "%s/errors.log", logdir);
  FILE *logfile = fopen(filenamebuf, "a");
  fprintf(logfile, "%s\t%s: %m\n", time_buf, message);
  fclose(logfile);
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
  fprintf(stderr, "Usage: %s -l <local addr:service> -r <remote addr:service> -o <log directory>\n", progname);
}

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
    logerror("Broken connection", logdir);
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

void handle_connection(int client_fd,
		       struct sockaddr *client_address,
		       socklen_t client_addrlen,
		       struct sockaddr *remote_address,
		       socklen_t remote_addrlen,
		       char *logdir,
		       struct observer_global *observer_global) {
  static char log_filename[BUFSIZE];
  static char time_buf[20], addr_buf[BUFSIZE];
  time_t t;
  struct tm* tm_info;
  t = time(NULL);
  tm_info = localtime(&t);
  strftime(time_buf, sizeof(time_buf), "%Y-%m-%d_%H%M%S", tm_info);
  sprintf(log_filename, "%s/%s_%s.log", logdir, time_buf, get_ip_str(client_address, addr_buf, BUFSIZE));
  FILE *logfile = fopen(log_filename, "a");
  struct observer_instance *observer = NULL;

  int remote_fd;
  remote_fd=socket(AF_INET,SOCK_STREAM,0);
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

int dolisten(struct sockaddr *local_address,
	     socklen_t local_addrlen,
	     struct sockaddr *remote_address,
	     socklen_t remote_addrlen,
	     char *logdir,
	     struct observer_global *observer_global) {
  int sockfd, connfd;
  struct sockaddr_in client_address;
  childproc children = NULL;
  
  // socket create and verification
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
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
  struct addrinfo hints = {
    0,
    AF_INET,
    SOCK_DGRAM,
    IPPROTO_UDP,
    0,
    NULL,
    NULL,
    NULL
  };
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
  if ((*res)->ai_family != AF_INET) {
    fprintf(stderr, "I only handle IPv4 at the moment.\n");
    exit(-98);
  }
}

int main(int argc, char *argv[]) {
  struct addrinfo *remote_address, *local_address;
  int o;
  char *remote_address_string = NULL;
  char *local_address_string = NULL;
  char *logdir = NULL;
  char *observer_config_string = NULL;
  struct observer_global *observer_global = NULL;

  while ((o=getopt(argc, argv, "l:r:o:O:"))!=-1) {
    switch (o) {
    case 'l':
      local_address_string = optarg;
      break;
    case 'r':
      remote_address_string = optarg;
      break;
    case 'o':
      logdir = optarg;
      while (strlen(logdir) > 1 && logdir[strlen(logdir) -1] == '/') {
	logdir[strlen(logdir) - 1] = '\0';
      }
      break;
    case 'O':
      observer_config_string = optarg;
      break;
    default:
      usage(argv[0]);
      return -1;
      break;
    }
  }
  if (local_address_string == NULL || remote_address_string == NULL || logdir == NULL) {
    usage(argv[0]);
    return -1;
  }
  parseaddr(local_address_string, &local_address);
  parseaddr(remote_address_string, &remote_address);
  if (observer_global_init(observer_config_string, &observer_global) != 0) {
    fprintf(stderr, "Unable to initialize observer configuration.\n");
    return -1;
  }
  dolisten(local_address->ai_addr,
	   local_address->ai_addrlen,
	   remote_address->ai_addr,
	   remote_address->ai_addrlen,
	   logdir,
	   observer_global);
  observer_global_free(observer_global);
}
