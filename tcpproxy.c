#include<signal.h>
#include<stdio.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<string.h>
#include<sys/types.h>
#include<stdlib.h>
#include<unistd.h>
#include<errno.h>
#include <netdb.h>
#include <fcntl.h>

#define ERR_SOCKFAIL -1
#define ERR_BINDFAIL -2
#define ERR_LISTENFAIL -3
#define ERR_ACCEPTFAIL -4

#define BUFSIZE 131072

void usage(char *progname) {
  fprintf(stderr, "Usage: %s -l <local addr:service> -r <remote addr:service>\n", progname);
}

int copy_message(int fromfd, int tofd) {
  static char buf[BUFSIZE + 1];
  int res = recv(fromfd, buf, BUFSIZE, 0);
  if (res == 0) {
    fprintf(stderr, "Broken connection\n");
    return 0;
  } else if (res < 0) {
    if (res != ECONNRESET) {
      perror("recv()");
    }
    return 1;
  } else {
    buf[res] = '\0';
    printf("[%d to %d]: |%s|\n", fromfd, tofd, buf);
#ifdef SPLIT
    if (strstr(buf, "][")) {
      char *s = buf;
      char *p;
      while (p = strchr(s + 1, '[')) {
	*p = '\0';
	printf("sending: |%s|\n", s);
	int sres = send(tofd, s, strlen(s), 0);
	if (sres == -1 && errno != EINTR) {
	  return 0;
	}
	s=p;
	*s='[';
      }
    }
#endif
    int sres = send(tofd, buf, res, 0);
    if (sres == -1 && errno != EINTR) {
      return 0;
    }
  }
  return res;
}

void handle_connection(int client_fd,
		       struct sockaddr *client_address,
		       socklen_t client_addrlen,
		       struct sockaddr *remote_address,
		       socklen_t remote_addrlen) {
    int remote_fd;
    remote_fd=socket(AF_INET,SOCK_STREAM,0);
    if (remote_fd == -1) {
      perror("socket()");
      return;
    }

    if(connect(remote_fd, remote_address, remote_addrlen)==-1) {
        perror("connect()");
	return;
    }
    fd_set my_set;
    fd_set wk_set;
    
    FD_ZERO(&my_set);         /* initialize  fd_set */
    FD_SET(client_fd, &my_set);  /* put listener into fd_set */
    FD_SET(remote_fd, &my_set);  /* put listener into fd_set */
    int max_fd = client_fd;
    if (remote_fd > client_fd) {
      max_fd = remote_fd;
    }
    while (1) {
      memcpy(&wk_set, &my_set, sizeof(my_set));
      
      int rc = select(max_fd + 1, &wk_set, NULL, NULL, NULL);
      if ( rc == -1 ) {
	if ( errno == EINTR )
	  continue;
	perror("select()");
	close(client_fd);
	close(remote_fd);
	return;
      }
      if (FD_ISSET(client_fd, &wk_set)) {
	if (!copy_message(client_fd, remote_fd)) {
	  break;
	}
      } else if (FD_ISSET(remote_fd, &wk_set)) {
	if (!copy_message(remote_fd, client_fd)) {
	  break;
	}
      }
    }
    close(client_fd);
    close(remote_fd);
    return;
}

int dolisten(struct sockaddr *local_address, socklen_t local_addrlen, struct sockaddr *remote_address, socklen_t remote_addrlen) {
  int sockfd, connfd;
  struct sockaddr_in client_address;
  
  // socket create and verification
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1) {
    printf("socket creation failed...\n");
    return ERR_SOCKFAIL;
  }
  else
    printf("Socket successfully created..\n");

  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0) {
    perror("SO_REUSEADDR");
  }

  // Binding newly created socket to given IP and verification
  if ((bind(sockfd, local_address, local_addrlen)) != 0) {
    perror("bind()");
    return ERR_BINDFAIL;
  }
  
  // Now server is ready to listen and verification
  if ((listen(sockfd, 5)) != 0) {
    perror("listen()");
    return ERR_LISTENFAIL;
  }

  
  while (1) {
    socklen_t len = sizeof(client_address);
    int pid;
    
    // Accept the data packet from client and verification
    connfd = accept(sockfd, (struct sockaddr*)&client_address, &len);
    if (connfd < 0) {
      perror("accept()");
    } else {
      if ((pid = fork()) == -1) {
	perror("fork()");
	close(connfd);
	continue;
      } else if (pid > 0) {
	close(connfd);
	continue;
      } else if (pid == 0) {
	handle_connection(connfd, (struct sockaddr *)&client_address, len, remote_address, remote_addrlen);
	exit(0);
      }
    }
  }
  
  // close(sockfd);
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

  while ((o=getopt(argc, argv, "l:r:"))!=-1) {
    switch (o) {
    case 'l':
	local_address_string = optarg;
	break;
    case 'r':
	remote_address_string = optarg;
	break;
    default:
	usage(argv[0]);
	return -1;
	break;
    }
  }
  if (local_address_string == NULL || remote_address_string == NULL) {
    usage(argv[0]);
    return -1;
  }
  parseaddr(local_address_string, &local_address);
  parseaddr(remote_address_string, &remote_address);
  dolisten(local_address->ai_addr, local_address->ai_addrlen, remote_address->ai_addr, local_address->ai_addrlen);
}
