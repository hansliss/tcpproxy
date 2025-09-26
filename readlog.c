#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<string.h>
#include<ctype.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include<pcre2.h>        
#include <locale.h>

#define BUFSIZE 131072

#define TIMESTAMP_LENGTH 19
#define SOURCE_LENGTH 6

#define VALID_CHAR_REGEX "^[-+a-zA-Z0-9*,._()/ \\[\\]]*$"

#define CLIENT_2G_BASIC_HELLO "^\\[(SG\\*9090089931\\*0009\\*LK),([^,]*),([^,]*)$"
#define CLIENT_4G_BASIC_HELLO "^\\[(SG\\*1234567890\\*000D\\*LK),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*)$"

pcre2_code *re_client_2g_basic_hello;
pcre2_code *re_client_4g_basic_hello;

int handle_client_2g_basic_hello(char *timestamp, char *source, char *msg) {
  if (strcmp(source, "client")) {
    return 2;
  }
  // Create match data block
  pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re_client_2g_basic_hello, NULL);
  int rc = pcre2_match(
		       re_client_2g_basic_hello,
		       (PCRE2_SPTR)msg,
		       strlen(msg),
		       0, 0,
		       match_data,
		       NULL
		       );
  if (rc < 0) {
    pcre2_match_data_free(match_data);
    return 1;
  }
  PCRE2_SIZE *ovector = pcre2_get_ovector_pointer(match_data);

  if (rc != 4) {
    fprintf(stderr, "%s\t%s\tUnexpected match count: expected 4, got %d\t%s\n", timestamp, source, rc, msg);
  }

  // Extract and print values
  const char *values[3];
  static char buffer[3][64];
  for (int i = 1; i <= 3; i++) {
    PCRE2_SIZE start = ovector[2*i];
    PCRE2_SIZE end   = ovector[2*i + 1];
    size_t len = end - start;
    
    if (len >= sizeof(buffer[i-1])) len = sizeof(buffer[i-1]) - 1;
    memcpy(buffer[i-1], msg + start, len);
    buffer[i-1][len] = '\0';
    values[i-1] = buffer[i-1];
  }
  
  printf("%s\t%s\t[TID,%s,BAT]\n", timestamp, source, 
           values[1]);

  pcre2_match_data_free(match_data);
  return 0;
}

int handle_client_4g_basic_hello(char *timestamp, char *source, char *msg) {
  if (strcmp(source, "client")) {
    return 2;
  }
  // Create match data block
  pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re_client_4g_basic_hello, NULL);
  int rc = pcre2_match(
		       re_client_4g_basic_hello,
		       (PCRE2_SPTR)msg,
		       strlen(msg),
		       0, 0,
		       match_data,
		       NULL
		       );
  if (rc < 0) {
    pcre2_match_data_free(match_data);
    return 1;
  }
  PCRE2_SIZE *ovector = pcre2_get_ovector_pointer(match_data);

  if (rc != 8) {
    fprintf(stderr, "%s\t%s\tUnexpected match count: expected 8, got %d\t%s\n", timestamp, source, rc, msg);
  }

  // Extract and print values
  const char *values[7];
  static char buffer[7][64];
  for (int i = 1; i <= 7; i++) {
    PCRE2_SIZE start = ovector[2*i];
    PCRE2_SIZE end   = ovector[2*i + 1];
    size_t len = end - start;
    
    if (len >= sizeof(buffer[i-1])) len = sizeof(buffer[i-1]) - 1;
    memcpy(buffer[i-1], msg + start, len);
    buffer[i-1][len] = '\0';
    values[i-1] = buffer[i-1];
  }
  
  printf("%s\t%s\t[TID,%s,%s,BAT,%s,%s,%s]\n", timestamp, source, 
           values[1], values[2], values[4], values[5], values[6]);

  pcre2_match_data_free(match_data);
  return 0;
}

int handle(int newfile, char *timestamp, char *source, char *msg) {
  static char msgbuf_client[BUFSIZE];
  static char msgbuf_server[BUFSIZE];
  char *msgbuf = msgbuf_client;
  if (!strcmp(source, "server")) {
    msgbuf = msgbuf_server;
  }
  if (newfile) {
    msgbuf[0] = '\0';
  }
  char *p1;
  //  printf("Adding =%s=\n", msg);
  strcat(msgbuf, msg);
  while ((p1 = strchr(msgbuf, ']')) != NULL) {
    *(p1++) = '\0';
    int len=strlen(msgbuf) - 20;
    if (!strcmp(source, "client")) {
      len -= 3;
    }
    // for long form, geo is col 5-8, battery is col 14 (both 909 and 911)
    if (handle_client_2g_basic_hello(timestamp, source, msgbuf) != 0 &&
	handle_client_4g_basic_hello(timestamp, source, msgbuf) != 0) {
      printf("%s\t%s\t%s]\n", timestamp, source, msgbuf);
    }
    memmove(msgbuf, p1, strlen(p1) + 1);
  }
  return 0;
}

int main(int argc, char *argv[]) {
  FILE *infile;
  static char inbuf[BUFSIZE];
  static char timestamp[64];
  static char source[16];
  char *filename, *msg, *p1, *p2;
  int lineno;
  int errornumber;
  PCRE2_SIZE erroroffset;
  
  // Compile the regex
  pcre2_code *re = pcre2_compile(
				 (PCRE2_SPTR)VALID_CHAR_REGEX,
				 PCRE2_ZERO_TERMINATED,
				 0,              // options
				 &errornumber,
				 &erroroffset,
				 NULL            // default compile context
				 );
  
  if (!re) {
    PCRE2_UCHAR buffer[256];
    pcre2_get_error_message(errornumber, buffer, sizeof(buffer));
    fprintf(stderr, "PCRE2 compilation failed at offset %d: %s\n",
	    (int)erroroffset, buffer);
    return -2;
  }
 re_client_2g_basic_hello = pcre2_compile(
					(PCRE2_SPTR)CLIENT_2G_BASIC_HELLO,
					PCRE2_ZERO_TERMINATED,
					0,              // options
					&errornumber,
					&erroroffset,
					NULL            // default compile context
					);
  
  if (!re_client_2g_basic_hello) {
    PCRE2_UCHAR buffer[256];
    pcre2_get_error_message(errornumber, buffer, sizeof(buffer));
    fprintf(stderr, "PCRE2 compilation failed at offset %d: %s\n",
	    (int)erroroffset, buffer);
    return -2;
  }
  re_client_4g_basic_hello = pcre2_compile(
					(PCRE2_SPTR)CLIENT_4G_BASIC_HELLO,
					PCRE2_ZERO_TERMINATED,
					0,              // options
					&errornumber,
					&erroroffset,
					NULL            // default compile context
					);
  
  if (!re_client_4g_basic_hello) {
    PCRE2_UCHAR buffer[256];
    pcre2_get_error_message(errornumber, buffer, sizeof(buffer));
    fprintf(stderr, "PCRE2 compilation failed at offset %d: %s\n",
	    (int)erroroffset, buffer);
    return -2;
  }
  timestamp[sizeof(timestamp) - 1] = '\0';
  source[sizeof(source) - 1] = '\0';
  for (int i=1; i<argc; i++) {
    int newfile = 1;
    filename = argv[i];
    if (!(infile = fopen(filename, "r"))) {
      perror(filename);
      return -1;
    }
    lineno = 0;
    while(fgets(inbuf, BUFSIZE, infile) != NULL) {
      lineno++;
      if ((p1 = strchr(inbuf, '\t')) != NULL) {
	*(p1++) = '\0';
	strncpy(timestamp, inbuf, sizeof(timestamp) - 1);
	if ((p2 = strchr(p1, '\t')) != NULL) {
	  *(p2++) = '\0';
	  strncpy(source, p1, sizeof(source) - 1);
	  if (strlen(timestamp) == TIMESTAMP_LENGTH && strlen(source) == SOURCE_LENGTH) {
	    while (p2[strlen(p2) - 1] == '\r' || p2[strlen(p2) - 1] == '\n') {
	      p2[strlen(p2) - 1] = '\0';
	    }
	    msg = p2;
	    if (!strlen(msg)) {
	      fprintf(stderr, "EMPTY: %s\t%s\n", timestamp, source);
	    } else {
	      pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re, NULL);
	      int rc = pcre2_match(
				   re,
				   (PCRE2_SPTR)msg,
				   strlen(msg),
				   0,             // start offset
				   0,             // options
				   match_data,
				   NULL           // match context
				   );
	      
	      if (rc >= 0 && (strchr(msg, '[') || strchr(msg, ']')) /*&& !strstr(msg, "GET") && !strstr(msg, "EHLO") && !strstr(msg, "ARGC") && !strstr(msg, "JRMI")  && !strstr(msg, "Query") && !strstr(msg, "Subscribe") && !strstr(msg, "Client")*/) {
		handle(newfile, timestamp, source, msg);
		newfile = 0;
	      } else {
		fprintf(stderr, "ERROR: %s\t%s\t%s\n", timestamp, source, msg);
	      }
	      
	      // Clean up
	      pcre2_match_data_free(match_data);
	    }
	  }
	} else {
	  // fprintf(stderr, "%s[%d]: missing source\n", filename, lineno);
	}
      } else {
	// fprintf(stderr, "%s[%d]: missing timestamp\n", filename, lineno);
      }
    }
    	       
    fclose(infile);
  }
  pcre2_code_free(re);
  pcre2_code_free(re_client_2g_basic_hello);
  pcre2_code_free(re_client_4g_basic_hello);
  return 0;
}
