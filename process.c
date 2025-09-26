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

#define TK909_POS "^\\[([A-Z0-9]*)\\*(..........)\\*([A-Z0-9]*)\\*UD,(......),(......),([AV]),([-0-9.]*),([NS]),([-0-9.]*),([EW]),([.0-9]*),([-.0-9]*),([^,]*),([0-9]*),([^,]*),([.0-9]*),([^,]*),([^,]*),([^,]*),([0-9]*),([0-9]*),([0-9]*),([0-9]*)((,[0-9][0-9]*,[0-9]*,[0-9]*)*),,00\\]$"
#define TK911_POS "^\\[([A-Z0-9]*)\\*(..........)\\*([A-Z0-9]*)\\*UD,(......),(......),([AV]),([-0-9.]*),([NS]),([-0-9.]*),([EW]),([.0-9]*),([-.0-9]*),([^,]*),([0-9]*),([^,]*),([.0-9]*),([^,]*),([^,]*),([^,]*),([0-9]*),([0-9]*),([0-9]*),([0-9]*)((,[0-9][0-9]*,[0-9]*,[0-9]*)*),,00\\]$"

/*
Group	Value
1	TType
2	TID
3	LEN
4	Date
5	Time
6	Active/Void
7	Lat
8	Latdir
9	Long
10	Longdir
11	Speed
12	Direction
13	Unknown1
14	Sats
15	Unknown2
16	Bat
17	Unknown3
18	Unknown4
19	Unknown5
20	Celltowers
21	MNC
22	MCC
23	Unknown6
24	Celltowers (list of LAC,TowerId,Signalstrength)
*/

#define FA29C_POS "^\\[([A-Z0-9]*)\\*(..........)\\*([A-Z0-9]*)\\*UD,(......),(......),([AV]),([-0-9.]*),([NS]),([-0-9.]*),([EW]),([.0-9]*),([-.0-9]*),([^,]*),([0-9]),([^,]*),([.0-9]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*),([^,]*)\\]$"

/*
Group	Value
1	TType
2	TID
3	LEN
4	Date
5	Time
6	Active/Void
7	Lat
8	Latdir
9	Long
10	Longdir
11	Speed
12	Direction
13	Unknown1
14	Sats
15	Unknown2
16	Bat
17	Unknown3
18	Unknown4
19	Unknown5
20	Unknown6
21	Unknown7
22	Unknown8
*/

pcre2_code *regex_valid_char;
pcre2_code *regex_tk909_pos;
pcre2_code *regex_tk911_pos;
pcre2_code *regex_fa29c_pos;

char ** handle_client_tk909_pos(char *timestamp, char *sourceip, char *source, char *msg) {
  if (strcmp(source, "client")) {
    return NULL;
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

int process_is_valid_message(char *msg) {
  if (!msg || !strlen(msg)) {
    return 0;
  }
  pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(regex_valid_chars, NULL);
  int rc = pcre2_match(
		       regex_valid_chars,
		       (PCRE2_SPTR)msg,
		       strlen(msg),
		       0,             // start offset
		       0,             // options
		       match_data,
		       NULL           // match context
		       );
  
  if (rc >= 0 && (strchr(msg, '[') || strchr(msg, ']'))) {
    return 1;
  }
  return 0;
}

pcre2_code *regex_compile(char *regex) {
  int errornumber;
  PCRE2_SIZE erroroffset;
  pcre2_code *re = pcre2_compile(
				 (PCRE2_SPTR)regex,
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
    return NULL;
  }
  return re;
}

int process_init() {
  int errornumber;
  PCRE2_SIZE erroroffset;
  
  // Compile the regex
  if (!(regex_valid_char = regex_compile(REGEX_VALID_CHAR)) ||
      !(regex_tk909_pos = regex_compile(REGEX_TK909_POS)) ||
      !(regex_tk911_pos = regex_compile(REGEX_TK911_POS)) ||
      !(regex_fa29c_pos = regex_compile(REGEX_FA29C_POS))) {
    return 0;
  }
  return 1;
}

int process_cleanup() {
  if (regex_valid_char) {
    pcre2_code_free(regex_valid_char);
  }
  if (regex_tk909_pos) {
    pcre2_code_free(regex_tk909_pos);
  }
  if (regex_tk911_pos) {
    pcre2_code_free(regex_tk911_pos);
  }
  if (regex_fa29c_pos) {
    pcre2_code_free(regex_fa29c_pos);
  }
}

int process_handle(char *timestamp, char *sourceip, char *source, char *msg) {
  static char msgbuf_client[BUFSIZE];
  static char msgbuf_server[BUFSIZE];
  char *msgbuf = msgbuf_client;
  if (!strcmp(source, "server")) {
    msgbuf = msgbuf_server;
  }
  char *p1;
  if (!process_is_valid_message(msg)) {
    return -1;
  }
  //  printf("Adding =%s=\n", msg);
  strcat(msgbuf, msg);
  while ((p1 = strchr(msgbuf, ']')) != NULL) {
    *(p1++) = '\0';
    if ((values = handle_client_fa29c_pos(timestamp, sourceip, source, msgbuf)) == NULL &&
	(values = handle_client_tk909_pos(timestamp, sourceip, source, msgbuf)) == NULL &&
	(values = handle_client_tk911_pos(timestamp, sourceip, source, msgbuf)) == NULL) {
      return 0;
    }
    memmove(msgbuf, p1, strlen(p1) + 1);
  }
  return 0;
}
