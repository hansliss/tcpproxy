#include "tracker_parser.h"

#include <stdlib.h>
#include <string.h>

static int is_allowed_char(unsigned char c) {
  if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
    return 1;
  }
  switch (c) {
  case '[':
  case ']':
  case '-':
  case '+':
  case '*':
  case ',':
  case '.':
  case '_':
  case '(':
  case ')':
  case '/':
  case ' ':
    return 1;
  default:
    return 0;
  }
}

static int message_is_valid(const char *msg, size_t len) {
  if (len < 2) {
    return 0;
  }
  if (msg[0] != '[' || msg[len - 1] != ']') {
    return 0;
  }
  for (size_t i = 0; i < len; i++) {
    if (!is_allowed_char((unsigned char)msg[i])) {
      return 0;
    }
  }
  return 1;
}

void tracker_parser_init(tracker_parser *parser) {
  if (!parser) {
    return;
  }
  parser->client_len = 0;
  parser->server_len = 0;
}

static void append_data(char *buffer, size_t *buffer_len, const char *data, size_t data_len) {
  if (data_len == 0) {
    return;
  }
  if (data_len >= TRACKER_PARSER_BUFFER) {
    memcpy(buffer, data + (data_len - TRACKER_PARSER_BUFFER), TRACKER_PARSER_BUFFER);
    *buffer_len = TRACKER_PARSER_BUFFER;
    return;
  }
  if (*buffer_len + data_len > TRACKER_PARSER_BUFFER) {
    size_t overflow = (*buffer_len + data_len) - TRACKER_PARSER_BUFFER;
    memmove(buffer, buffer + overflow, *buffer_len - overflow);
    *buffer_len -= overflow;
  }
  memcpy(buffer + *buffer_len, data, data_len);
  *buffer_len += data_len;
}

static void process_buffer(char *buffer,
                           size_t *buffer_len,
                           enum observer_direction direction,
                           const char *timestamp,
                           tracker_parser_emit_fn emit,
                           void *emit_ctx) {
  size_t len = *buffer_len;
  while (len > 0) {
    char *start = memchr(buffer, '[', len);
    if (!start) {
      *buffer_len = 0;
      return;
    }
    if (start != buffer) {
      size_t delta = (size_t)(start - buffer);
      len -= delta;
      memmove(buffer, start, len);
      *buffer_len = len;
      continue;
    }
    char *end = memchr(buffer + 1, ']', len - 1);
    if (!end) {
      break;
    }
    size_t msg_len = (size_t)(end - buffer + 1);
    if (message_is_valid(buffer, msg_len) && emit) {
      char *message = (char *)malloc(msg_len + 1);
      if (message) {
        memcpy(message, buffer, msg_len);
        message[msg_len] = '\0';
        emit(emit_ctx, direction, timestamp, message, msg_len);
        free(message);
      }
    }
    len -= msg_len;
    if (len > 0) {
      memmove(buffer, buffer + msg_len, len);
    }
    *buffer_len = len;
  }
}

void tracker_parser_push(tracker_parser *parser,
                         enum observer_direction direction,
                         const char *timestamp,
                         const char *data,
                         size_t data_len,
                         tracker_parser_emit_fn emit,
                         void *emit_ctx) {
  if (!parser || !data || data_len == 0) {
    return;
  }
  char *buffer = parser->client_buf;
  size_t *buffer_len = &parser->client_len;
  if (direction == OBSERVER_DIR_SERVER) {
    buffer = parser->server_buf;
    buffer_len = &parser->server_len;
  }
  append_data(buffer, buffer_len, data, data_len);
  process_buffer(buffer, buffer_len, direction, timestamp, emit, emit_ctx);
}
