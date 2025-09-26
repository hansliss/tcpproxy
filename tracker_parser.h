#ifndef TRACKER_PARSER_H
#define TRACKER_PARSER_H

#include <stddef.h>
#include "observer.h"

#define TRACKER_PARSER_BUFFER 262144

typedef void (*tracker_parser_emit_fn)(void *ctx,
                                       enum observer_direction direction,
                                       const char *timestamp,
                                       const char *message,
                                       size_t message_len);

typedef struct tracker_parser_s {
  char client_buf[TRACKER_PARSER_BUFFER];
  size_t client_len;
  char server_buf[TRACKER_PARSER_BUFFER];
  size_t server_len;
} tracker_parser;

void tracker_parser_init(tracker_parser *parser);
void tracker_parser_push(tracker_parser *parser,
                         enum observer_direction direction,
                         const char *timestamp,
                         const char *data,
                         size_t data_len,
                         tracker_parser_emit_fn emit,
                         void *emit_ctx);

#endif /* TRACKER_PARSER_H */
