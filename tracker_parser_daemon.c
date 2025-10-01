#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

/*
 * Minimal C reimplementation of the tracker_parser_daemon helper. The daemon
 * consumes JSON events from RabbitMQ, extracts tracker payloads, and publishes
 * derived JSON objects to another exchange or queue. The original Python
 * implementation used pika; this version relies on librabbitmq.
 */

#define MAX_URI_PART 256
#define MAX_JSON_VALUE 8192

struct uri_parts {
  char host[MAX_URI_PART];
  char username[MAX_URI_PART];
  char password[MAX_URI_PART];
  char vhost[MAX_URI_PART];
  int port;
};

struct daemon_config {
  char *input_uri;
  char *input_queue;
  char *input_exchange;
  char *input_routing_key;
  char *output_uri;
  char *output_exchange;
  char *output_routing_key;
  int log_level;
};

enum {
  LOG_ERROR = 0,
  LOG_WARN = 1,
  LOG_INFO = 2,
  LOG_DEBUG = 3
};

static volatile sig_atomic_t g_stop = 0;

static void log_message(int level, int configured_level, const char *fmt, ...) {
  if (level > configured_level) {
    return;
  }
  const char *tag = "INFO";
  if (level == LOG_ERROR) {
    tag = "ERROR";
  } else if (level == LOG_WARN) {
    tag = "WARN";
  } else if (level == LOG_DEBUG) {
    tag = "DEBUG";
  }
  struct timeval tv;
  gettimeofday(&tv, NULL);
  struct tm tm;
  localtime_r(&tv.tv_sec, &tm);
  char timestamp[64];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &tm);
  fprintf(stderr, "%s %-5s tracker_parser_daemon: ", timestamp, tag);
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
  fputc('\n', stderr);
}

static void signal_handler(int sig) {
  (void)sig;
  g_stop = 1;
}

static int hex_value(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return 10 + (c - 'a');
  }
  if (c >= 'A' && c <= 'F') {
    return 10 + (c - 'A');
  }
  return -1;
}

static void percent_decode(const char *input, char *output, size_t out_size) {
  size_t oi = 0;
  for (size_t i = 0; input[i] != '\0' && oi + 1 < out_size; ++i) {
    if (input[i] == '%' && isxdigit((unsigned char)input[i + 1]) && isxdigit((unsigned char)input[i + 2])) {
      int hi = hex_value(input[i + 1]);
      int lo = hex_value(input[i + 2]);
      if (hi >= 0 && lo >= 0) {
        output[oi++] = (char)((hi << 4) | lo);
        i += 2;
        continue;
      }
    }
    if (input[i] == '+') {
      output[oi++] = ' ';
    } else {
      output[oi++] = input[i];
    }
  }
  output[oi] = '\0';
}

static int parse_amqp_uri(const char *uri, struct uri_parts *out) {
  if (!uri || !out || strncmp(uri, "amqp://", 7) != 0) {
    return -1;
  }
  memset(out, 0, sizeof(*out));
  strcpy(out->username, "guest");
  strcpy(out->password, "guest");
  strcpy(out->vhost, "/");
  out->port = 5672;

  const char *cursor = uri + 7;
  const char *at = strchr(cursor, '@');
  if (at) {
    const char *cred = cursor;
    const char *col = memchr(cred, ':', (size_t)(at - cred));
    if (col) {
      size_t ulen = (size_t)(col - cred);
      size_t plen = (size_t)(at - col - 1);
      if (ulen >= MAX_URI_PART || plen >= MAX_URI_PART) {
        return -1;
      }
      char user[MAX_URI_PART];
      char pass[MAX_URI_PART];
      memcpy(user, cred, ulen);
      user[ulen] = '\0';
      memcpy(pass, col + 1, plen);
      pass[plen] = '\0';
      percent_decode(user, out->username, sizeof(out->username));
      percent_decode(pass, out->password, sizeof(out->password));
    } else {
      size_t ulen = (size_t)(at - cred);
      if (ulen >= MAX_URI_PART) {
        return -1;
      }
      char user[MAX_URI_PART];
      memcpy(user, cred, ulen);
      user[ulen] = '\0';
      percent_decode(user, out->username, sizeof(out->username));
    }
    cursor = at + 1;
  }

  const char *slash = strchr(cursor, '/');
  const char *host_part = cursor;
  char hostbuf[MAX_URI_PART];
  if (slash) {
    size_t host_len = (size_t)(slash - cursor);
    if (host_len >= sizeof(hostbuf)) {
      return -1;
    }
    memcpy(hostbuf, cursor, host_len);
    hostbuf[host_len] = '\0';
    cursor = slash + 1;
  } else {
    strncpy(hostbuf, cursor, sizeof(hostbuf));
    hostbuf[sizeof(hostbuf) - 1] = '\0';
    cursor = NULL;
  }

  const char *colon = strchr(host_part, ':');
  if (colon && (!slash || colon < slash)) {
    size_t host_len = (size_t)(colon - host_part);
    if (host_len >= sizeof(out->host)) {
      return -1;
    }
    memcpy(out->host, host_part, host_len);
    out->host[host_len] = '\0';
    out->port = atoi(colon + 1);
  } else {
    if (strlen(hostbuf) >= sizeof(out->host)) {
      return -1;
    }
    strcpy(out->host, hostbuf);
  }

  if (slash) {
    const char *qmark = cursor ? strchr(cursor, '?') : NULL;
    size_t vlen = cursor ? (qmark ? (size_t)(qmark - cursor) : strlen(cursor)) : 0;
    if (vlen > 0) {
      if (vlen >= MAX_URI_PART) {
        return -1;
      }
      char vhost_buf[MAX_URI_PART];
      memcpy(vhost_buf, cursor, vlen);
      vhost_buf[vlen] = '\0';
      percent_decode(vhost_buf, out->vhost, sizeof(out->vhost));
    }
  }

  if (out->port <= 0) {
    out->port = 5672;
  }
  if (out->host[0] == '\0') {
    strcpy(out->host, "127.0.0.1");
  }
  return 0;
}

static amqp_connection_state_t open_connection(const struct uri_parts *uri,
                                               int log_level) {
  amqp_connection_state_t conn = amqp_new_connection();
  amqp_socket_t *socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    log_message(LOG_ERROR, log_level, "Failed to allocate AMQP socket");
    amqp_destroy_connection(conn);
    return NULL;
  }
  int status = amqp_socket_open(socket, uri->host, uri->port);
  if (status != AMQP_STATUS_OK) {
    log_message(LOG_ERROR, log_level, "Failed to open AMQP socket to %s:%d (%s)",
                uri->host, uri->port, amqp_error_string2(status));
    amqp_destroy_connection(conn);
    return NULL;
  }
  amqp_rpc_reply_t reply = amqp_login(conn,
                                      uri->vhost[0] ? uri->vhost : "/",
                                      0,
                                      131072,
                                      60,
                                      AMQP_SASL_METHOD_PLAIN,
                                      uri->username,
                                      uri->password);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, log_level, "AMQP login failed: %s", amqp_error_string2(reply.library_error));
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    return NULL;
  }
  return conn;
}

static int open_channel(amqp_connection_state_t conn, amqp_channel_t channel, int log_level) {
  amqp_channel_open(conn, channel);
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, log_level, "Failed to open channel %u", channel);
    return -1;
  }
  return 0;
}

static int declare_input(amqp_connection_state_t conn,
                         amqp_channel_t channel,
                         const char *queue,
                         const char *exchange,
                         const char *routing_key,
                         int log_level) {
  amqp_table_t args = {0};
  amqp_queue_declare(conn,
                     channel,
                     amqp_cstring_bytes(queue),
                     0,
                     1,
                     0,
                     0,
                     args);
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, log_level, "Queue declare failed for %s", queue);
    return -1;
  }
  if (exchange && *exchange) {
    amqp_exchange_declare(conn,
                          channel,
                          amqp_cstring_bytes(exchange),
                          amqp_cstring_bytes("topic"),
                          0,
                          1,
                          0,
                          0,
                          args);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_ERROR, log_level, "Exchange declare failed for %s", exchange);
      return -1;
    }
    amqp_queue_bind(conn,
                    channel,
                    amqp_cstring_bytes(queue),
                    amqp_cstring_bytes(exchange),
                    amqp_cstring_bytes(routing_key && *routing_key ? routing_key : "#"),
                    args);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_ERROR, log_level, "Queue bind failed for %s", queue);
      return -1;
    }
  }

  amqp_basic_qos(conn, channel, 0, 1, 0);
  reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, log_level, "Failed to set basic.qos");
    return -1;
  }

  amqp_basic_consume(conn,
                     channel,
                     amqp_cstring_bytes(queue),
                     amqp_empty_bytes,
                     0,
                     0,
                     0,
                     args);
  reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, log_level, "basic.consume failed for %s", queue);
    return -1;
  }
  return 0;
}

static int declare_output(amqp_connection_state_t conn,
                          amqp_channel_t channel,
                          const char *exchange,
                          const char *routing_key,
                          int log_level) {
  amqp_table_t args = {0};
  if (exchange && *exchange) {
    amqp_exchange_declare(conn,
                          channel,
                          amqp_cstring_bytes(exchange),
                          amqp_cstring_bytes("topic"),
                          0,
                          1,
                          0,
                          0,
                          args);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_ERROR, log_level, "Failed to declare exchange %s", exchange);
      return -1;
    }
  } else {
    amqp_queue_declare(conn,
                       channel,
                       amqp_cstring_bytes(routing_key),
                       0,
                       1,
                       0,
                       0,
                       args);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_ERROR, log_level, "Failed to declare output queue %s", routing_key);
      return -1;
    }
  }

  amqp_confirm_select(conn, channel);
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, log_level, "Failed to enable confirm select");
    return -1;
  }
  return 0;
}

static int json_extract_string(const char *json,
                               const char *key,
                               char *out,
                               size_t out_size) {
  char needle[64];
  snprintf(needle, sizeof(needle), "\"%s\":", key);
  const char *start = strstr(json, needle);
  if (!start) {
    return -1;
  }
  start += strlen(needle);
  while (*start == ' ' || *start == '\t') {
    ++start;
  }
  if (*start != '"') {
    return -1;
  }
  ++start;
  const char *cur = start;
  char tmp[MAX_JSON_VALUE];
  size_t ti = 0;
  while (*cur && ti + 1 < sizeof(tmp)) {
    if (*cur == '\\') {
      ++cur;
      if (*cur == '\0') {
        break;
      }
      char translated = *cur;
      switch (*cur) {
      case 'b': translated = '\b'; break;
      case 'f': translated = '\f'; break;
      case 'n': translated = '\n'; break;
      case 'r': translated = '\r'; break;
      case 't': translated = '\t'; break;
      case '\\': translated = '\\'; break;
      case '"': translated = '"'; break;
      case 'u':
        if (isxdigit((unsigned char)cur[1]) && isxdigit((unsigned char)cur[2]) &&
            isxdigit((unsigned char)cur[3]) && isxdigit((unsigned char)cur[4])) {
          int h1 = hex_value(cur[1]);
          int h2 = hex_value(cur[2]);
          int h3 = hex_value(cur[3]);
          int h4 = hex_value(cur[4]);
          int codepoint = (h1 << 12) | (h2 << 8) | (h3 << 4) | h4;
          if (codepoint < 128) {
            translated = (char)codepoint;
            cur += 4;
          } else {
            translated = '?';
            cur += 4;
          }
        }
        break;
      default:
        break;
      }
      tmp[ti++] = translated;
      ++cur;
      continue;
    }
    if (*cur == '"') {
      break;
    }
    tmp[ti++] = *cur++;
  }
  tmp[ti] = '\0';
  if (ti >= out_size) {
    return -1;
  }
  memcpy(out, tmp, ti + 1);
  return 0;
}

static int json_extract_optional_string(const char *json,
                                        const char *key,
                                        char *out,
                                        size_t out_size) {
  if (json_extract_string(json, key, out, out_size) == 0) {
    return 0;
  }
  out[0] = '\0';
  return -1;
}

static int parse_tracker_payload(const char *payload,
                                 char *tracker_id,
                                 size_t tracker_id_size,
                                 char *date_out,
                                 size_t date_size,
                                 char *time_out,
                                 size_t time_size,
                                 char *status_out,
                                 double *lat_out,
                                 double *lon_out,
                                 double *speed_out,
                                 double *direction_out,
                                 double *battery_out) {
  size_t len = strlen(payload);
  if (len < 4 || payload[0] != '[' || payload[len - 1] != ']') {
    return -1;
  }
  char *copy = (char *)malloc(len + 1);
  if (!copy) {
    return -1;
  }
  memcpy(copy, payload + 1, len - 2);
  copy[len - 2] = '\0';

  char *comma = strchr(copy, ',');
  if (!comma) {
    free(copy);
    return -1;
  }
  *comma = '\0';
  char *body = comma + 1;

  /* Header looks like SG*1234567890*XXXX*UD... */
  char *saveptr = NULL;
  char *token = strtok_r(copy, "*", &saveptr);
  int header_index = 0;
  int success = 0;
  while (token) {
    if (header_index == 1) {
      if (strlen(token) >= tracker_id_size) {
        break;
      }
      strcpy(tracker_id, token);
      success = 1;
      break;
    }
    token = strtok_r(NULL, "*", &saveptr);
    header_index++;
  }
  if (!success) {
    free(copy);
    return -1;
  }

  char *fields[32] = {0};
  int field_count = 0;
  saveptr = NULL;
  token = strtok_r(body, ",", &saveptr);
  while (token && field_count < (int)(sizeof(fields) / sizeof(fields[0]))) {
    fields[field_count++] = token;
    token = strtok_r(NULL, ",", &saveptr);
  }
  if (field_count < 13) {
    free(copy);
    return -1;
  }

  if (strlen(fields[0]) != 6 || strlen(fields[1]) != 6) {
    free(copy);
    return -1;
  }
  char day[3] = {fields[0][0], fields[0][1], '\0'};
  char month[3] = {fields[0][2], fields[0][3], '\0'};
  char year[3] = {fields[0][4], fields[0][5], '\0'};
  int year_val = atoi(year);
  year_val += (year_val >= 90) ? 1900 : 2000;
  snprintf(date_out, date_size, "%04d-%s-%s", year_val, month, day);

  char hour[3] = {fields[1][0], fields[1][1], '\0'};
  char minute[3] = {fields[1][2], fields[1][3], '\0'};
  char second[3] = {fields[1][4], fields[1][5], '\0'};
  snprintf(time_out, time_size, "%s:%s:%s", hour, minute, second);

  if (fields[2] && fields[2][0]) {
    *status_out = fields[2][0];
  } else {
    *status_out = 'V';
  }

  if (!fields[3] || !fields[4] || !fields[5] || !fields[6]) {
    free(copy);
    return -1;
  }

  char *endptr = NULL;
  double lat = strtod(fields[3], &endptr);
  if (endptr == fields[3]) {
    free(copy);
    return -1;
  }
  if (fields[4][0] == 'S' || fields[4][0] == 's') {
    lat = -lat;
  }
  double lon = strtod(fields[5], &endptr);
  if (endptr == fields[5]) {
    free(copy);
    return -1;
  }
  if (fields[6][0] == 'W' || fields[6][0] == 'w') {
    lon = -lon;
  }
  double speed = strtod(fields[7] ? fields[7] : "0", NULL);
  double direction = strtod(fields[8] ? fields[8] : "0", NULL);
  double battery = 0.0;
  if (field_count > 12 && fields[12]) {
    battery = strtod(fields[12], NULL);
  }

  *lat_out = lat;
  *lon_out = lon;
  *speed_out = speed;
  *direction_out = direction;
  *battery_out = battery;

  free(copy);
  return 0;
}

static char *build_output_json(const char *tracker_id,
                               const char *date,
                               const char *time,
                               char status,
                               double lat,
                               double lon,
                               double speed,
                               double direction,
                               double battery,
                               const char *timestamp) {
  const char *ts = (timestamp && *timestamp) ? timestamp : NULL;
  size_t extra = ts ? strlen(ts) + 25 : 0;
  size_t size = 256 + extra;
  char *out = (char *)malloc(size);
  if (!out) {
    return NULL;
  }
  if (ts) {
    snprintf(out,
             size,
             "{\"tracker_id\":\"%s\",\"date\":\"%s\",\"time\":\"%s\",\"status\":\"%c\",\"latitude\":%.6f,\"longitude\":%.6f,\"speed\":%.3f,\"direction\":%.3f,\"battery\":%.2f,\"timestamp\":\"%s\"}",
             tracker_id,
             date,
             time,
             status,
             lat,
             lon,
             speed,
             direction,
             battery,
             ts);
  } else {
    snprintf(out,
             size,
             "{\"tracker_id\":\"%s\",\"date\":\"%s\",\"time\":\"%s\",\"status\":\"%c\",\"latitude\":%.6f,\"longitude\":%.6f,\"speed\":%.3f,\"direction\":%.3f,\"battery\":%.2f}",
             tracker_id,
             date,
             time,
             status,
             lat,
             lon,
             speed,
             direction,
             battery);
  }
  return out;
}

static int wait_for_publish_ack(amqp_connection_state_t conn,
                                amqp_channel_t channel,
                                int log_level) {
  while (1) {
    amqp_frame_t frame;
    int wait_status = amqp_simple_wait_frame(conn, &frame);
    if (wait_status != AMQP_STATUS_OK) {
      log_message(LOG_ERROR, log_level, "Waiting for publish confirmation failed (%s)",
                  amqp_error_string2(wait_status));
      return -1;
    }
    if (frame.frame_type != AMQP_FRAME_METHOD) {
      continue;
    }
    if (frame.channel != channel) {
      continue;
    }
    switch (frame.payload.method.id) {
    case AMQP_BASIC_ACK_METHOD:
      return 0;
    case AMQP_BASIC_NACK_METHOD:
      log_message(LOG_ERROR, log_level, "Publish negatively acknowledged by broker");
      return -1;
    case AMQP_BASIC_RETURN_METHOD: {
      amqp_basic_return_t *ret = (amqp_basic_return_t *)frame.payload.method.decoded;
      amqp_message_t message;
      amqp_rpc_reply_t read_reply = amqp_read_message(conn, channel, &message, 0);
      if (read_reply.reply_type == AMQP_RESPONSE_NORMAL) {
        amqp_destroy_message(&message);
      }
      log_message(LOG_ERROR,
                  log_level,
                  "Publish returned by broker (code=%u text=%.*s exchange=%.*s routing_key=%.*s)",
                  ret->reply_code,
                  (int)ret->reply_text.len,
                  (char *)ret->reply_text.bytes,
                  (int)ret->exchange.len,
                  (char *)ret->exchange.bytes,
                  (int)ret->routing_key.len,
                  (char *)ret->routing_key.bytes);
      return -1;
    }
    default:
      if (log_level >= LOG_DEBUG) {
        log_message(LOG_DEBUG, log_level, "Ignoring frame method %u", frame.payload.method.id);
      }
      break;
    }
  }
}

static int publish_event(amqp_connection_state_t conn,
                         amqp_channel_t channel,
                         const char *exchange,
                         const char *routing_key,
                         const char *body,
                         size_t body_len,
                         int log_level) {
  amqp_basic_properties_t props;
  memset(&props, 0, sizeof(props));
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
  props.content_type = amqp_cstring_bytes("application/json");
  props.delivery_mode = 2;

  amqp_bytes_t message_bytes;
  message_bytes.len = body_len;
  message_bytes.bytes = (void *)body;

  amqp_bytes_t exchange_bytes = (exchange && *exchange) ? amqp_cstring_bytes(exchange) : amqp_empty_bytes;
  amqp_bytes_t routing_bytes = amqp_cstring_bytes(routing_key);

  int status = amqp_basic_publish(conn,
                                  channel,
                                  exchange_bytes,
                                  routing_bytes,
                                  1,
                                  0,
                                  &props,
                                  message_bytes);
  if (status != AMQP_STATUS_OK) {
    log_message(LOG_ERROR, log_level, "basic.publish failed: %s", amqp_error_string2(status));
    return -1;
  }
  if (wait_for_publish_ack(conn, channel, log_level) != 0) {
    return -1;
  }
  return 0;
}

static void close_connection(amqp_connection_state_t conn, amqp_channel_t channel) {
  if (!conn) {
    return;
  }
  amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
  amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(conn);
}

static int process_message(const char *json,
                           amqp_connection_state_t output_conn,
                           amqp_channel_t output_channel,
                           const char *output_exchange,
                           const char *output_routing_key,
                           int log_level) {
  char direction[64];
  if (json_extract_optional_string(json, "direction", direction, sizeof(direction)) == 0) {
    if (strcmp(direction, "client") != 0) {
      return 0; /* Nothing to do, ack. */
    }
  }

  char payload[MAX_JSON_VALUE];
  if (json_extract_string(json, "payload", payload, sizeof(payload)) != 0) {
    log_message(LOG_WARN, log_level, "Message missing payload field (%0.120s)", json);
    return 0;
  }

  char tracker_id[64];
  char date_buf[16];
  char time_buf[16];
  char status;
  double latitude = 0.0;
  double longitude = 0.0;
  double speed = 0.0;
  double direction_val = 0.0;
  double battery = 0.0;
  if (parse_tracker_payload(payload,
                             tracker_id,
                             sizeof(tracker_id),
                             date_buf,
                             sizeof(date_buf),
                             time_buf,
                             sizeof(time_buf),
                             &status,
                             &latitude,
                             &longitude,
                             &speed,
                             &direction_val,
                             &battery) != 0) {
    log_message(LOG_DEBUG, log_level, "Failed to parse tracker payload: %s", payload);
    return 0;
  }

  char timestamp[64];
  json_extract_optional_string(json, "timestamp", timestamp, sizeof(timestamp));

  char *out_json = build_output_json(tracker_id,
                                     date_buf,
                                     time_buf,
                                     status,
                                     latitude,
                                     longitude,
                                     speed,
                                     direction_val,
                                     battery,
                                     timestamp);
  if (!out_json) {
    return -1;
  }

  int rc = publish_event(output_conn,
                         output_channel,
                         output_exchange,
                         output_routing_key,
                         out_json,
                         strlen(out_json),
                         log_level);
  if (log_level >= LOG_DEBUG) {
    log_message(LOG_DEBUG, log_level, "Published %s", out_json);
  }
  free(out_json);
  if (rc != 0) {
    return -1;
  }
  if (log_level >= LOG_DEBUG) {
    log_message(LOG_DEBUG, log_level, "Published parsed tracker event for %s", tracker_id);
  }
  return 1;
}

static int parse_log_level(const char *level) {
  if (!level) {
    return LOG_INFO;
  }
  if (strcasecmp(level, "DEBUG") == 0) {
    return LOG_DEBUG;
  }
  if (strcasecmp(level, "INFO") == 0) {
    return LOG_INFO;
  }
  if (strcasecmp(level, "WARN") == 0 || strcasecmp(level, "WARNING") == 0) {
    return LOG_WARN;
  }
  if (strcasecmp(level, "ERROR") == 0) {
    return LOG_ERROR;
  }
  return LOG_INFO;
}

static void usage(const char *prog) {
  fprintf(stderr,
          "Usage: %s --input-uri URI [options]\n\n"
          "Options:\n"
          "  --input-uri URI            AMQP URI for consuming tracker events (required)\n"
          "  --input-queue NAME         Queue to consume (default: tracker.events)\n"
          "  --input-exchange NAME      Exchange to bind the input queue to\n"
          "  --input-routing-key KEY    Routing key for the input binding\n"
          "  --output-uri URI           AMQP URI for publishing (defaults to input URI)\n"
          "  --output-exchange NAME     Exchange for parsed events (default: default exchange)\n"
          "  --output-routing-key KEY   Routing key or queue name for parsed events\n"
          "  --log-level LEVEL          DEBUG, INFO, WARN, or ERROR (default: INFO)\n",
          prog);
}

static int parse_arguments(int argc, char **argv, struct daemon_config *cfg) {
  memset(cfg, 0, sizeof(*cfg));
  cfg->input_queue = strdup("tracker.events");
  cfg->output_routing_key = strdup("tracker.parsed");
  cfg->log_level = LOG_INFO;

  static struct option options[] = {
      {"input-uri", required_argument, NULL, 1},
      {"input-queue", required_argument, NULL, 2},
      {"input-exchange", required_argument, NULL, 3},
      {"input-routing-key", required_argument, NULL, 4},
      {"output-uri", required_argument, NULL, 5},
      {"output-exchange", required_argument, NULL, 6},
      {"output-routing-key", required_argument, NULL, 7},
      {"log-level", required_argument, NULL, 8},
      {0, 0, 0, 0}};

  while (1) {
    int opt_index = 0;
    int c = getopt_long(argc, argv, "", options, &opt_index);
    if (c == -1) {
      break;
    }
    switch (c) {
    case 1:
      free(cfg->input_uri);
      cfg->input_uri = strdup(optarg);
      break;
    case 2:
      free(cfg->input_queue);
      cfg->input_queue = strdup(optarg);
      break;
    case 3:
      free(cfg->input_exchange);
      cfg->input_exchange = strdup(optarg);
      break;
    case 4:
      free(cfg->input_routing_key);
      cfg->input_routing_key = strdup(optarg);
      break;
    case 5:
      free(cfg->output_uri);
      cfg->output_uri = strdup(optarg);
      break;
    case 6:
      free(cfg->output_exchange);
      cfg->output_exchange = strdup(optarg);
      break;
    case 7:
      free(cfg->output_routing_key);
      cfg->output_routing_key = strdup(optarg);
      break;
    case 8:
      cfg->log_level = parse_log_level(optarg);
      break;
    default:
      usage(argv[0]);
      return -1;
    }
  }

  if (!cfg->input_uri) {
    usage(argv[0]);
    return -1;
  }
  if (!cfg->output_uri) {
    cfg->output_uri = strdup(cfg->input_uri);
  }
  if (!cfg->input_routing_key || !*cfg->input_routing_key) {
    free(cfg->input_routing_key);
    cfg->input_routing_key = cfg->input_queue ? strdup(cfg->input_queue) : strdup("#");
  }
  if (!cfg->output_routing_key) {
    cfg->output_routing_key = strdup("tracker.parsed");
  }

  if (!cfg->input_queue || !cfg->output_routing_key) {
    return -1;
  }
  return 0;
}

static void free_config(struct daemon_config *cfg) {
  free(cfg->input_uri);
  free(cfg->input_queue);
  free(cfg->input_exchange);
  free(cfg->input_routing_key);
  free(cfg->output_uri);
  free(cfg->output_exchange);
  free(cfg->output_routing_key);
}

int main(int argc, char **argv) {
  struct daemon_config cfg;
  if (parse_arguments(argc, argv, &cfg) != 0) {
    return 1;
  }

  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  struct uri_parts input_uri_parts;
  struct uri_parts output_uri_parts;
  if (parse_amqp_uri(cfg.input_uri, &input_uri_parts) != 0) {
    log_message(LOG_ERROR, cfg.log_level, "Invalid input AMQP URI: %s", cfg.input_uri);
    free_config(&cfg);
    return 1;
  }
  if (parse_amqp_uri(cfg.output_uri, &output_uri_parts) != 0) {
    log_message(LOG_ERROR, cfg.log_level, "Invalid output AMQP URI: %s", cfg.output_uri);
    free_config(&cfg);
    return 1;
  }

  amqp_connection_state_t input_conn = open_connection(&input_uri_parts, cfg.log_level);
  if (!input_conn) {
    free_config(&cfg);
    return 1;
  }
  amqp_connection_state_t output_conn = open_connection(&output_uri_parts, cfg.log_level);
  if (!output_conn) {
    close_connection(input_conn, 1);
    free_config(&cfg);
    return 1;
  }

  const amqp_channel_t INPUT_CHANNEL = 1;
  const amqp_channel_t OUTPUT_CHANNEL = 1;

  if (open_channel(input_conn, INPUT_CHANNEL, cfg.log_level) != 0 ||
      open_channel(output_conn, OUTPUT_CHANNEL, cfg.log_level) != 0) {
    close_connection(input_conn, INPUT_CHANNEL);
    close_connection(output_conn, OUTPUT_CHANNEL);
    free_config(&cfg);
    return 1;
  }

  if (declare_input(input_conn,
                    INPUT_CHANNEL,
                    cfg.input_queue,
                    cfg.input_exchange,
                    cfg.input_routing_key,
                    cfg.log_level) != 0) {
    close_connection(input_conn, INPUT_CHANNEL);
    close_connection(output_conn, OUTPUT_CHANNEL);
    free_config(&cfg);
    return 1;
  }

  if (declare_output(output_conn,
                     OUTPUT_CHANNEL,
                     cfg.output_exchange,
                     cfg.output_routing_key,
                     cfg.log_level) != 0) {
    close_connection(input_conn, INPUT_CHANNEL);
    close_connection(output_conn, OUTPUT_CHANNEL);
    free_config(&cfg);
    return 1;
  }

  struct timeval timeout;
  timeout.tv_sec = 1;
  timeout.tv_usec = 0;

  while (!g_stop) {
    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(input_conn);
    amqp_rpc_reply_t reply = amqp_consume_message(input_conn, &envelope, &timeout, 0);
    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        reply.library_error == AMQP_STATUS_TIMEOUT) {
      /* Give the output connection a chance to process heartbeats. */
      amqp_frame_t frame;
      struct timeval zero = {0, 0};
      amqp_simple_wait_frame_noblock(output_conn, &frame, &zero);
      continue;
    }
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_WARN, cfg.log_level, "Consume error: %s", amqp_error_string2(reply.library_error));
      continue;
    }

    int result = 0;
    if (envelope.message.body.len > 0) {
      char *body = (char *)malloc(envelope.message.body.len + 1);
      if (body) {
        memcpy(body, envelope.message.body.bytes, envelope.message.body.len);
        body[envelope.message.body.len] = '\0';
        result = process_message(body,
                                 output_conn,
                                 OUTPUT_CHANNEL,
                                 cfg.output_exchange,
                                 cfg.output_routing_key,
                                 cfg.log_level);
        free(body);
      }
    }

    if (result >= 0) {
      amqp_basic_ack(input_conn, INPUT_CHANNEL, envelope.delivery_tag, 0);
    } else {
      amqp_basic_nack(input_conn, INPUT_CHANNEL, envelope.delivery_tag, 0, 1);
    }
    amqp_destroy_envelope(&envelope);
  }

  close_connection(input_conn, INPUT_CHANNEL);
  close_connection(output_conn, OUTPUT_CHANNEL);
  free_config(&cfg);
  return 0;
}
