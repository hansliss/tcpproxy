#include "daemon_common.h"

#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

char *daemon_dup_string(const char *value) {
  if (!value) {
    return NULL;
  }
  return strdup(value);
}

char *daemon_trim_whitespace(char *value) {
  if (!value) {
    return NULL;
  }
  while (*value == ' ' || *value == '\t' || *value == '\r' || *value == '\n') {
    value++;
  }
  size_t len = strlen(value);
  while (len > 0 && (value[len - 1] == ' ' || value[len - 1] == '\t' ||
                     value[len - 1] == '\r' || value[len - 1] == '\n')) {
    value[--len] = '\0';
  }
  return value;
}

static char *g_pidfile_path = NULL;

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

char *daemon_percent_decode(const char *input, int plus_to_space) {
  if (!input) {
    return NULL;
  }
  size_t len = strlen(input);
  char *out = (char *)malloc(len + 1);
  if (!out) {
    return NULL;
  }
  size_t oi = 0;
  for (size_t i = 0; i < len; i++) {
    if (input[i] == '%' && i + 2 < len && isxdigit((unsigned char)input[i + 1]) && isxdigit((unsigned char)input[i + 2])) {
      int hi = hex_value(input[i + 1]);
      int lo = hex_value(input[i + 2]);
      if (hi >= 0 && lo >= 0) {
        out[oi++] = (char)((hi << 4) | lo);
        i += 2;
        continue;
      }
    }
    if (plus_to_space && input[i] == '+') {
      out[oi++] = ' ';
    } else {
      out[oi++] = input[i];
    }
  }
  out[oi] = '\0';
  return out;
}

int daemon_parse_amqp_uri(const char *uri, struct daemon_uri_parts *out) {
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
      char *user = strndup(cred, (size_t)(col - cred));
      char *pass = strndup(col + 1, (size_t)(at - col - 1));
      if (!user || !pass) {
        free(user);
        free(pass);
        return -1;
      }
      char *decoded_user = daemon_percent_decode(user, 1);
      char *decoded_pass = daemon_percent_decode(pass, 1);
      free(user);
      free(pass);
      if (!decoded_user || !decoded_pass) {
        free(decoded_user);
        free(decoded_pass);
        return -1;
      }
      strncpy(out->username, decoded_user, sizeof(out->username) - 1);
      strncpy(out->password, decoded_pass, sizeof(out->password) - 1);
      free(decoded_user);
      free(decoded_pass);
    } else {
      char *user = strndup(cred, (size_t)(at - cred));
      if (!user) {
        return -1;
      }
      char *decoded_user = daemon_percent_decode(user, 1);
      free(user);
      if (!decoded_user) {
        return -1;
      }
      strncpy(out->username, decoded_user, sizeof(out->username) - 1);
      free(decoded_user);
    }
    cursor = at + 1;
  }

  const char *slash = strchr(cursor, '/');
  const char *host_part = cursor;
  char hostbuf[256];
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
    strncpy(out->host, hostbuf, sizeof(out->host) - 1);
  }

  if (slash) {
    const char *qmark = cursor ? strchr(cursor, '?') : NULL;
    size_t vlen = cursor ? (qmark ? (size_t)(qmark - cursor) : strlen(cursor)) : 0;
    if (vlen > 0) {
      char *vhost_raw = strndup(cursor, vlen);
      if (!vhost_raw) {
        return -1;
      }
      char *decoded = daemon_percent_decode(vhost_raw, 1);
      free(vhost_raw);
      if (!decoded) {
        return -1;
      }
      strncpy(out->vhost, decoded, sizeof(out->vhost) - 1);
      free(decoded);
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

int daemon_parse_config_file(const char *path,
                             daemon_kv_handler handler,
                             void *ctx,
                             daemon_log_fn log_fn,
                             int log_level,
                             const char *component_name) {
  if (!path || !handler) {
    return -1;
  }
  FILE *fp = fopen(path, "r");
  if (!fp) {
    if (log_fn) {
      log_fn(0, log_level, "%s: failed to open config %s: %s",
             component_name ? component_name : "daemon",
             path,
             strerror(errno));
    }
    return -1;
  }
  char line[4096];
  int lineno = 0;
  while (fgets(line, sizeof(line), fp)) {
    lineno++;
    char *trimmed = daemon_trim_whitespace(line);
    if (*trimmed == '\0' || *trimmed == '#' || *trimmed == ';') {
      continue;
    }
    char *eq = strchr(trimmed, '=');
    if (!eq) {
      if (log_fn) {
        log_fn(1, log_level, "%s: ignoring malformed line %d in %s",
               component_name ? component_name : "daemon",
               lineno,
               path);
      }
      continue;
    }
    *eq = '\0';
    char *key = daemon_trim_whitespace(trimmed);
    char *value = daemon_trim_whitespace(eq + 1);
    if (handler(ctx, key, value) != 0 && log_fn) {
      log_fn(1, log_level, "%s: unknown config key '%s' in %s",
             component_name ? component_name : "daemon",
             key,
             path);
    }
  }
  fclose(fp);
  return 0;
}

char *daemon_find_config_path(int argc, char **argv) {
  for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];
    if (strcmp(arg, "--config") == 0) {
      if (i + 1 < argc) {
        return daemon_dup_string(argv[i + 1]);
      }
    } else if (strncmp(arg, "--config=", 9) == 0) {
      return daemon_dup_string(arg + 9);
    }
  }
  return NULL;
}

static void pidfile_log(daemon_log_fn log_fn,
                        int level,
                        int configured_level,
                        const char *component_name,
                        const char *path,
                        const char *error) {
  if (log_fn) {
    log_fn(level, configured_level, "%s: %s: %s", component_name ? component_name : "daemon", path, error);
  } else {
    fprintf(stderr, "%s: %s: %s\n", component_name ? component_name : "daemon", path, error);
  }
}

int daemon_pidfile_create(const char *path,
                          daemon_log_fn log_fn,
                          int log_level,
                          const char *component_name) {
  if (!path || !*path) {
    return 0;
  }

  FILE *fp = fopen(path, "w");
  if (!fp) {
    pidfile_log(log_fn, 0, log_level, component_name, path, strerror(errno));
    return -1;
  }
  int ok = (fprintf(fp, "%ld\n", (long)getpid()) >= 0);
  if (fclose(fp) != 0) {
    ok = 0;
  }
  if (!ok) {
    pidfile_log(log_fn, 0, log_level, component_name, path, strerror(errno));
    unlink(path);
    return -1;
  }

  char *dup = daemon_dup_string(path);
  if (!dup) {
    pidfile_log(log_fn, 0, log_level, component_name, path, "unable to allocate pidfile path");
    unlink(path);
    return -1;
  }

  free(g_pidfile_path);
  g_pidfile_path = dup;
  return 0;
}

void daemon_pidfile_cleanup(void) {
  if (g_pidfile_path) {
    unlink(g_pidfile_path);
    free(g_pidfile_path);
    g_pidfile_path = NULL;
  }
}

const char *daemon_pidfile_path(void) {
  return g_pidfile_path;
}

static void amqp_log_error(daemon_log_fn log_fn,
                           int level,
                           int configured_level,
                           const char *component,
                           const char *message,
                           const char *detail) {
  if (log_fn) {
    log_fn(level,
           configured_level,
           "%s: %s%s%s",
           component ? component : "daemon",
           message,
           detail ? ": " : "",
           detail ? detail : "");
  } else {
    fprintf(stderr,
            "%s: %s%s%s\n",
            component ? component : "daemon",
            message,
            detail ? ": " : "",
            detail ? detail : "");
  }
}

amqp_connection_state_t daemon_amqp_open(const struct daemon_amqp_config *cfg,
                                         amqp_channel_t channel,
                                         int declare_exchange,
                                         int durable_queue,
                                         int enable_confirms,
                                         daemon_log_fn log_fn) {
  if (!cfg || !cfg->uri) {
    return NULL;
  }

  struct daemon_uri_parts parts;
  if (daemon_parse_amqp_uri(cfg->uri, &parts) != 0) {
    amqp_log_error(log_fn,
                   0,
                   cfg->log_level,
                   cfg->component_name,
                   "Invalid AMQP URI",
                   cfg->uri);
    return NULL;
  }

  amqp_connection_state_t conn = amqp_new_connection();
  amqp_socket_t *socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    amqp_destroy_connection(conn);
    amqp_log_error(log_fn,
                   0,
                   cfg->log_level,
                   cfg->component_name,
                   "Failed to allocate AMQP socket",
                   NULL);
    return NULL;
  }

  int status = amqp_socket_open(socket, parts.host, parts.port);
  if (status != AMQP_STATUS_OK) {
    amqp_destroy_connection(conn);
    amqp_log_error(log_fn,
                   0,
                   cfg->log_level,
                   cfg->component_name,
                   "Failed to open AMQP socket",
                   amqp_error_string2(status));
    return NULL;
  }

  amqp_rpc_reply_t reply = amqp_login(conn,
                                      parts.vhost[0] ? parts.vhost : "/",
                                      0,
                                      131072,
                                      60,
                                      AMQP_SASL_METHOD_PLAIN,
                                      parts.username,
                                      parts.password);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    amqp_log_error(log_fn,
                   0,
                   cfg->log_level,
                   cfg->component_name,
                   "AMQP login failed",
                   amqp_error_string2(reply.library_error));
    return NULL;
  }

  amqp_channel_open(conn, channel);
  reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    amqp_log_error(log_fn,
                   0,
                   cfg->log_level,
                   cfg->component_name,
                   "Failed to open AMQP channel",
                   amqp_error_string2(reply.library_error));
    return NULL;
  }

  if (enable_confirms) {
    amqp_confirm_select(conn, channel);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      amqp_log_error(log_fn,
                     0,
                     cfg->log_level,
                     cfg->component_name,
                     "Failed to enable confirm mode",
                     amqp_error_string2(reply.library_error));
      return NULL;
    }
  }

  if (declare_exchange && cfg->exchange && *cfg->exchange) {
    amqp_exchange_declare(conn,
                          channel,
                          amqp_cstring_bytes(cfg->exchange),
                          amqp_cstring_bytes("topic"),
                          0,
                          1,
                          0,
                          0,
                          (amqp_table_t){0});
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      amqp_log_error(log_fn,
                     0,
                     cfg->log_level,
                     cfg->component_name,
                     "Failed to declare exchange",
                     amqp_error_string2(reply.library_error));
      return NULL;
    }
  } else if (!cfg->exchange && cfg->routing_key && *cfg->routing_key) {
    amqp_queue_declare(conn,
                       channel,
                       amqp_cstring_bytes(cfg->routing_key),
                       0,
                       durable_queue ? 1 : 0,
                       0,
                       durable_queue ? 0 : 1,
                       (amqp_table_t){0});
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      amqp_log_error(log_fn,
                     0,
                     cfg->log_level,
                     cfg->component_name,
                     "Failed to declare queue",
                     amqp_error_string2(reply.library_error));
      return NULL;
    }
  }

  return conn;
}

void daemon_amqp_close(amqp_connection_state_t conn, amqp_channel_t channel) {
  if (!conn) {
    return;
  }
  amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
  amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(conn);
}

int daemon_amqp_declare_queue(amqp_connection_state_t conn,
                              amqp_channel_t channel,
                              const char *queue,
                              const char *exchange,
                              const char *routing_key,
                              int durable,
                              int log_level,
                              const char *component_name,
                              daemon_log_fn log_fn) {
  if (!conn || !queue || !*queue) {
    return -1;
  }

  amqp_queue_declare(conn,
                     channel,
                     amqp_cstring_bytes(queue),
                     0,
                     durable ? 1 : 0,
                     0,
                     0,
                     (amqp_table_t){0});
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    amqp_log_error(log_fn,
                   0,
                   log_level,
                   component_name,
                   "Queue declare failed",
                   amqp_error_string2(reply.library_error));
    return -1;
  }
  if (exchange && *exchange) {
    const char *bind_key = (routing_key && *routing_key) ? routing_key : queue;
    amqp_queue_bind(conn,
                    channel,
                    amqp_cstring_bytes(queue),
                    amqp_cstring_bytes(exchange),
                    amqp_cstring_bytes(bind_key),
                    (amqp_table_t){0});
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      amqp_log_error(log_fn,
                     0,
                     log_level,
                     component_name,
                     "Queue bind failed",
                     amqp_error_string2(reply.library_error));
      return -1;
    }
  }
  return 0;
}

int daemon_amqp_wait_heartbeat(amqp_connection_state_t conn) {
  if (!conn) {
    return 0;
  }
  struct timeval zero = {0, 0};
  amqp_frame_t frame;
  int rc = amqp_simple_wait_frame_noblock(conn, &frame, &zero);
  if (rc == AMQP_STATUS_TIMEOUT || rc == AMQP_STATUS_OK) {
    return 0;
  }
  return rc;
}

static int daemon_wait_for_publish_ack(amqp_connection_state_t conn,
                                       amqp_channel_t channel,
                                       daemon_log_fn log_fn,
                                       int log_level,
                                       const char *component_name) {
  while (1) {
    amqp_frame_t frame;
    int wait_status = amqp_simple_wait_frame(conn, &frame);
    if (wait_status != AMQP_STATUS_OK) {
      amqp_log_error(log_fn,
                     0,
                     log_level,
                     component_name,
                     "Waiting for publish confirmation failed",
                     amqp_error_string2(wait_status));
      return -1;
    }
    if (frame.frame_type != AMQP_FRAME_METHOD || frame.channel != channel) {
      continue;
    }
    switch (frame.payload.method.id) {
    case AMQP_BASIC_ACK_METHOD:
      return 0;
    case AMQP_BASIC_NACK_METHOD:
      amqp_log_error(log_fn,
                     0,
                     log_level,
                     component_name,
                     "Publish negatively acknowledged by broker",
                     NULL);
      return -1;
    case AMQP_BASIC_RETURN_METHOD: {
      amqp_basic_return_t *ret = (amqp_basic_return_t *)frame.payload.method.decoded;
      amqp_message_t message;
      amqp_rpc_reply_t read_reply = amqp_read_message(conn, channel, &message, 0);
      if (read_reply.reply_type == AMQP_RESPONSE_NORMAL) {
        amqp_destroy_message(&message);
      }
      char detail[256];
      snprintf(detail,
               sizeof(detail),
               "code=%u exchange=%.*s routing_key=%.*s",
               ret->reply_code,
               (int)ret->exchange.len,
               (char *)ret->exchange.bytes,
               (int)ret->routing_key.len,
               (char *)ret->routing_key.bytes);
      amqp_log_error(log_fn,
                     0,
                     log_level,
                     component_name,
                     "Publish returned by broker",
                     detail);
      return -1;
    }
    default:
      break;
    }
  }
}

int daemon_amqp_publish(amqp_connection_state_t conn,
                        amqp_channel_t channel,
                        const char *exchange,
                        const char *routing_key,
                        const void *body,
                        size_t body_len,
                        const amqp_basic_properties_t *props,
                        int wait_for_confirm,
                        daemon_log_fn log_fn,
                        int log_level,
                        const char *component_name) {
  if (!conn || !body) {
    return -1;
  }

  amqp_bytes_t exchange_bytes = (exchange && *exchange) ? amqp_cstring_bytes(exchange) : amqp_empty_bytes;
  amqp_bytes_t routing_bytes = amqp_cstring_bytes(routing_key);
  amqp_bytes_t body_bytes = {.len = body_len, .bytes = (void *)body};

  int status = amqp_basic_publish(conn,
                                  channel,
                                  exchange_bytes,
                                  routing_bytes,
                                  wait_for_confirm ? 1 : 0,
                                  0,
                                  props ? props : &(amqp_basic_properties_t){0},
                                  body_bytes);
  if (status != AMQP_STATUS_OK) {
    amqp_log_error(log_fn,
                   0,
                   log_level,
                   component_name,
                   "basic.publish failed",
                   amqp_error_string2(status));
    return -1;
  }

  if (wait_for_confirm) {
    return daemon_wait_for_publish_ack(conn, channel, log_fn, log_level, component_name);
  }
  return 0;
}
