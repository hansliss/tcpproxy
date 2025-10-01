#include "observer.h"

#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tracker_parser.h"

/*
 * The observer module routes parsed tracker events to pluggable sinks. Two
 * modes are supported today: appending to a logfile and publishing JSON events
 * to RabbitMQ.
 */

enum observer_mode {
  OBSERVER_MODE_NONE = 0,
  OBSERVER_MODE_FILE,
  OBSERVER_MODE_AMQP
};

struct observer_global {
  enum observer_mode mode;
  char *log_path;
  char *amqp_config;
  struct observer_amqp *amqp;
};

struct observer_instance {
  struct observer_global *global;
  tracker_parser parser;
  tracker_parser_emit_fn emit_fn;
  char connection_id[64];
  char client_ip[128];
  int log_fd;
};

struct observer_amqp {
  char *uri;
  char *exchange;
  char *routing_key;
  char *queue;
  char *publish_exchange;
  char *publish_routing_key;
  amqp_connection_state_t connection;
  amqp_channel_t channel;
};

static int observer_amqp_publish(struct observer_global *global, const char *body, size_t len);

/* Trim leading/trailing ASCII whitespace in-place. */
static char *trim_whitespace(char *value) {
  if (!value) {
    return NULL;
  }
  while (*value == ' ' || *value == '\t') {
    value++;
  }
  size_t len = strlen(value);
  while (len > 0 && (value[len - 1] == ' ' || value[len - 1] == '\t')) {
    value[--len] = '\0';
  }
  return value;
}

static char *dup_string(const char *value) {
  if (!value) {
    return NULL;
  }
  char *copy = strdup(value);
  return copy;
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

static char *percent_decode_string(const char *input) {
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
    } else if (input[i] == '+') {
      out[oi++] = ' ';
      continue;
    }
    out[oi++] = input[i];
  }
  out[oi] = '\0';
  return out;
}

static int append_query_pair(char **dest, const char *pair) {
  if (!pair || !*pair) {
    return 0;
  }
  size_t old_len = (*dest) ? strlen(*dest) : 0;
  size_t pair_len = strlen(pair);
  size_t add = pair_len + (old_len ? 1 : 0);
  char *tmp = (char *)realloc(*dest, old_len + add + 1);
  if (!tmp) {
    return -1;
  }
  if (old_len) {
    tmp[old_len] = '&';
    memcpy(tmp + old_len + 1, pair, pair_len);
    tmp[old_len + 1 + pair_len] = '\0';
  } else {
    memcpy(tmp, pair, pair_len);
    tmp[pair_len] = '\0';
  }
  *dest = tmp;
  return 0;
}

struct uri_parts {
  char host[256];
  char username[256];
  char password[256];
  char vhost[256];
  int port;
};

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
      char *user = strndup(cred, ulen);
      char *pass = strndup(col + 1, plen);
      if (!user || !pass) {
        free(user);
        free(pass);
        return -1;
      }
      char *decoded_user = percent_decode_string(user);
      char *decoded_pass = percent_decode_string(pass);
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
      char *decoded_user = percent_decode_string(user);
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
      char *decoded = percent_decode_string(vhost_raw);
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

static void observer_amqp_disconnect(struct observer_amqp *amqp) {
  if (!amqp || !amqp->connection) {
    return;
  }
  amqp_channel_close(amqp->connection, amqp->channel, AMQP_REPLY_SUCCESS);
  amqp_connection_close(amqp->connection, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(amqp->connection);
  amqp->connection = NULL;
}

static int observer_amqp_connect(struct observer_amqp *amqp) {
  if (!amqp || !amqp->uri) {
    return -1;
  }

  struct uri_parts parts;
  if (parse_amqp_uri(amqp->uri, &parts) != 0) {
    fprintf(stderr, "observer: invalid AMQP URI %s\n", amqp->uri);
    return -1;
  }

  amqp_connection_state_t conn = amqp_new_connection();
  amqp_socket_t *socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    amqp_destroy_connection(conn);
    fprintf(stderr, "observer: failed to allocate AMQP socket\n");
    return -1;
  }
  int status = amqp_socket_open(socket, parts.host, parts.port);
  if (status != AMQP_STATUS_OK) {
    fprintf(stderr, "observer: cannot connect to %s:%d (%s)\n",
            parts.host,
            parts.port,
            amqp_error_string2(status));
    amqp_destroy_connection(conn);
    return -1;
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
    fprintf(stderr, "observer: AMQP login failed (%s)\n", amqp_error_string2(reply.library_error));
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    return -1;
  }

  amqp_channel_t channel = 1;
  amqp_channel_open(conn, channel);
  reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    fprintf(stderr, "observer: failed to open AMQP channel\n");
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    return -1;
  }

  amqp_table_t args = {0};
  if (amqp->queue && *amqp->queue) {
    amqp_queue_declare(conn,
                       channel,
                       amqp_cstring_bytes(amqp->queue),
                       0,
                       0,
                       0,
                       1,
                       args);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      fprintf(stderr, "observer: queue declare failed for %s\n", amqp->queue);
      amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      return -1;
    }
    if (amqp->exchange && *amqp->exchange) {
      amqp_exchange_declare(conn,
                            channel,
                            amqp_cstring_bytes(amqp->exchange),
                            amqp_cstring_bytes("topic"),
                            0,
                            1,
                            0,
                            0,
                            args);
      reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "observer: exchange declare failed for %s\n", amqp->exchange);
        amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
        return -1;
      }
      const char *bind_key = (amqp->routing_key && *amqp->routing_key) ? amqp->routing_key : amqp->queue;
      amqp_queue_bind(conn,
                      channel,
                      amqp_cstring_bytes(amqp->queue),
                      amqp_cstring_bytes(amqp->exchange),
                      amqp_cstring_bytes(bind_key),
                      args);
      reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "observer: queue bind failed for %s\n", amqp->queue);
        amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
        return -1;
      }
    }
  } else if (amqp->exchange && *amqp->exchange) {
    amqp_exchange_declare(conn,
                          channel,
                          amqp_cstring_bytes(amqp->exchange),
                          amqp_cstring_bytes("topic"),
                          0,
                          1,
                          0,
                          0,
                          args);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      fprintf(stderr, "observer: exchange declare failed for %s\n", amqp->exchange);
      amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      return -1;
    }
  }

  amqp->connection = conn;
  amqp->channel = channel;
  return 0;
}

static int observer_amqp_reconnect(struct observer_amqp *amqp) {
  observer_amqp_disconnect(amqp);
  return observer_amqp_connect(amqp);
}

static void observer_amqp_free(struct observer_amqp *amqp) {
  if (!amqp) {
    return;
  }
  observer_amqp_disconnect(amqp);
  free(amqp->uri);
  free(amqp->exchange);
  free(amqp->routing_key);
  free(amqp->queue);
  free(amqp->publish_exchange);
  free(amqp->publish_routing_key);
  free(amqp);
}

static struct observer_amqp *observer_amqp_new(const char *config) {
  if (!config) {
    return NULL;
  }

  char *copy = dup_string(config);
  if (!copy) {
    return NULL;
  }
  char *trimmed = trim_whitespace(copy);
  char *question = strchr(trimmed, '?');
  char *base_uri = NULL;
  char *query = NULL;
  if (question) {
    base_uri = strndup(trimmed, (size_t)(question - trimmed));
    query = question + 1;
  } else {
    base_uri = strdup(trimmed);
  }
  if (!base_uri) {
    free(copy);
    return NULL;
  }

  struct observer_amqp *amqp = calloc(1, sizeof(*amqp));
  if (!amqp) {
    free(base_uri);
    free(copy);
    return NULL;
  }

  char *sanitized_query = NULL;
  if (query && *query) {
    const char *cursor = query;
    while (*cursor) {
      const char *amp = strchr(cursor, '&');
      size_t pair_len = amp ? (size_t)(amp - cursor) : strlen(cursor);
      char *pair_raw = strndup(cursor, pair_len);
      char *pair = pair_raw ? strdup(pair_raw) : NULL;
      if (!pair_raw || !pair) {
        free(pair_raw);
        free(pair);
        free(sanitized_query);
        observer_amqp_free(amqp);
        free(base_uri);
        free(copy);
        return NULL;
      }
      char *eq = strchr(pair, '=');
      char *key = pair;
      char *value = "";
      if (eq) {
        *eq = '\0';
        value = eq + 1;
      }
      char *decoded_key = percent_decode_string(key);
      char *decoded_value = percent_decode_string(value);
      if (decoded_key) {
        if (strcmp(decoded_key, "exchange") == 0) {
          free(amqp->exchange);
          amqp->exchange = decoded_value ? strdup(decoded_value) : strdup("");
          free(decoded_value);
        } else if (strcmp(decoded_key, "routing_key") == 0) {
          free(amqp->routing_key);
          amqp->routing_key = decoded_value ? strdup(decoded_value) : strdup("");
          free(decoded_value);
        } else if (strcmp(decoded_key, "queue") == 0) {
          free(amqp->queue);
          amqp->queue = decoded_value ? strdup(decoded_value) : strdup("");
          free(decoded_value);
        } else {
          if (append_query_pair(&sanitized_query, pair_raw) != 0) {
            free(decoded_key);
            free(decoded_value);
            free(pair_raw);
          free(pair);
          free(sanitized_query);
          observer_amqp_free(amqp);
          free(base_uri);
          free(copy);
          return NULL;
          }
          free(decoded_value);
        }
        free(decoded_key);
      } else {
        free(decoded_value);
      }
      free(pair_raw);
      free(pair);
      if (!amp) {
        break;
      }
      cursor = amp + 1;
    }
  }

  size_t uri_len = strlen(base_uri) + 1;
  if (sanitized_query && *sanitized_query) {
    uri_len += 1 + strlen(sanitized_query);
  }
  amqp->uri = (char *)malloc(uri_len);
  if (!amqp->uri) {
    free(sanitized_query);
    observer_amqp_free(amqp);
    free(base_uri);
    free(copy);
    return NULL;
  }
  strcpy(amqp->uri, base_uri);
  if (sanitized_query && *sanitized_query) {
    strcat(amqp->uri, "?");
    strcat(amqp->uri, sanitized_query);
  }

  free(sanitized_query);
  free(base_uri);
  free(copy);

  if (!amqp->exchange) {
    amqp->exchange = strdup("");
  }
  if (!amqp->routing_key) {
    amqp->routing_key = strdup("");
  }
  if (!amqp->queue) {
    amqp->queue = strdup("");
  }
  if (!amqp->exchange || !amqp->routing_key || !amqp->queue) {
    observer_amqp_free(amqp);
    return NULL;
  }

  const char *publish_exchange = (amqp->exchange && *amqp->exchange) ? amqp->exchange : "";
  const char *publish_routing = NULL;
  if (amqp->routing_key && *amqp->routing_key) {
    publish_routing = amqp->routing_key;
  } else if (amqp->queue && *amqp->queue) {
    publish_routing = amqp->queue;
  } else {
    publish_routing = "observer.event";
  }

  amqp->publish_exchange = strdup(publish_exchange);
  amqp->publish_routing_key = strdup(publish_routing);
  if (!amqp->publish_exchange || !amqp->publish_routing_key) {
    observer_amqp_free(amqp);
    return NULL;
  }

  if (observer_amqp_connect(amqp) != 0) {
    observer_amqp_free(amqp);
    return NULL;
  }

  return amqp;
}

static int observer_amqp_publish(struct observer_global *global, const char *body, size_t len) {
  if (!global || !global->amqp || !body) {
    return -1;
  }
  struct observer_amqp *amqp = global->amqp;
  amqp_basic_properties_t props;
  memset(&props, 0, sizeof(props));
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
  props.content_type = amqp_cstring_bytes("application/json");

  amqp_bytes_t exchange_bytes = (amqp->publish_exchange && *amqp->publish_exchange)
                                    ? amqp_cstring_bytes(amqp->publish_exchange)
                                    : amqp_empty_bytes;
  amqp_bytes_t routing_bytes = amqp_cstring_bytes(amqp->publish_routing_key);
  amqp_bytes_t body_bytes = {.len = len, .bytes = (void *)body};

  int status = amqp_basic_publish(amqp->connection,
                                  amqp->channel,
                                  exchange_bytes,
                                  routing_bytes,
                                  0,
                                  0,
                                  &props,
                                  body_bytes);
  if (status != AMQP_STATUS_OK) {
    fprintf(stderr, "observer: publish failed (%s), retrying\n", amqp_error_string2(status));
    if (observer_amqp_reconnect(amqp) != 0) {
      return -1;
    }
    status = amqp_basic_publish(amqp->connection,
                                amqp->channel,
                                exchange_bytes,
                                routing_bytes,
                                0,
                                0,
                                &props,
                                body_bytes);
    if (status != AMQP_STATUS_OK) {
      fprintf(stderr, "observer: publish failed after reconnect (%s)\n", amqp_error_string2(status));
      return -1;
    }
  }
  return 0;
}

/* Extract the path portion from file=... observer config strings. */
static char *extract_path(const char *config_string) {
  char *copy = dup_string(config_string);
  if (!copy) {
    return NULL;
  }
  char *trimmed = trim_whitespace(copy);
  const char *prefixes[] = {"file=", "logfile="};
  for (size_t i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); i++) {
    size_t prefix_len = strlen(prefixes[i]);
    if (strncmp(trimmed, prefixes[i], prefix_len) == 0) {
      char *path = trim_whitespace(trimmed + prefix_len);
      if (*path == '\0') {
        free(copy);
        return NULL;
      }
      char *result = strdup(path);
      free(copy);
      return result;
    }
  }
  if (*trimmed == '\0') {
    free(copy);
    return NULL;
  }
  char *result = strdup(trimmed);
  free(copy);
  return result;
}

/* Parse the -O argument and allocate global observer state. */
int observer_global_init(const char *config_string, struct observer_global **out_global) {
  if (!out_global) {
    return -1;
  }
  *out_global = NULL;
  if (!config_string || *config_string == '\0') {
    return 0;
  }

  struct observer_global *global = calloc(1, sizeof(*global));
  if (!global) {
    return -1;
  }

  if (strncmp(config_string, "amqp=", 5) == 0) {
    global->mode = OBSERVER_MODE_AMQP;
    global->amqp_config = dup_string(config_string + 5);
    if (!global->amqp_config) {
      free(global);
      return -1;
    }
    global->amqp = observer_amqp_new(global->amqp_config);
    if (!global->amqp) {
      free(global->amqp_config);
      free(global);
      return -1;
    }
  } else if (strncmp(config_string, "amqp://", 7) == 0) {
    global->mode = OBSERVER_MODE_AMQP;
    global->amqp_config = dup_string(config_string);
    if (!global->amqp_config) {
      free(global);
      return -1;
    }
    global->amqp = observer_amqp_new(global->amqp_config);
    if (!global->amqp) {
      free(global->amqp_config);
      free(global);
      return -1;
    }
  } else {
    global->mode = OBSERVER_MODE_FILE;
    global->log_path = extract_path(config_string);
    if (!global->log_path) {
      free(global);
      return -1;
    }
  }

  *out_global = global;
  return 0;
}

void observer_global_free(struct observer_global *global) {
  if (!global) {
    return;
  }
  if (global->amqp) {
    observer_amqp_free(global->amqp);
  }
  free(global->log_path);
  free(global->amqp_config);
  free(global);
}

/* Return a stable string for logging AMQP direction. */
static const char *direction_to_string(enum observer_direction direction) {
  return direction == OBSERVER_DIR_CLIENT ? "client" : "server";
}

/* Minimal JSON string escaper for the helper protocol. */
static char *json_escape(const char *input, size_t len) {
  if (!input) {
    return strdup("");
  }
  size_t extra = 0;
  for (size_t i = 0; i < len; i++) {
    unsigned char c = (unsigned char)input[i];
    if (c == '"' || c == '\\' || c == '/' ) {
      extra += 1;
    } else if (c < 0x20) {
      extra += 5;
    }
  }
  size_t out_len = len + extra + 1;
  char *out = (char *)malloc(out_len);
  if (!out) {
    return NULL;
  }
  char *p = out;
  for (size_t i = 0; i < len; i++) {
    unsigned char c = (unsigned char)input[i];
    switch (c) {
    case '"':
      *(p++) = '\\';
      *(p++) = '"';
      break;
    case '\\':
      *(p++) = '\\';
      *(p++) = '\\';
      break;
    case '/':
      *(p++) = '\\';
      *(p++) = '/';
      break;
    case '\b':
      *(p++) = '\\';
      *(p++) = 'b';
      break;
    case '\f':
      *(p++) = '\\';
      *(p++) = 'f';
      break;
    case '\n':
      *(p++) = '\\';
      *(p++) = 'n';
      break;
    case '\r':
      *(p++) = '\\';
      *(p++) = 'r';
      break;
    case '\t':
      *(p++) = '\\';
      *(p++) = 't';
      break;
    default:
      if (c < 0x20) {
        sprintf(p, "\\u%04x", c);
        p += 6;
      } else {
        *(p++) = (char)c;
      }
      break;
    }
  }
  *p = '\0';
  return out;
}

/* Allow overriding the helper path through an environment variable. */
/* Write observer events to the logfile sink. */
static void emit_event_file(void *ctx,
                            enum observer_direction direction,
                            const char *timestamp,
                            const char *message,
                            size_t message_len) {
  struct observer_instance *instance = (struct observer_instance *)ctx;
  if (!instance || instance->log_fd == -1 || !timestamp || !message) {
    return;
  }
  const char *direction_str = direction_to_string(direction);
  size_t total = strlen(timestamp) + strlen(direction_str) + strlen(instance->client_ip)
                 + strlen(instance->connection_id) + message_len + 8;
  char *line = (char *)malloc(total);
  if (!line) {
    return;
  }
  int written = snprintf(line, total, "%s\t%s\t%s\t%s\t",
                         timestamp,
                         direction_str,
                         instance->client_ip,
                         instance->connection_id);
  if (written < 0) {
    free(line);
    return;
  }
  size_t offset = (size_t)written;
  if (message_len > 0) {
    memcpy(line + offset, message, message_len);
    offset += message_len;
  }
  line[offset++] = '\n';
  write(instance->log_fd, line, offset);
  free(line);
}

/* Serialize observer events as JSON and publish them to RabbitMQ. */
static void emit_event_amqp(void *ctx,
                            enum observer_direction direction,
                            const char *timestamp,
                            const char *message,
                            size_t message_len) {
  struct observer_instance *instance = (struct observer_instance *)ctx;
  if (!instance || !instance->global || !instance->global->amqp) {
    return;
  }
  char *escaped_timestamp = json_escape(timestamp, strlen(timestamp));
  char *escaped_direction = json_escape(direction_to_string(direction), strlen(direction_to_string(direction)));
  char *escaped_ip = json_escape(instance->client_ip, strlen(instance->client_ip));
  char *escaped_conn = json_escape(instance->connection_id, strlen(instance->connection_id));
  char *escaped_payload = json_escape(message, message_len);
  if (!escaped_timestamp || !escaped_direction || !escaped_ip || !escaped_conn || !escaped_payload) {
    free(escaped_timestamp);
    free(escaped_direction);
    free(escaped_ip);
    free(escaped_conn);
    free(escaped_payload);
    return;
  }
  size_t total = strlen(escaped_timestamp) + strlen(escaped_direction) +
                 strlen(escaped_ip) + strlen(escaped_conn) + strlen(escaped_payload) + 80;
  char *line = (char *)malloc(total);
  if (line) {
    int written = snprintf(line,
                           total,
                           "{\"timestamp\":\"%s\",\"direction\":\"%s\",\"client_ip\":\"%s\",\"connection_id\":\"%s\",\"payload\":\"%s\"}",
                           escaped_timestamp,
                           escaped_direction,
                           escaped_ip,
                           escaped_conn,
                           escaped_payload);
    if (written > 0) {
      observer_amqp_publish(instance->global, line, (size_t)written);
    }
    free(line);
  }
  free(escaped_timestamp);
  free(escaped_direction);
  free(escaped_ip);
  free(escaped_conn);
  free(escaped_payload);
}

struct observer_instance *observer_instance_create(struct observer_global *global,
                                                   const char *connection_id,
                                                   const char *client_ip) {
  if (!global) {
    return NULL;
  }
  if (global->mode == OBSERVER_MODE_NONE) {
    return NULL;
  }

  struct observer_instance *instance = calloc(1, sizeof(*instance));
  if (!instance) {
    return NULL;
  }
  instance->global = global;
  tracker_parser_init(&instance->parser);
  instance->log_fd = -1;

  if (connection_id) {
    snprintf(instance->connection_id, sizeof(instance->connection_id), "%s", connection_id);
  } else {
    instance->connection_id[0] = '\0';
  }
  if (client_ip) {
    snprintf(instance->client_ip, sizeof(instance->client_ip), "%s", client_ip);
  } else {
    instance->client_ip[0] = '\0';
  }

  if (global->mode == OBSERVER_MODE_FILE) {
    instance->log_fd = open(global->log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (instance->log_fd == -1) {
      fprintf(stderr, "observer: cannot open log file %s: %s\n",
              global->log_path, strerror(errno));
      free(instance);
      return NULL;
    }
    instance->emit_fn = emit_event_file;
  } else if (global->mode == OBSERVER_MODE_AMQP) {
    if (!global->amqp) {
      free(instance);
      return NULL;
    }
    instance->emit_fn = emit_event_amqp;
  }

  return instance;
}

void observer_instance_record(struct observer_instance *instance,
                              enum observer_direction direction,
                              const char *timestamp,
                              const char *data,
                              size_t data_len) {
  if (!instance || !data || data_len == 0 || !instance->emit_fn) {
    return;
  }
  tracker_parser_push(&instance->parser,
                      direction,
                      timestamp,
                      data,
                      data_len,
                      instance->emit_fn,
                      instance);
}

void observer_instance_close(struct observer_instance *instance) {
  if (!instance) {
    return;
  }
  if (instance->log_fd != -1) {
    close(instance->log_fd);
  }
  free(instance);
}
