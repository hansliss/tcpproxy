#include "observer.h"

#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <jansson.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "daemon_common.h"
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

static void observer_log(int level, int configured_level, const char *fmt, ...) {
  (void)level;
  (void)configured_level;
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
  fputc('\n', stderr);
}

/* Trim leading/trailing ASCII whitespace in-place. */
static inline char *trim_whitespace(char *value) { return daemon_trim_whitespace(value); }

static inline char *dup_string(const char *value) { return daemon_dup_string(value); }

static inline char *percent_decode_string(const char *input) {
  return daemon_percent_decode(input, 1);
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

static void observer_amqp_disconnect(struct observer_amqp *amqp) {
  if (!amqp || !amqp->connection) {
    return;
  }
  daemon_amqp_close(amqp->connection, amqp->channel);
  amqp->connection = NULL;
}

static int observer_amqp_connect(struct observer_amqp *amqp) {
  if (!amqp || !amqp->uri) {
    return -1;
  }
  struct daemon_amqp_config cfg = {
      .uri = amqp->uri,
      .exchange = (amqp->exchange && *amqp->exchange) ? amqp->exchange : NULL,
      .routing_key = amqp->publish_routing_key,
      .log_level = 0,
      .component_name = "observer"};

  amqp_connection_state_t conn = daemon_amqp_open(&cfg,
                                                  1,
                                                  (amqp->exchange && *amqp->exchange),
                                                  (amqp->queue && *amqp->queue) ? 1 : 0,
                                                  1,
                                                  observer_log);
  if (!conn) {
    return -1;
  }

  if (amqp->queue && *amqp->queue) {
    if (daemon_amqp_declare_queue(conn,
                                  1,
                                  amqp->queue,
                                  amqp->exchange,
                                  amqp->routing_key,
                                  0,
                                  0,
                                  "observer",
                                  observer_log) != 0) {
      daemon_amqp_close(conn, 1);
      return -1;
    }
  }

  amqp->connection = conn;
  amqp->channel = 1;
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

  if (daemon_amqp_publish(amqp->connection,
                          amqp->channel,
                          amqp->publish_exchange && *amqp->publish_exchange ? amqp->publish_exchange : NULL,
                          amqp->publish_routing_key,
                          body,
                          len,
                          &props,
                          1,
                          observer_log,
                          0,
                          "observer") != 0) {
    observer_log(0, 0, "observer: publish failed, reconnecting");
    if (observer_amqp_reconnect(amqp) != 0) {
      return -1;
    }
    if (daemon_amqp_publish(amqp->connection,
                            amqp->channel,
                            amqp->publish_exchange && *amqp->publish_exchange ? amqp->publish_exchange : NULL,
                            amqp->publish_routing_key,
                            body,
                            len,
                            &props,
                            1,
                            observer_log,
                            0,
                            "observer") != 0) {
      observer_log(0, 0, "observer: publish failed after reconnect");
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
  json_t *root = json_object();
  if (!root) {
    return;
  }
  const char *direction_str = direction_to_string(direction);
  json_t *timestamp_json = timestamp ? json_string(timestamp) : json_string("");
  json_t *direction_json = direction_str ? json_string(direction_str) : json_string("");
  json_t *ip_json = json_string(instance->client_ip);
  json_t *conn_json = json_string(instance->connection_id);
  json_t *payload_json = json_stringn_nocheck(message, message_len);
  if (!timestamp_json || !direction_json || !ip_json || !conn_json || !payload_json) {
    json_decref(timestamp_json);
    json_decref(direction_json);
    json_decref(ip_json);
    json_decref(conn_json);
    json_decref(payload_json);
    json_decref(root);
    return;
  }
  json_object_set_new(root, "timestamp", timestamp_json);
  json_object_set_new(root, "direction", direction_json);
  json_object_set_new(root, "client_ip", ip_json);
  json_object_set_new(root, "connection_id", conn_json);
  json_object_set_new(root, "payload", payload_json);

  char *serialized = json_dumps(root, JSON_COMPACT);
  if (serialized) {
    observer_amqp_publish(instance->global, serialized, strlen(serialized));
    free(serialized);
  }
  json_decref(root);
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
      observer_log(0, 0, "observer: cannot open log file %s: %s",
                   global->log_path,
                   strerror(errno));
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
