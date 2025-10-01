#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "daemon_common.h"

struct location {
  char *name;
  double latitude;
  double longitude;
};

struct location_resolver {
  struct location *entries;
  size_t count;
};

struct daemon_config {
  char *input_uri;
  char *input_queue;
  char *input_exchange;
  char *input_routing_key;
  char *output_uri;
  char *output_exchange;
  char *output_routing_key;
  char *kml_path;
  char *pidfile_path;
  char *config_path;
  int log_level;
};

enum {
  LOG_ERROR = 0,
  LOG_WARN = 1,
  LOG_INFO = 2,
  LOG_DEBUG = 3
};

static volatile sig_atomic_t g_stop = 0;

static int parse_log_level(const char *level);

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
  fprintf(stderr, "%s %-5s cat_location_daemon: ", timestamp, tag);
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

static void set_string(char **dest, const char *value) {
  if (!dest) {
    return;
  }
  free(*dest);
  *dest = value ? daemon_dup_string(value) : NULL;
}

static int apply_config_key(struct daemon_config *cfg, const char *key, const char *value) {
  if (!cfg || !key) {
    return -1;
  }
  if (!value) {
    value = "";
  }
  if (strcmp(key, "input-uri") == 0) {
    set_string(&cfg->input_uri, value);
  } else if (strcmp(key, "input-queue") == 0) {
    set_string(&cfg->input_queue, value);
  } else if (strcmp(key, "input-exchange") == 0) {
    set_string(&cfg->input_exchange, value);
  } else if (strcmp(key, "input-routing-key") == 0) {
    set_string(&cfg->input_routing_key, value);
  } else if (strcmp(key, "output-uri") == 0) {
    set_string(&cfg->output_uri, value);
  } else if (strcmp(key, "output-exchange") == 0) {
    set_string(&cfg->output_exchange, value);
  } else if (strcmp(key, "output-routing-key") == 0) {
    set_string(&cfg->output_routing_key, value);
  } else if (strcmp(key, "kml") == 0) {
    set_string(&cfg->kml_path, value);
  } else if (strcmp(key, "pidfile") == 0 || strcmp(key, "pid-file") == 0) {
    set_string(&cfg->pidfile_path, value);
  } else if (strcmp(key, "log-level") == 0) {
    cfg->log_level = parse_log_level(value);
  } else {
    return -1;
  }
  return 0;
}

static void location_resolver_free(struct location_resolver *resolver) {
  if (!resolver) {
    return;
  }
  for (size_t i = 0; i < resolver->count; i++) {
    free(resolver->entries[i].name);
  }
  free(resolver->entries);
  resolver->entries = NULL;
  resolver->count = 0;
}

static int location_resolver_add(struct location_resolver *resolver,
                                 const char *name,
                                 double latitude,
                                 double longitude) {
  if (!resolver || !name) {
    return -1;
  }
  char *dup = daemon_dup_string(name);
  if (!dup) {
    return -1;
  }
  struct location *new_entries = (struct location *)realloc(resolver->entries,
                                                            (resolver->count + 1) * sizeof(*new_entries));
  if (!new_entries) {
    free(dup);
    return -1;
  }
  resolver->entries = new_entries;
  resolver->entries[resolver->count].name = dup;
  resolver->entries[resolver->count].latitude = latitude;
  resolver->entries[resolver->count].longitude = longitude;
  resolver->count++;
  return 0;
}

static void location_resolver_traverse(xmlNodePtr node, struct location_resolver *resolver) {
  for (xmlNodePtr cur = node; cur; cur = cur->next) {
    if (cur->type == XML_ELEMENT_NODE && xmlStrcmp(cur->name, BAD_CAST "Placemark") == 0) {
      char *name_text = NULL;
      char *coord_text = NULL;
      for (xmlNodePtr child = cur->children; child; child = child->next) {
        if (child->type != XML_ELEMENT_NODE) {
          continue;
        }
        if (xmlStrcmp(child->name, BAD_CAST "name") == 0) {
          xmlChar *content = xmlNodeGetContent(child);
          if (content) {
            free(name_text);
            name_text = (char *)content;
          }
        } else if (xmlStrcmp(child->name, BAD_CAST "Point") == 0) {
          for (xmlNodePtr point_child = child->children; point_child; point_child = point_child->next) {
            if (point_child->type != XML_ELEMENT_NODE) {
              continue;
            }
            if (xmlStrcmp(point_child->name, BAD_CAST "coordinates") == 0) {
              xmlChar *content = xmlNodeGetContent(point_child);
              if (content) {
                free(coord_text);
                coord_text = (char *)content;
              }
            }
          }
        }
      }
      if (name_text && coord_text) {
        char *coords = coord_text;
        while (*coords && isspace((unsigned char)*coords)) {
          coords++;
        }
        char *comma = strchr(coords, ',');
        if (comma) {
          *comma = '\0';
          char *lat_str = comma + 1;
          char *lon_str = coords;
          char *next_comma = strchr(lat_str, ',');
          if (next_comma) {
            *next_comma = '\0';
          }
          double longitude = strtod(lon_str, NULL);
          double latitude = strtod(lat_str, NULL);
          if (location_resolver_add(resolver, name_text, latitude, longitude) != 0) {
            free(name_text);
            free(coord_text);
            return;
          }
        }
      }
      free(name_text);
      free(coord_text);
    }
    location_resolver_traverse(cur->children, resolver);
  }
}

static int location_resolver_init(struct location_resolver *resolver, const char *kml_path) {
  memset(resolver, 0, sizeof(*resolver));
  xmlDocPtr doc = xmlReadFile(kml_path, NULL, 0);
  if (!doc) {
    return -1;
  }
  xmlNodePtr root = xmlDocGetRootElement(doc);
  if (!root) {
    xmlFreeDoc(doc);
    return -1;
  }
  location_resolver_traverse(root, resolver);
  xmlFreeDoc(doc);
  if (resolver->count == 0) {
    return -1;
  }
  return 0;
}

static const char *location_resolver_resolve(struct location_resolver *resolver,
                                             double latitude,
                                             double longitude) {
  if (!resolver || resolver->count == 0) {
    return NULL;
  }
  size_t best_index = 0;
  double best_distance = -1.0;
  for (size_t i = 0; i < resolver->count; i++) {
    double dlat = resolver->entries[i].latitude - latitude;
    double dlon = resolver->entries[i].longitude - longitude;
    double distance = dlat * dlat + dlon * dlon;
    if (best_distance < 0 || distance < best_distance) {
      best_distance = distance;
      best_index = i;
    }
  }
  return resolver->entries[best_index].name;
}

static char *current_timestamp_utc(void) {
  time_t now = time(NULL);
  struct tm tm;
  gmtime_r(&now, &tm);
  char buffer[32];
  strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm);
  return strdup(buffer);
}

static int hex_value_json(char c) {
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

static int json_extract_string(const char *json, const char *key, char *out, size_t out_size) {
  char needle[64];
  snprintf(needle, sizeof(needle), "\"%s\":", key);
  const char *start = strstr(json, needle);
  if (!start) {
    return -1;
  }
  start += strlen(needle);
  while (*start == ' ' || *start == '\t') {
    start++;
  }
  if (*start != '"') {
    return -1;
  }
  start++;
  const char *cur = start;
  char *ptr = out;
  size_t remaining = out_size;
  if (remaining == 0) {
    return -1;
  }
  remaining--;
  while (*cur && remaining > 0) {
    if (*cur == '\\') {
      cur++;
      if (!*cur) {
        break;
      }
      char translated = *cur;
      switch (*cur) {
      case 'b': translated = '\b'; break;
      case 'f': translated = '\f'; break;
      case 'n': translated = '\n'; break;
      case 'r': translated = '\r'; break;
      case 't': translated = '\t'; break;
      case '"': translated = '"'; break;
      case '\\': translated = '\\'; break;
      case 'u': {
        if (isxdigit((unsigned char)cur[1]) && isxdigit((unsigned char)cur[2]) &&
            isxdigit((unsigned char)cur[3]) && isxdigit((unsigned char)cur[4])) {
          int h1 = hex_value_json(cur[1]);
          int h2 = hex_value_json(cur[2]);
          int h3 = hex_value_json(cur[3]);
          int h4 = hex_value_json(cur[4]);
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
      }
      default:
        break;
      }
      *ptr++ = translated;
      remaining--;
      cur++;
      continue;
    }
    if (*cur == '"') {
      *ptr = '\0';
      return 0;
    }
    *ptr++ = *cur++;
    remaining--;
  }
  *ptr = '\0';
  return 0;
}

static int json_extract_optional_string(const char *json, const char *key, char *out, size_t out_size) {
  if (json_extract_string(json, key, out, out_size) == 0) {
    return 0;
  }
  if (out_size > 0) {
    out[0] = '\0';
  }
  return -1;
}

static int parse_tracker_payload(const char *payload,
                                 char *tracker_id,
                                 size_t tracker_id_size,
                                 double *latitude,
                                 double *longitude) {
  if (!payload || !tracker_id || tracker_id_size == 0) {
    return -1;
  }
  size_t len = strlen(payload);
  if (len < 2 || payload[0] != '[' || payload[len - 1] != ']') {
    return -1;
  }
  const char *device_start = payload + 1;
  if (strncmp(device_start, "SG*", 3) != 0) {
    return -1;
  }
  device_start += 3;
  const char *device_end = strchr(device_start, '*');
  if (!device_end) {
    return -1;
  }
  size_t device_len = (size_t)(device_end - device_start);
  if (device_len == 0 || device_len >= tracker_id_size) {
    return -1;
  }
  memcpy(tracker_id, device_start, device_len);
  tracker_id[device_len] = '\0';

  const char *field = strchr(device_end, ',');
  if (!field) {
    return -1;
  }
  field++; /* date */
  const char *next = strchr(field, ',');
  if (!next) {
    return -1;
  }
  field = next + 1; /* time */
  next = strchr(field, ',');
  if (!next) {
    return -1;
  }
  field = next + 1; /* status */
  next = strchr(field, ',');
  if (!next) {
    return -1;
  }
  const char *lat_start = next + 1;
  const char *lat_end = strchr(lat_start, ',');
  if (!lat_end) {
    return -1;
  }
  char lat_buf[32];
  size_t lat_len = (size_t)(lat_end - lat_start);
  if (lat_len >= sizeof(lat_buf)) {
    return -1;
  }
  memcpy(lat_buf, lat_start, lat_len);
  lat_buf[lat_len] = '\0';

  const char *ns_field = lat_end + 1;
  const char *ns_end = strchr(ns_field, ',');
  if (!ns_end) {
    return -1;
  }
  const char ns_char = *ns_field;

  const char *lon_start = ns_end + 1;
  const char *lon_end = strchr(lon_start, ',');
  if (!lon_end) {
    return -1;
  }
  char lon_buf[32];
  size_t lon_len = (size_t)(lon_end - lon_start);
  if (lon_len >= sizeof(lon_buf)) {
    return -1;
  }
  memcpy(lon_buf, lon_start, lon_len);
  lon_buf[lon_len] = '\0';

  const char *ew_field = lon_end + 1;
  if (!*ew_field) {
    return -1;
  }
  const char ew_char = *ew_field;

  double lat_value = strtod(lat_buf, NULL);
  double lon_value = strtod(lon_buf, NULL);
  if (ns_char == 'S' || ns_char == 's') {
    lat_value = -lat_value;
  }
  if (ew_char == 'W' || ew_char == 'w') {
    lon_value = -lon_value;
  }
  *latitude = lat_value;
  *longitude = lon_value;
  return 0;
}

static char *build_output_json(const char *timestamp,
                               const char *position,
                               const char *tracker_id) {
  if (!position || !tracker_id) {
    return NULL;
  }
  const char *ts = (timestamp && *timestamp) ? timestamp : NULL;
  size_t size = strlen(position) + strlen(tracker_id) + 64;
  if (ts) {
    size += strlen(ts);
  }
  char *out = (char *)malloc(size);
  if (!out) {
    return NULL;
  }
  if (ts) {
    snprintf(out,
             size,
             "{\"timestamp\":\"%s\",\"position\":\"%s\",\"tracker_id\":\"%s\"}",
             ts,
             position,
             tracker_id);
  } else {
    snprintf(out,
             size,
             "{\"position\":\"%s\",\"tracker_id\":\"%s\"}",
             position,
             tracker_id);
  }
  return out;
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
          "Usage: %s --input-uri URI --kml PATH [options]\n\n"
          "Options:\n"
          "  --input-uri URI            AMQP URI to consume tracker events (required)\n"
          "  --input-queue NAME         Queue name with tracker events (default: tcpproxy.integration)\n"
          "  --input-exchange NAME      Exchange to bind the input queue to\n"
          "  --input-routing-key KEY    Routing key for the input binding\n"
          "  --output-uri URI           AMQP URI for publishing location updates (defaults to input URI)\n"
          "  --output-exchange NAME     Exchange for location updates\n"
          "  --output-routing-key KEY   Routing key (or queue) for location updates\n"
          "  --kml PATH                 KML file containing placemarks (required)\n"
          "  --pidfile PATH             Optional pidfile to create while running\n"
          "  --config PATH              Optional configuration file (key=value); CLI overrides file values\n"
          "  --log-level LEVEL          DEBUG, INFO, WARN, or ERROR (default INFO)\n",
          prog);
}

static int parse_arguments(int argc, char **argv, struct daemon_config *cfg) {
  static struct option options[] = {
      {"input-uri", required_argument, NULL, 1},
      {"input-queue", required_argument, NULL, 2},
      {"input-exchange", required_argument, NULL, 3},
      {"input-routing-key", required_argument, NULL, 4},
      {"output-uri", required_argument, NULL, 5},
      {"output-exchange", required_argument, NULL, 6},
      {"output-routing-key", required_argument, NULL, 7},
      {"kml", required_argument, NULL, 8},
      {"log-level", required_argument, NULL, 9},
      {"config", required_argument, NULL, 10},
      {"pidfile", required_argument, NULL, 11},
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
      cfg->input_uri = daemon_dup_string(optarg);
      break;
    case 2:
      free(cfg->input_queue);
      cfg->input_queue = daemon_dup_string(optarg);
      break;
    case 3:
      free(cfg->input_exchange);
      cfg->input_exchange = daemon_dup_string(optarg);
      break;
    case 4:
      free(cfg->input_routing_key);
      cfg->input_routing_key = daemon_dup_string(optarg);
      break;
    case 5:
      free(cfg->output_uri);
      cfg->output_uri = daemon_dup_string(optarg);
      break;
    case 6:
      free(cfg->output_exchange);
      cfg->output_exchange = daemon_dup_string(optarg);
      break;
    case 7:
      free(cfg->output_routing_key);
      cfg->output_routing_key = daemon_dup_string(optarg);
      break;
    case 8:
      free(cfg->kml_path);
      cfg->kml_path = daemon_dup_string(optarg);
      break;
    case 9:
      cfg->log_level = parse_log_level(optarg);
      break;
    case 10:
      free(cfg->config_path);
      cfg->config_path = daemon_dup_string(optarg);
      break;
    case 11:
      free(cfg->pidfile_path);
      cfg->pidfile_path = daemon_dup_string(optarg);
      break;
    default:
      usage(argv[0]);
      return -1;
    }
  }

  if (!cfg->input_queue) {
    cfg->input_queue = daemon_dup_string("tcpproxy.integration");
  }
  if (!cfg->output_routing_key) {
    cfg->output_routing_key = daemon_dup_string("cat.location");
  }
  if (!cfg->log_level) {
    cfg->log_level = LOG_INFO;
  }

  if (!cfg->input_uri || !cfg->kml_path) {
    usage(argv[0]);
    return -1;
  }
  if (!cfg->output_uri) {
    cfg->output_uri = daemon_dup_string(cfg->input_uri);
  }
  if (!cfg->input_routing_key || !*cfg->input_routing_key) {
    free(cfg->input_routing_key);
    cfg->input_routing_key = cfg->input_queue ? daemon_dup_string(cfg->input_queue) : daemon_dup_string("#");
  }
  if (!cfg->output_routing_key) {
    cfg->output_routing_key = daemon_dup_string("cat.location");
  }
  if (!cfg->input_queue || !cfg->output_routing_key) {
    return -1;
  }
  if (!cfg->log_level) {
    cfg->log_level = LOG_INFO;
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
  free(cfg->kml_path);
  free(cfg->pidfile_path);
  free(cfg->config_path);
}

static amqp_connection_state_t open_connection(const char *uri,
                                               int log_level,
                                               amqp_channel_t channel,
                                               const char *queue,
                                               const char *exchange,
                                               const char *routing_key,
                                               bool declare_queue,
                                               bool durable_queue) {
  struct daemon_uri_parts parts;
  if (daemon_parse_amqp_uri(uri, &parts) != 0) {
    log_message(LOG_ERROR, log_level, "Invalid AMQP URI: %s", uri);
    return NULL;
  }
  amqp_connection_state_t conn = amqp_new_connection();
  amqp_socket_t *socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    log_message(LOG_ERROR, log_level, "Failed to allocate AMQP socket");
    amqp_destroy_connection(conn);
    return NULL;
  }
  int status = amqp_socket_open(socket, parts.host, parts.port);
  if (status != AMQP_STATUS_OK) {
    log_message(LOG_ERROR, log_level, "Failed to open AMQP socket to %s:%d (%s)",
                parts.host,
                parts.port,
                amqp_error_string2(status));
    amqp_destroy_connection(conn);
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
    log_message(LOG_ERROR, log_level, "AMQP login failed: %s", amqp_error_string2(reply.library_error));
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    return NULL;
  }

  amqp_channel_open(conn, channel);
  reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, log_level, "Failed to open AMQP channel");
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    return NULL;
  }

  amqp_table_t args = {0};
  if (declare_queue && queue && *queue) {
    amqp_queue_declare(conn,
                       channel,
                       amqp_cstring_bytes(queue),
                       0,
                       durable_queue ? 1 : 0,
                       0,
                       durable_queue ? 0 : 1,
                       args);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_ERROR, log_level, "Queue declare failed for %s", queue);
      amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      return NULL;
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
        amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
        return NULL;
      }
      const char *bind_key = (routing_key && *routing_key) ? routing_key : queue;
      amqp_queue_bind(conn,
                      channel,
                      amqp_cstring_bytes(queue),
                      amqp_cstring_bytes(exchange),
                      amqp_cstring_bytes(bind_key),
                      args);
      reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        log_message(LOG_ERROR, log_level, "Queue bind failed for %s", queue);
        amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
        return NULL;
      }
    }
  } else if (exchange && *exchange) {
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
      amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      return NULL;
    }
  }

  return conn;
}

static void close_connection(amqp_connection_state_t conn, amqp_channel_t channel) {
  if (!conn) {
    return;
  }
  amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
  amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(conn);
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
  } else if (routing_key && *routing_key) {
    amqp_queue_declare(conn,
                       channel,
                       amqp_cstring_bytes(routing_key),
                       0,
                       0,
                       0,
                       1,
                       args);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_ERROR, log_level, "Failed to declare output queue %s", routing_key);
      return -1;
    }
  }
  return 0;
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

  amqp_bytes_t exchange_bytes = (exchange && *exchange) ? amqp_cstring_bytes(exchange) : amqp_empty_bytes;
  amqp_bytes_t routing_bytes = amqp_cstring_bytes(routing_key);
  amqp_bytes_t body_bytes = {.len = body_len, .bytes = (void *)body};

  int status = amqp_basic_publish(conn,
                                  channel,
                                  exchange_bytes,
                                  routing_bytes,
                                  0,
                                  0,
                                  &props,
                                  body_bytes);
  if (status != AMQP_STATUS_OK) {
    log_message(LOG_WARN, log_level, "Publish failed (%s)", amqp_error_string2(status));
    return -1;
  }
  return 0;
}

int main(int argc, char **argv) {
  struct daemon_config cfg;
  memset(&cfg, 0, sizeof(cfg));
  cfg.log_level = LOG_INFO;
  cfg.input_queue = daemon_dup_string("tcpproxy.integration");
  cfg.output_routing_key = daemon_dup_string("cat.location");

  char *config_path = daemon_find_config_path(argc, argv);
  if (config_path) {
    cfg.config_path = config_path;
    daemon_parse_config_file(cfg.config_path,
                             (daemon_kv_handler)apply_config_key,
                             &cfg,
                             (daemon_log_fn)log_message,
                             cfg.log_level,
                             "cat_location_daemon");
  }

  if (parse_arguments(argc, argv, &cfg) != 0) {
    free_config(&cfg);
    return 1;
  }

  if (cfg.pidfile_path) {
    if (daemon_pidfile_create(cfg.pidfile_path,
                              (daemon_log_fn)log_message,
                              cfg.log_level,
                              "cat_location_daemon") != 0) {
      free_config(&cfg);
      return 1;
    }
    atexit(daemon_pidfile_cleanup);
  }

  xmlInitParser();

  struct location_resolver resolver;
  if (location_resolver_init(&resolver, cfg.kml_path) != 0) {
    log_message(LOG_ERROR, cfg.log_level, "Failed to load locations from %s", cfg.kml_path);
    free_config(&cfg);
    xmlCleanupParser();
    return 1;
  }

  signal(SIGTERM, signal_handler);
  signal(SIGINT, signal_handler);

  amqp_connection_state_t input_conn = NULL;
  amqp_connection_state_t output_conn = NULL;
  const amqp_channel_t INPUT_CHANNEL = 1;
  const amqp_channel_t OUTPUT_CHANNEL = 1;

  input_conn = open_connection(cfg.input_uri,
                               cfg.log_level,
                               INPUT_CHANNEL,
                               cfg.input_queue,
                               cfg.input_exchange,
                               cfg.input_routing_key,
                               true,
                               false);
  if (!input_conn) {
    location_resolver_free(&resolver);
    free_config(&cfg);
    xmlCleanupParser();
    return 1;
  }

  amqp_basic_qos(input_conn, INPUT_CHANNEL, 0, 1, 0);
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(input_conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, cfg.log_level, "Failed to set basic.qos");
    close_connection(input_conn, INPUT_CHANNEL);
    location_resolver_free(&resolver);
    free_config(&cfg);
    xmlCleanupParser();
    return 1;
  }

  amqp_table_t args = {0};
  amqp_basic_consume(input_conn,
                     INPUT_CHANNEL,
                     amqp_cstring_bytes(cfg.input_queue),
                     amqp_empty_bytes,
                     0,
                     0,
                     0,
                     args);
  reply = amqp_get_rpc_reply(input_conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, cfg.log_level, "basic.consume failed for %s", cfg.input_queue);
    close_connection(input_conn, INPUT_CHANNEL);
    location_resolver_free(&resolver);
    free_config(&cfg);
    xmlCleanupParser();
    return 1;
  }

  output_conn = open_connection(cfg.output_uri,
                                cfg.log_level,
                                OUTPUT_CHANNEL,
                                NULL,
                                cfg.output_exchange,
                                cfg.output_routing_key,
                                false,
                                false);
  if (!output_conn) {
    close_connection(input_conn, INPUT_CHANNEL);
    location_resolver_free(&resolver);
    free_config(&cfg);
    xmlCleanupParser();
    return 1;
  }

  if (declare_output(output_conn,
                     OUTPUT_CHANNEL,
                     cfg.output_exchange,
                     cfg.output_routing_key,
                     cfg.log_level) != 0) {
    close_connection(input_conn, INPUT_CHANNEL);
    close_connection(output_conn, OUTPUT_CHANNEL);
    location_resolver_free(&resolver);
    free_config(&cfg);
    xmlCleanupParser();
    return 1;
  }

  char *last_position = NULL;

  struct timeval timeout;
  timeout.tv_sec = 1;
  timeout.tv_usec = 0;

  while (!g_stop) {
    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(input_conn);
    amqp_rpc_reply_t consume_reply = amqp_consume_message(input_conn, &envelope, &timeout, 0);
    if (consume_reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        consume_reply.library_error == AMQP_STATUS_TIMEOUT) {
      continue;
    }
    if (consume_reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_WARN, cfg.log_level, "Consume error: %s", amqp_error_string2(consume_reply.library_error));
      continue;
    }

    int result = 0;
    if (envelope.message.body.len > 0) {
      char *body = (char *)malloc(envelope.message.body.len + 1);
      if (body) {
        memcpy(body, envelope.message.body.bytes, envelope.message.body.len);
        body[envelope.message.body.len] = '\0';

        char payload[8192];
        if (json_extract_string(body, "payload", payload, sizeof(payload)) == 0) {
          char tracker_id[128];
          double latitude = 0.0;
          double longitude = 0.0;
          if (parse_tracker_payload(payload, tracker_id, sizeof(tracker_id), &latitude, &longitude) == 0) {
            const char *position = location_resolver_resolve(&resolver, latitude, longitude);
            if (position) {
              if (!last_position || strcmp(last_position, position) != 0) {
                char timestamp[64];
                if (json_extract_optional_string(body, "timestamp", timestamp, sizeof(timestamp)) != 0) {
                  char *generated = current_timestamp_utc();
                  if (generated) {
                    strncpy(timestamp, generated, sizeof(timestamp) - 1);
                    timestamp[sizeof(timestamp) - 1] = '\0';
                    free(generated);
                  } else {
                    timestamp[0] = '\0';
                  }
                }
                char *json_out = build_output_json(timestamp, position, tracker_id);
                if (json_out) {
                  if (publish_event(output_conn,
                                    OUTPUT_CHANNEL,
                                    cfg.output_exchange,
                                    cfg.output_routing_key,
                                    json_out,
                                    strlen(json_out),
                                    cfg.log_level) == 0) {
                    free(last_position);
                    last_position = daemon_dup_string(position);
                  } else {
                    result = -1;
                  }
                  free(json_out);
                }
              }
            }
          }
        }
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

  free(last_position);
  close_connection(input_conn, INPUT_CHANNEL);
  close_connection(output_conn, OUTPUT_CHANNEL);
  location_resolver_free(&resolver);
  daemon_pidfile_cleanup();
  free_config(&cfg);
  xmlCleanupParser();
  return 0;
}
