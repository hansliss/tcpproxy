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
#include <unistd.h>

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

static int json_extract_double(const char *json,
                               const char *key,
                               double *out) {
  if (!json || !key || !out) {
    return -1;
  }
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
  char *endptr = NULL;
  double value = strtod(start, &endptr);
  if (start == endptr) {
    return -1;
  }
  *out = value;
  return 0;
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

static const char *k_component = "cat_location_daemon";

static amqp_connection_state_t connect_input(struct daemon_config *cfg,
                                             amqp_channel_t channel) {
  struct daemon_amqp_config acfg = {
      .uri = cfg->input_uri,
      .exchange = cfg->input_exchange,
      .routing_key = cfg->input_routing_key,
      .log_level = cfg->log_level,
      .component_name = k_component};

  amqp_connection_state_t conn = daemon_amqp_open(&acfg,
                                                  channel,
                                                  cfg->input_exchange && *cfg->input_exchange,
                                                  0,
                                                  0,
                                                  (daemon_log_fn)log_message);
  if (!conn) {
    return NULL;
  }

  if (daemon_amqp_declare_queue(conn,
                                channel,
                                cfg->input_queue,
                                cfg->input_exchange,
                                cfg->input_routing_key,
                                0,
                                cfg->log_level,
                                k_component,
                                (daemon_log_fn)log_message) != 0) {
    daemon_amqp_close(conn, channel);
    return NULL;
  }

  amqp_basic_qos(conn, channel, 0, 1, 0);
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, cfg->log_level, "Failed to set basic.qos");
    daemon_amqp_close(conn, channel);
    return NULL;
  }

  amqp_table_t args = {0};
  amqp_basic_consume(conn,
                     channel,
                     amqp_cstring_bytes(cfg->input_queue),
                     amqp_empty_bytes,
                     0,
                     0,
                     0,
                     args);
  reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    log_message(LOG_ERROR, cfg->log_level, "basic.consume failed for %s", cfg->input_queue);
    daemon_amqp_close(conn, channel);
    return NULL;
  }

  return conn;
}

static amqp_connection_state_t connect_output(struct daemon_config *cfg,
                                              amqp_channel_t channel) {
  struct daemon_amqp_config acfg = {
      .uri = cfg->output_uri,
      .exchange = cfg->output_exchange,
      .routing_key = cfg->output_routing_key,
      .log_level = cfg->log_level,
      .component_name = k_component};

  return daemon_amqp_open(&acfg,
                          channel,
                          cfg->output_exchange && *cfg->output_exchange,
                          0,
                          1,
                          (daemon_log_fn)log_message);
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

  return daemon_amqp_publish(conn,
                             channel,
                             exchange,
                             routing_key,
                             body,
                             body_len,
                             &props,
                             1,
                             (daemon_log_fn)log_message,
                             log_level,
                             k_component);
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

  char *last_position = NULL;

  struct timeval timeout;
  timeout.tv_sec = 1;
  timeout.tv_usec = 0;

  while (!g_stop) {
    if (!input_conn) {
      input_conn = connect_input(&cfg, INPUT_CHANNEL);
      if (!input_conn) {
        log_message(LOG_WARN, cfg.log_level, "Input connection unavailable, retrying...");
        sleep(1);
        continue;
      }
    }
    if (!output_conn) {
      output_conn = connect_output(&cfg, OUTPUT_CHANNEL);
      if (!output_conn) {
        log_message(LOG_WARN, cfg.log_level, "Output connection unavailable, retrying...");
        sleep(1);
        continue;
      }
    }

    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(input_conn);
    amqp_rpc_reply_t consume_reply = amqp_consume_message(input_conn, &envelope, &timeout, 0);
    if (consume_reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        consume_reply.library_error == AMQP_STATUS_TIMEOUT) {
      if (daemon_amqp_wait_heartbeat(output_conn) != 0) {
        log_message(LOG_WARN, cfg.log_level, "Output heartbeat failed, reconnecting");
        daemon_amqp_close(output_conn, OUTPUT_CHANNEL);
        output_conn = NULL;
      }
      continue;
    }
    if (consume_reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_WARN,
                  cfg.log_level,
                  "Consume error: %s",
                  amqp_error_string2(consume_reply.library_error));
      daemon_amqp_close(input_conn, INPUT_CHANNEL);
      input_conn = NULL;
      continue;
    }

    int result = 0;
    if (envelope.message.body.len > 0) {
      char *body = (char *)malloc(envelope.message.body.len + 1);
      if (body) {
        memcpy(body, envelope.message.body.bytes, envelope.message.body.len);
        body[envelope.message.body.len] = '\0';

        char tracker_id[128];
        if (json_extract_string(body, "tracker_id", tracker_id, sizeof(tracker_id)) != 0) {
          log_message(LOG_WARN, cfg.log_level, "Missing tracker_id in parsed event");
        } else {
          double latitude = 0.0;
          double longitude = 0.0;
          if (json_extract_double(body, "latitude", &latitude) != 0 ||
              json_extract_double(body, "longitude", &longitude) != 0) {
            log_message(LOG_WARN, cfg.log_level, "Missing latitude/longitude in parsed event");
          } else {
            const char *position = location_resolver_resolve(&resolver, latitude, longitude);
            if (position && (!last_position || strcmp(last_position, position) != 0)) {
              char timestamp_buf[64];
              if (json_extract_optional_string(body, "timestamp", timestamp_buf, sizeof(timestamp_buf)) != 0) {
                char date_buf[32];
                char time_buf[32];
                if (json_extract_string(body, "date", date_buf, sizeof(date_buf)) == 0 &&
                    json_extract_string(body, "time", time_buf, sizeof(time_buf)) == 0) {
                  snprintf(timestamp_buf, sizeof(timestamp_buf), "%s %s", date_buf, time_buf);
                } else {
                  timestamp_buf[0] = '\0';
                }
              }

              char json_out[256];
              if (timestamp_buf[0]) {
                snprintf(json_out,
                         sizeof(json_out),
                         "{\"tracker_id\":\"%s\",\"timestamp\":\"%s\",\"position\":\"%s\"}",
                         tracker_id,
                         timestamp_buf,
                         position);
              } else {
                snprintf(json_out,
                         sizeof(json_out),
                         "{\"tracker_id\":\"%s\",\"position\":\"%s\"}",
                         tracker_id,
                         position);
              }

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
                daemon_amqp_close(output_conn, OUTPUT_CHANNEL);
                output_conn = NULL;
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
  daemon_amqp_close(input_conn, INPUT_CHANNEL);
  daemon_amqp_close(output_conn, OUTPUT_CHANNEL);
  location_resolver_free(&resolver);
  daemon_pidfile_cleanup();
  free_config(&cfg);
  xmlCleanupParser();
  return 0;
}
