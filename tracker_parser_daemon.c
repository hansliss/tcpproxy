#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <jansson.h>
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

/*
 * Minimal C reimplementation of the tracker_parser_daemon helper. The daemon
 * consumes JSON events from RabbitMQ, extracts tracker payloads, and publishes
 * derived JSON objects to another exchange or queue. The original Python
 * implementation used pika; this version relies on librabbitmq.
 */

#define MAX_URI_PART 256
struct daemon_config {
  char *input_uri;
  char *input_queue;
  char *input_exchange;
  char *input_routing_key;
  char *output_uri;
  char *output_exchange;
  char *output_routing_key;
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

static const char *TK_PATTERN =
    "^\\[([A-Z0-9]*)\\*(?P<device>[0-9]+)\\*[0-9A-Fa-f]+\\*UD[^,]*,"
    "(?P<day>..)(?P<month>..)(?P<year>..),"
    "(?P<hour>..)(?P<minute>..)(?P<second>..),"
    "(?P<status>[AV]),"
    "(?P<lat>[-0-9.]*),(?P<ns>[NS]),"
    "(?P<lon>[-0-9.]*),(?P<ew>[EW]),"
    "(?P<speed>[.0-9]*),(?P<direction>[-.0-9]*),(?P<tkunk01>[^,]*),"
    "(?P<nsats>[0-9]*),(?P<tkunk02>[^,]*),"
    "(?P<bat>[.0-9]*),(?P<tkunk03>[^,]*),(?P<tkunk04>[^,]*),(?P<tkunk05>[^,]*),"
    "(?P<ntowers>[0-9]*),(?P<mnc>[0-9]*),(?P<mcc>[0-9]*),(?P<tkunk06>[0-9]*)"
    "(?P<celltowers>.*),,00\\]$";

static const char *FA_PATTERN =
    "^\\[([A-Z0-9]*)\\*(?P<device>[0-9]+)\\*[0-9A-Fa-f]+\\*UD[^,]*,"
    "(?P<day>..)(?P<month>..)(?P<year>..),"
    "(?P<hour>..)(?P<minute>..)(?P<second>..),"
    "(?P<status>[AV]),"
    "(?P<lat>[-0-9.]*),(?P<ns>[NS]),"
    "(?P<lon>[-0-9.]*),(?P<ew>[EW]),"
    "(?P<speed>[.0-9]*),(?P<direction>[-.0-9]*),(?P<faunk01>[^,]*),"
    "(?P<nsats>[0-9]*),(?P<faunk02>[^,]*),"
    "(?P<bat>[.0-9]*),"
    "(?P<faunk03>[^,]*),(?P<faunk04>[^,]*),(?P<faunk05>[^,]*),"
    "(?P<faunk06>[^,]*),(?P<faunk07>[^,]*),(?P<faunk08>[^,]*).*\\]$";

static pcre2_code *g_tk_regex = NULL;
static pcre2_code *g_fa_regex = NULL;

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

static pcre2_code *compile_regex(const char *pattern, const char *name, int log_level) {
  int errornumber = 0;
  PCRE2_SIZE erroffset = 0;
  uint32_t options = PCRE2_UTF | PCRE2_UCP;
  pcre2_code *code = pcre2_compile((PCRE2_SPTR)pattern,
                                   PCRE2_ZERO_TERMINATED,
                                   options,
                                   &errornumber,
                                   &erroffset,
                                   NULL);
  if (!code) {
    PCRE2_UCHAR buffer[256];
    pcre2_get_error_message(errornumber, buffer, sizeof(buffer));
    log_message(LOG_ERROR,
                log_level,
                "Failed to compile %s pattern at offset %zu: %s",
                name,
                (size_t)erroffset,
                buffer);
  }
  return code;
}

static void free_regexes(void) {
  if (g_tk_regex) {
    pcre2_code_free(g_tk_regex);
    g_tk_regex = NULL;
  }
  if (g_fa_regex) {
    pcre2_code_free(g_fa_regex);
    g_fa_regex = NULL;
  }
}

static int get_substring_byname(pcre2_match_data *match_data,
                                const char *name,
                                char *out,
                                size_t out_size) {
  if (!out || out_size == 0) {
    return -1;
  }
  PCRE2_UCHAR *substring = NULL;
  PCRE2_SIZE length = 0;
  int rc = pcre2_substring_get_byname(match_data,
                                      (PCRE2_SPTR)name,
                                      &substring,
                                      &length);
  if (rc != 0) {
    out[0] = '\0';
    return -1;
  }
  size_t copy_len = (length < out_size - 1) ? (size_t)length : out_size - 1;
  memcpy(out, substring, copy_len);
  out[copy_len] = '\0';
  pcre2_substring_free(substring);
  if (length >= out_size) {
    return -1;
  }
  return 0;
}

static double parse_double_field(const char *value) {
  if (!value || !*value) {
    return 0.0;
  }
  char *endptr = NULL;
  double result = strtod(value, &endptr);
  if (endptr == value) {
    return 0.0;
  }
  return result;
}

static int parse_log_level(const char *level);

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
  } else if (strcmp(key, "pidfile") == 0 || strcmp(key, "pid-file") == 0) {
    set_string(&cfg->pidfile_path, value);
  } else if (strcmp(key, "log-level") == 0) {
    cfg->log_level = parse_log_level(value);
  } else {
    return -1;
  }
  return 0;
}

static const char *k_component = "tracker_parser_daemon";

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
                                                  1,
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
                                1,
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
                          1,
                          1,
                          (daemon_log_fn)log_message);
}

static int parse_with_regex(pcre2_code *code,
                            const char *payload,
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
  if (!code) {
    return -1;
  }

  pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(code, NULL);
  if (!match_data) {
    return -1;
  }

  int rc = pcre2_match(code,
                        (PCRE2_SPTR)payload,
                        (PCRE2_SIZE)strlen(payload),
                        0,
                        0,
                        match_data,
                        NULL);
  if (rc < 0) {
    pcre2_match_data_free(match_data);
    return -1;
  }

  char device_buf[128];
  char day_buf[8];
  char month_buf[8];
  char year_buf[8];
  char hour_buf[8];
  char minute_buf[8];
  char second_buf[8];
  char status_buf[8];
  char lat_buf[32];
  char ns_buf[4];
  char lon_buf[32];
  char ew_buf[4];
  char speed_buf[32];
  char direction_buf[32];
  char battery_buf[32];

  if (get_substring_byname(match_data, "device", device_buf, sizeof(device_buf)) != 0 ||
      get_substring_byname(match_data, "day", day_buf, sizeof(day_buf)) != 0 ||
      get_substring_byname(match_data, "month", month_buf, sizeof(month_buf)) != 0 ||
      get_substring_byname(match_data, "year", year_buf, sizeof(year_buf)) != 0 ||
      get_substring_byname(match_data, "hour", hour_buf, sizeof(hour_buf)) != 0 ||
      get_substring_byname(match_data, "minute", minute_buf, sizeof(minute_buf)) != 0 ||
      get_substring_byname(match_data, "second", second_buf, sizeof(second_buf)) != 0 ||
      get_substring_byname(match_data, "status", status_buf, sizeof(status_buf)) != 0 ||
      get_substring_byname(match_data, "lat", lat_buf, sizeof(lat_buf)) != 0 ||
      get_substring_byname(match_data, "ns", ns_buf, sizeof(ns_buf)) != 0 ||
      get_substring_byname(match_data, "lon", lon_buf, sizeof(lon_buf)) != 0 ||
      get_substring_byname(match_data, "ew", ew_buf, sizeof(ew_buf)) != 0 ||
      get_substring_byname(match_data, "speed", speed_buf, sizeof(speed_buf)) != 0 ||
      get_substring_byname(match_data, "direction", direction_buf, sizeof(direction_buf)) != 0 ||
      get_substring_byname(match_data, "bat", battery_buf, sizeof(battery_buf)) != 0) {
    pcre2_match_data_free(match_data);
    return -1;
  }

  if (strlen(device_buf) >= tracker_id_size) {
    pcre2_match_data_free(match_data);
    return -1;
  }
  strcpy(tracker_id, device_buf);

  int year_val = atoi(year_buf);
  year_val += (year_val >= 90) ? 1900 : 2000;
  snprintf(date_out, date_size, "%04d-%s-%s", year_val, month_buf, day_buf);
  snprintf(time_out, time_size, "%s:%s:%s", hour_buf, minute_buf, second_buf);

  char status_char = (status_buf[0] != '\0') ? status_buf[0] : 'V';
  *status_out = status_char;

  double latitude = parse_double_field(lat_buf);
  if (ns_buf[0] == 'S' || ns_buf[0] == 's') {
    latitude = -latitude;
  }
  double longitude = parse_double_field(lon_buf);
  if (ew_buf[0] == 'W' || ew_buf[0] == 'w') {
    longitude = -longitude;
  }
  double speed = parse_double_field(speed_buf);
  double direction_val = parse_double_field(direction_buf);
  double battery = parse_double_field(battery_buf);

  *lat_out = latitude;
  *lon_out = longitude;
  *speed_out = speed;
  *direction_out = direction_val;
  *battery_out = battery;

  pcre2_match_data_free(match_data);
  return 0;
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
  if (parse_with_regex(g_tk_regex,
                       payload,
                       tracker_id,
                       tracker_id_size,
                       date_out,
                       date_size,
                       time_out,
                       time_size,
                       status_out,
                       lat_out,
                       lon_out,
                       speed_out,
                       direction_out,
                       battery_out) == 0) {
    return 0;
  }
  if (parse_with_regex(g_fa_regex,
                       payload,
                       tracker_id,
                       tracker_id_size,
                       date_out,
                       date_size,
                       time_out,
                       time_size,
                       status_out,
                       lat_out,
                       lon_out,
                       speed_out,
                       direction_out,
                       battery_out) == 0) {
    return 0;
  }
  return -1;
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
  json_t *root = json_object();
  if (!root) {
    return NULL;
  }

  char status_buf[2] = {status, '\0'};
  if (json_object_set_new(root, "tracker_id", json_string(tracker_id)) != 0 ||
      json_object_set_new(root, "date", json_string(date)) != 0 ||
      json_object_set_new(root, "time", json_string(time)) != 0 ||
      json_object_set_new(root, "status", json_string(status_buf)) != 0 ||
      json_object_set_new(root, "latitude", json_real(lat)) != 0 ||
      json_object_set_new(root, "longitude", json_real(lon)) != 0 ||
      json_object_set_new(root, "speed", json_real(speed)) != 0 ||
      json_object_set_new(root, "direction", json_real(direction)) != 0 ||
      json_object_set_new(root, "battery", json_real(battery)) != 0) {
    json_decref(root);
    return NULL;
  }

  if (timestamp && *timestamp) {
    if (json_object_set_new(root, "timestamp", json_string(timestamp)) != 0) {
      json_decref(root);
      return NULL;
    }
  }

  char *serialized = json_dumps(root, JSON_COMPACT);
  json_decref(root);
  return serialized;
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

static int process_message(const char *json,
                           amqp_connection_state_t output_conn,
                           amqp_channel_t output_channel,
                           const char *output_exchange,
                           const char *output_routing_key,
                           int log_level) {
  json_error_t error;
  json_t *root = json_loads(json, 0, &error);
  if (!root) {
    log_message(LOG_WARN,
                log_level,
                "Invalid JSON (%s at line %d column %d)",
                error.text,
                error.line,
                error.column);
    return 0;
  }
  if (!json_is_object(root)) {
    log_message(LOG_WARN, log_level, "JSON message is not an object");
    json_decref(root);
    return 0;
  }

  const char *direction = NULL;
  json_t *direction_json = json_object_get(root, "direction");
  if (direction_json && json_is_string(direction_json)) {
    direction = json_string_value(direction_json);
  }
  if (direction && strcmp(direction, "client") != 0) {
    json_decref(root);
    return 0;
  }

  json_t *payload_val = json_object_get(root, "payload");
  if (!payload_val || !json_is_string(payload_val)) {
    log_message(LOG_WARN, log_level, "Message missing payload string field");
    json_decref(root);
    return 0;
  }
  const char *payload = json_string_value(payload_val);
  if (!payload) {
    json_decref(root);
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

  const char *timestamp = NULL;
  json_t *timestamp_val = json_object_get(root, "timestamp");
  if (timestamp_val && json_is_string(timestamp_val)) {
    timestamp = json_string_value(timestamp_val);
  }

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
    json_decref(root);
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
  json_decref(root);
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
          "  --pidfile PATH             Optional pidfile to create while running\n"
          "  --config PATH              Optional configuration file (key=value); CLI overrides file values\n"
          "  --log-level LEVEL          DEBUG, INFO, WARN, or ERROR (default: INFO)\n",
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
      {"config", required_argument, NULL, 8},
      {"log-level", required_argument, NULL, 9},
      {"pidfile", required_argument, NULL, 10},
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
      free(cfg->config_path);
      cfg->config_path = daemon_dup_string(optarg);
      break;
    case 9:
      cfg->log_level = parse_log_level(optarg);
      break;
    case 10:
      free(cfg->pidfile_path);
      cfg->pidfile_path = daemon_dup_string(optarg);
      break;
    default:
      usage(argv[0]);
      return -1;
    }
  }

  if (!cfg->input_queue) {
    cfg->input_queue = daemon_dup_string("tracker.events");
  }
  if (!cfg->output_routing_key) {
    cfg->output_routing_key = daemon_dup_string("tracker.parsed");
  }
  if (!cfg->log_level) {
    cfg->log_level = LOG_INFO;
  }

  if (!cfg->input_uri) {
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
    cfg->output_routing_key = daemon_dup_string("tracker.parsed");
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
  free(cfg->pidfile_path);
  free(cfg->config_path);
}

int main(int argc, char **argv) {
  struct daemon_config cfg;
  memset(&cfg, 0, sizeof(cfg));
  cfg.log_level = LOG_INFO;
  cfg.input_queue = daemon_dup_string("tracker.events");
  cfg.output_routing_key = daemon_dup_string("tracker.parsed");

  char *config_path = daemon_find_config_path(argc, argv);
  if (config_path) {
    cfg.config_path = config_path;
    daemon_parse_config_file(cfg.config_path,
                             (daemon_kv_handler)apply_config_key,
                             &cfg,
                             (daemon_log_fn)log_message,
                             cfg.log_level,
                             "tracker_parser_daemon");
  }

  if (parse_arguments(argc, argv, &cfg) != 0) {
    free_config(&cfg);
    return 1;
  }

  if (cfg.pidfile_path) {
    if (daemon_pidfile_create(cfg.pidfile_path,
                              (daemon_log_fn)log_message,
                              cfg.log_level,
                              "tracker_parser_daemon") != 0) {
      free_config(&cfg);
      return 1;
    }
    atexit(daemon_pidfile_cleanup);
  }

  g_tk_regex = compile_regex(TK_PATTERN, "TK", cfg.log_level);
  if (!g_tk_regex) {
    free_config(&cfg);
    return 1;
  }
  g_fa_regex = compile_regex(FA_PATTERN, "FA", cfg.log_level);
  if (!g_fa_regex) {
    free_regexes();
    free_config(&cfg);
    return 1;
  }

  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  amqp_connection_state_t input_conn = NULL;
  amqp_connection_state_t output_conn = NULL;
  const amqp_channel_t INPUT_CHANNEL = 1;
  const amqp_channel_t OUTPUT_CHANNEL = 1;

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
    amqp_rpc_reply_t reply = amqp_consume_message(input_conn, &envelope, &timeout, 0);
    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        reply.library_error == AMQP_STATUS_TIMEOUT) {
      if (daemon_amqp_wait_heartbeat(output_conn) != 0) {
        log_message(LOG_WARN, cfg.log_level, "Output heartbeat failed, reconnecting");
        daemon_amqp_close(output_conn, OUTPUT_CHANNEL);
        output_conn = NULL;
      }
      continue;
    }
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      log_message(LOG_WARN,
                  cfg.log_level,
                  "Consume error: %s",
                  amqp_error_string2(reply.library_error));
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
      daemon_amqp_close(output_conn, OUTPUT_CHANNEL);
      output_conn = NULL;
    }
    amqp_destroy_envelope(&envelope);
  }

  daemon_amqp_close(input_conn, INPUT_CHANNEL);
  daemon_amqp_close(output_conn, OUTPUT_CHANNEL);
  free_regexes();
  daemon_pidfile_cleanup();
  free_config(&cfg);
  return 0;
}
