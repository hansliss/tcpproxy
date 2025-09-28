#include "observer.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tracker_parser.h"

#ifndef DEFAULT_AMQP_HELPER_PATH
#define DEFAULT_AMQP_HELPER_PATH "scripts/observer_amqp_publisher.py"
#endif

/*
 * The observer module routes parsed tracker events to pluggable sinks. Two
 * modes are supported today: appending to a logfile and publishing JSON events
 * to RabbitMQ via an external helper process.
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
};

struct observer_instance {
  struct observer_global *global;
  tracker_parser parser;
  tracker_parser_emit_fn emit_fn;
  char connection_id[64];
  char client_ip[128];
  int log_fd;
  int helper_fd;
  pid_t helper_pid;
};

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
  } else if (strncmp(config_string, "amqp://", 7) == 0) {
    global->mode = OBSERVER_MODE_AMQP;
    global->amqp_config = dup_string(config_string);
    if (!global->amqp_config) {
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
static const char *resolve_helper_path(void) {
  const char *env = getenv("TCPPROXY_AMQP_HELPER");
  if (env && *env) {
    return env;
  }
  return DEFAULT_AMQP_HELPER_PATH;
}

/* Spawn the Python helper that publishes observer events to RabbitMQ. */
static int spawn_amqp_helper(const char *config, pid_t *pid_out, int *fd_out) {
  if (!config || !pid_out || !fd_out) {
    return -1;
  }
  int pipefd[2];
  if (pipe(pipefd) == -1) {
    return -1;
  }
  static int sigpipe_ignored = 0;
  if (!sigpipe_ignored) {
    signal(SIGPIPE, SIG_IGN);
    sigpipe_ignored = 1;
  }
  pid_t pid = fork();
  if (pid == -1) {
    close(pipefd[0]);
    close(pipefd[1]);
    return -1;
  }
  if (pid == 0) {
    const char *helper_path = resolve_helper_path();
    dup2(pipefd[0], STDIN_FILENO);
    close(pipefd[0]);
    close(pipefd[1]);
    execlp("python3", "python3", helper_path, "--config", config, (char *)NULL);
    _exit(111);
  }
  close(pipefd[0]);
  *pid_out = pid;
  *fd_out = pipefd[1];
  return 0;
}

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

/* Serialize observer events as JSON and stream them to the helper. */
static void emit_event_amqp(void *ctx,
                            enum observer_direction direction,
                            const char *timestamp,
                            const char *message,
                            size_t message_len) {
  struct observer_instance *instance = (struct observer_instance *)ctx;
  if (!instance || instance->helper_fd == -1) {
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
                           "{\"timestamp\":\"%s\",\"direction\":\"%s\",\"client_ip\":\"%s\",\"connection_id\":\"%s\",\"payload\":\"%s\"}\n",
                           escaped_timestamp,
                           escaped_direction,
                           escaped_ip,
                           escaped_conn,
                           escaped_payload);
    if (written > 0) {
      write(instance->helper_fd, line, (size_t)written);
    }
    free(line);
  }
  free(escaped_timestamp);
  free(escaped_direction);
  free(escaped_ip);
  free(escaped_conn);
  free(escaped_payload);
}

static void close_helper(pid_t pid, int fd) {
  if (fd != -1) {
    close(fd);
  }
  if (pid > 0) {
    int status;
    waitpid(pid, &status, 0);
  }
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
  instance->helper_fd = -1;
  instance->helper_pid = -1;

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
    if (!global->amqp_config) {
      free(instance);
      return NULL;
    }
    if (spawn_amqp_helper(global->amqp_config, &instance->helper_pid, &instance->helper_fd) != 0) {
      fprintf(stderr, "observer: failed to spawn AMQP helper for %s\n", global->amqp_config);
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
  if (instance->helper_fd != -1 || instance->helper_pid > 0) {
    close_helper(instance->helper_pid, instance->helper_fd);
  }
  free(instance);
}
