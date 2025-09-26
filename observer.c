#include "observer.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tracker_parser.h"

struct observer_global {
  char *log_path;
};

struct observer_instance {
  struct observer_global *global;
  tracker_parser parser;
  int log_fd;
  char connection_id[64];
  char client_ip[128];
};

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

static char *extract_path(const char *config_string) {
  if (!config_string) {
    return NULL;
  }
  char *copy = strdup(config_string);
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

int observer_global_init(const char *config_string, struct observer_global **out_global) {
  if (!out_global) {
    return -1;
  }
  *out_global = NULL;
  if (!config_string || *config_string == '\0') {
    return 0;
  }
  char *log_path = extract_path(config_string);
  if (!log_path) {
    return -1;
  }
  struct observer_global *global = calloc(1, sizeof(*global));
  if (!global) {
    free(log_path);
    return -1;
  }
  global->log_path = log_path;
  *out_global = global;
  return 0;
}

void observer_global_free(struct observer_global *global) {
  if (!global) {
    return;
  }
  free(global->log_path);
  free(global);
}

static void emit_event(void *ctx,
                       enum observer_direction direction,
                       const char *timestamp,
                       const char *message,
                       size_t message_len) {
  struct observer_instance *instance = (struct observer_instance *)ctx;
  if (!instance || instance->log_fd == -1 || !timestamp || !message) {
    return;
  }
  const char *direction_str = direction == OBSERVER_DIR_CLIENT ? "client" : "server";
  size_t timestamp_len = strlen(timestamp);
  size_t direction_len = strlen(direction_str);
  size_t ip_len = strlen(instance->client_ip);
  size_t connection_len = strlen(instance->connection_id);
  size_t total = timestamp_len + direction_len + ip_len + connection_len + message_len + 8;
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
  ssize_t rc = write(instance->log_fd, line, offset);
  (void)rc;
  free(line);
}

struct observer_instance *observer_instance_create(struct observer_global *global,
                                                   const char *connection_id,
                                                   const char *client_ip) {
  if (!global || !global->log_path) {
    return NULL;
  }
  struct observer_instance *instance = calloc(1, sizeof(*instance));
  if (!instance) {
    return NULL;
  }
  instance->global = global;
  tracker_parser_init(&instance->parser);
  instance->log_fd = open(global->log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (instance->log_fd == -1) {
    fprintf(stderr, "observer: cannot open log file %s: %s\n",
            global->log_path, strerror(errno));
    free(instance);
    return NULL;
  }
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
  return instance;
}

void observer_instance_record(struct observer_instance *instance,
                              enum observer_direction direction,
                              const char *timestamp,
                              const char *data,
                              size_t data_len) {
  if (!instance || data_len == 0) {
    return;
  }
  tracker_parser_push(&instance->parser, direction, timestamp, data, data_len, emit_event, instance);
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
