#include "daemon_common.h"

#include <assert.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct kv_capture {
  char last_key[64];
  char last_value[64];
  int seen;
};

static int capture_handler(void *ctx, const char *key, const char *value) {
  struct kv_capture *cap = (struct kv_capture *)ctx;
  if (strcmp(key, "known") == 0) {
    strncpy(cap->last_key, key, sizeof(cap->last_key) - 1);
    cap->last_key[sizeof(cap->last_key) - 1] = '\0';
    strncpy(cap->last_value, value, sizeof(cap->last_value) - 1);
    cap->last_value[sizeof(cap->last_value) - 1] = '\0';
    cap->seen++;
    return 0;
  }
  return -1;
}

static int g_log_messages = 0;

static void capture_log(int level, int configured_level, const char *fmt, ...) {
  (void)level;
  (void)configured_level;
  (void)fmt;
  va_list args;
  va_start(args, fmt);
  va_end(args);
  g_log_messages++;
}

static void test_dup_string(void) {
  char *dup = daemon_dup_string("hello");
  assert(dup);
  assert(strcmp(dup, "hello") == 0);
  assert(dup != "hello");
  free(dup);
  assert(daemon_dup_string(NULL) == NULL);
}

static void test_trim_whitespace(void) {
  char buffer[] = "  \t value\n";
  char *trimmed = daemon_trim_whitespace(buffer);
  assert(strcmp(trimmed, "value") == 0);
  char empty[] = "  \n\t  ";
  char *trimmed_empty = daemon_trim_whitespace(empty);
  assert(strcmp(trimmed_empty, "") == 0);
}

static void test_percent_decode(void) {
  char *decoded = daemon_percent_decode("hello%20world", 0);
  assert(decoded);
  assert(strcmp(decoded, "hello world") == 0);
  free(decoded);
  char *plus = daemon_percent_decode("a+b", 1);
  assert(plus);
  assert(strcmp(plus, "a b") == 0);
  free(plus);
  char *literal_plus = daemon_percent_decode("a+b", 0);
  assert(literal_plus);
  assert(strcmp(literal_plus, "a+b") == 0);
  free(literal_plus);
}

static void test_parse_amqp_uri(void) {
  struct daemon_uri_parts parts;
  assert(daemon_parse_amqp_uri("amqp://user:pass@host:1234/vhost", &parts) == 0);
  assert(strcmp(parts.username, "user") == 0);
  assert(strcmp(parts.password, "pass") == 0);
  assert(strcmp(parts.host, "host") == 0);
  assert(parts.port == 1234);
  assert(strcmp(parts.vhost, "vhost") == 0);

  assert(daemon_parse_amqp_uri("amqp://host", &parts) == 0);
  assert(strcmp(parts.host, "host") == 0);
  assert(parts.port == 5672);
  assert(strcmp(parts.username, "guest") == 0);
  assert(strcmp(parts.vhost, "/") == 0);

  assert(daemon_parse_amqp_uri("http://example.com", &parts) != 0);
}

static void test_parse_config_file(void) {
  char template_path[] = "daemon_common_confXXXXXX";
  int fd = mkstemp(template_path);
  assert(fd >= 0);
  const char *content = "# comment\n\nknown = value\nunknown = other\n";
  assert(write(fd, content, (size_t)strlen(content)) == (ssize_t)strlen(content));
  close(fd);

  struct kv_capture cap = {0};
  g_log_messages = 0;

  assert(daemon_parse_config_file(template_path,
                                  capture_handler,
                                  &cap,
                                  (daemon_log_fn)capture_log,
                                  0,
                                  "daemon_common_test") == 0);
  assert(cap.seen == 1);
  assert(strcmp(cap.last_key, "known") == 0);
  assert(strcmp(cap.last_value, "value") == 0);
  assert(g_log_messages >= 1);

  unlink(template_path);
}

static void test_find_config_path(void) {
  char *argv1[] = {"prog", "--config", "/tmp/foo", NULL};
  int argc1 = 3;
  char *path = daemon_find_config_path(argc1, argv1);
  assert(path);
  assert(strcmp(path, "/tmp/foo") == 0);
  free(path);

  char *argv2[] = {"prog", "--config=/tmp/bar", NULL};
  int argc2 = 2;
  path = daemon_find_config_path(argc2, argv2);
  assert(path);
  assert(strcmp(path, "/tmp/bar") == 0);
  free(path);

  char *argv3[] = {"prog", "--other", NULL};
  assert(daemon_find_config_path(2, argv3) == NULL);
}

static void test_pidfile_helpers(void) {
  char template_path[] = "daemon_common_pidXXXXXX";
  int fd = mkstemp(template_path);
  assert(fd >= 0);
  close(fd);

  assert(daemon_pidfile_create(template_path, NULL, 0, "test_daemon") == 0);
  const char *stored = daemon_pidfile_path();
  assert(stored);
  assert(strcmp(stored, template_path) == 0);

  FILE *fp = fopen(template_path, "r");
  assert(fp);
  long pid = 0;
  assert(fscanf(fp, "%ld", &pid) == 1);
  fclose(fp);
  assert(pid == (long)getpid());

  daemon_pidfile_cleanup();
  assert(access(template_path, F_OK) == -1);
}

int main(void) {
  test_dup_string();
  test_trim_whitespace();
  test_percent_decode();
  test_parse_amqp_uri();
  test_parse_config_file();
  test_find_config_path();
  test_pidfile_helpers();
  return 0;
}
