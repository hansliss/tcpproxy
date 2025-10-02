#ifndef DAEMON_COMMON_H
#define DAEMON_COMMON_H

#include <amqp.h>
#include <amqp_framing.h>
#include <stddef.h>

struct daemon_uri_parts {
  char host[256];
  char username[256];
  char password[256];
  char vhost[256];
  int port;
};

typedef int (*daemon_kv_handler)(void *ctx, const char *key, const char *value);

typedef void (*daemon_log_fn)(int level, int configured_level, const char *fmt, ...);

struct daemon_amqp_config {
  const char *uri;
  const char *exchange;
  const char *routing_key;
  int log_level;
  const char *component_name;
};

char *daemon_dup_string(const char *value);
char *daemon_trim_whitespace(char *value);
char *daemon_percent_decode(const char *input, int plus_to_space);
int daemon_parse_amqp_uri(const char *uri, struct daemon_uri_parts *out);
int daemon_parse_config_file(const char *path,
                             daemon_kv_handler handler,
                             void *ctx,
                             daemon_log_fn log_fn,
                             int log_level,
                             const char *component_name);
char *daemon_find_config_path(int argc, char **argv);
int daemon_pidfile_create(const char *path,
                          daemon_log_fn log_fn,
                          int log_level,
                          const char *component_name);
void daemon_pidfile_cleanup(void);
const char *daemon_pidfile_path(void);

amqp_connection_state_t daemon_amqp_open(const struct daemon_amqp_config *cfg,
                                         amqp_channel_t channel,
                                         int declare_exchange,
                                         int durable_queue,
                                         int enable_confirms,
                                         daemon_log_fn log_fn);
void daemon_amqp_close(amqp_connection_state_t conn, amqp_channel_t channel);
int daemon_amqp_declare_queue(amqp_connection_state_t conn,
                              amqp_channel_t channel,
                              const char *queue,
                              const char *exchange,
                              const char *routing_key,
                              int durable,
                              int log_level,
                              const char *component_name,
                              daemon_log_fn log_fn);
int daemon_amqp_wait_heartbeat(amqp_connection_state_t conn);
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
                        const char *component_name);

#endif
