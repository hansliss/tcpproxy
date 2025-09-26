#ifndef OBSERVER_H
#define OBSERVER_H

#include <stddef.h>

enum observer_direction {
  OBSERVER_DIR_CLIENT = 0,
  OBSERVER_DIR_SERVER = 1
};

struct observer_global;
struct observer_instance;

int observer_global_init(const char *config_string, struct observer_global **out_global);
void observer_global_free(struct observer_global *global);

struct observer_instance *observer_instance_create(struct observer_global *global,
                                                   const char *connection_id,
                                                   const char *client_ip);
void observer_instance_record(struct observer_instance *instance,
                              enum observer_direction direction,
                              const char *timestamp,
                              const char *data,
                              size_t data_len);
void observer_instance_close(struct observer_instance *instance);

#endif /* OBSERVER_H */
