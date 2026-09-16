#ifndef DMESH_SERVICE_REGISTRY_H
#define DMESH_SERVICE_REGISTRY_H
#include <stddef.h>
#include <stdint.h>
#define DMESH_REGISTRY_MAX 127
struct dmesh_service {
    char name[128];
    uint32_t ipv4;
    uint16_t port;
    int16_t id;
};
struct dmesh_service_registry {
    size_t count;
    struct dmesh_service entries[DMESH_REGISTRY_MAX];
};
/* Immutable per-owner snapshot. Format: IPv4:port name service_id. */
int dmesh_registry_load(struct dmesh_service_registry *, const char *path);
const struct dmesh_service *dmesh_registry_name(const struct dmesh_service_registry *, const char *);
const struct dmesh_service *dmesh_registry_addr(const struct dmesh_service_registry *, uint32_t, uint16_t);
const struct dmesh_service *dmesh_registry_id(const struct dmesh_service_registry *, int);
#endif
