#include "service_registry.h"
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const struct dmesh_service *dmesh_registry_name(const struct dmesh_service_registry *r, const char *name) {
    if (name) for (size_t i = 0; i < r->count; ++i)
        if (!strcmp(r->entries[i].name, name)) return &r->entries[i];
    return NULL;
}
const struct dmesh_service *dmesh_registry_addr(const struct dmesh_service_registry *r, uint32_t addr, uint16_t port) {
    for (size_t i = 0; i < r->count; ++i)
        if (r->entries[i].ipv4 == addr && r->entries[i].port == port) return &r->entries[i];
    return NULL;
}
const struct dmesh_service *dmesh_registry_id(const struct dmesh_service_registry *r, int id) {
    for (size_t i = 0; i < r->count; ++i)
        if (r->entries[i].id == id) return &r->entries[i];
    return NULL;
}
int dmesh_registry_load(struct dmesh_service_registry *r, const char *path) {
    if (!r || !path || !*path) { errno = EINVAL; return -1; }
    FILE *f = fopen(path, "re");
    if (!f) return -1;
    struct dmesh_service_registry parsed = {0};
    char line[512];
    int error = 0;
    while (fgets(line, sizeof(line), f)) {
        if (!strchr(line, '\n') && !feof(f)) { error = EINVAL; break; }
        char *comment = strchr(line, '#'); if (comment) *comment = 0;
        char *p = line; while (isspace((unsigned char)*p)) ++p;
        if (!*p) continue;
        char addr[64], name[128], idtext[32], extra;
        if (sscanf(p, "%63s %127s %31s %c", addr, name, idtext, &extra) != 3) { error = EINVAL; break; }
        char *colon = strrchr(addr, ':');
        if (!colon || colon == addr) { error = EINVAL; break; }
        *colon++ = 0;
        char *end;
        long port = strtol(colon, &end, 10);
        if (!*colon || *end || port < 1 || port > 65535) { error = EINVAL; break; }
        long id = strtol(idtext, &end, 10);
        if (!*idtext || *end || id < 0 || id >= DMESH_REGISTRY_MAX) { error = EINVAL; break; }
        struct dmesh_service e = {.port = (uint16_t)port, .id = (int16_t)id};
        if (inet_pton(AF_INET, addr, &e.ipv4) != 1) { error = EINVAL; break; }
        for (const char *n = name; *n; ++n)
            if (!isalnum((unsigned char)*n) && *n != '.' && *n != '-' && *n != '_' && *n != '/') error = EINVAL;
        if (error) break;
        memcpy(e.name, name, strlen(name) + 1);
        if (dmesh_registry_name(&parsed, name) || dmesh_registry_id(&parsed, id) ||
            dmesh_registry_addr(&parsed, e.ipv4, e.port)) { error = EEXIST; break; }
        if (parsed.count == DMESH_REGISTRY_MAX) { error = ENOSPC; break; }
        parsed.entries[parsed.count++] = e;
    }
    if (ferror(f)) error = EIO;
    fclose(f);
    if (error) { errno = error; return -1; }
    *r = parsed;
    return 0;
}
