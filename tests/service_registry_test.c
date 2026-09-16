#include "src/core/service_registry.h"
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
static void write_config(const char *path, const char *text) {
    FILE *f = fopen(path, "w"); assert(f); assert(fputs(text, f) >= 0); assert(!fclose(f));
}
int main(void) {
    char path[] = "/tmp/dmesh-registry-XXXXXX";
    int fd = mkstemp(path); assert(fd >= 0); close(fd);
    struct dmesh_service_registry r = {0}, saved;
    write_config(path, "# static services\n10.0.0.1:80 ns/echo 0\n10.0.0.2:9092 other 126\n");
    assert(!dmesh_registry_load(&r, path)); assert(r.count == 2);
    assert(dmesh_registry_name(&r, "ns/echo") == dmesh_registry_id(&r, 0));
    assert(dmesh_registry_addr(&r, inet_addr("10.0.0.2"), 9092)->id == 126);
    saved = r;
    const char *bad[] = {"10.0.0.1:0 a 0", "10.0.0.1:80 a 127", "10.0.0.1:80 a -1",
        "300.0.0.1:80 a 0", "10.0.0.1:80 a 0 extra", "10.0.0.1:80 a 0\n10.0.0.2:80 a 1",
        "10.0.0.1:80 a 0\n10.0.0.1:80 b 1", "10.0.0.1:80 a 0\n10.0.0.2:80 b 0"};
    for (size_t i = 0; i < sizeof(bad)/sizeof(*bad); ++i) {
        write_config(path, bad[i]); assert(dmesh_registry_load(&r, path) == -1);
        assert(!memcmp(&r, &saved, sizeof(r)));
    }
    unlink(path); puts("service registry validation and transactional reload: PASS");
}
