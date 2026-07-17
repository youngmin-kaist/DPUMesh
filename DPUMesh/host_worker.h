#ifndef HOST_WORKER_H
#define HOST_WORKER_H

struct objects;
struct global_config;

/* Single-connection host pipeline: connect to the DPU server, export DMA
 * metadata and push descriptors forever. Runs on the caller's thread with a
 * caller-owned struct objects (objs->dev must already be open). */
void
run_host_worker(struct objects *objs, const char *server_name);

/* Multi-threaded host worker: spawns gcfg->num_threads threads, each opening
 * its own DOCA device handle and one connection to the DPU worker. */
void
run_host_workers(const struct global_config *gcfg);

#endif /* HOST_WORKER_H */