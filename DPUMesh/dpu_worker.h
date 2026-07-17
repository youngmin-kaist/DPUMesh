#ifndef DPU_WORKER_H
#define DPU_WORKER_H

struct objects;
struct global_config;

/* Multi-connection DPU worker, busy-poll variant: shared infra built up front,
 * then both PEs are busy-polled while per-connection state machines advance. */
void run_dpu_worker(struct objects *objs, const char *server_name);

/* Event-driven (on-demand) variant: epoll on both PE notification fds; the
 * process sleeps until the control or data path actually has work. */
void run_dpu_worker_event_driven(struct objects *objs, const char *server_name);

/* Multi-threaded DPU worker: spawns gcfg->num_threads shared-nothing threads.
 * Thread i owns its own device handles, comch server ("DPUMesh<i>"), PEs, DPA
 * instance + thread pool (DPA_THREAD_POOL_SIZE) and connection slots, and
 * serves its connections fully independently. */
void run_dpu_workers(const struct global_config *gcfg);

#endif // DPU_WORKER_H