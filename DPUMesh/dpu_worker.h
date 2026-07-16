#ifndef DPU_WORKER_H
#define DPU_WORKER_H

struct objects;
void run_dpu_worker(struct objects *objs);

/* Event-driven (on-demand) variant of run_dpu_worker: replaces the control-path
 * busy-poll waits with epoll on the control PE notification fd. */
void run_dpu_worker_event_driven(struct objects *objs);

/* Steady-state data-path loop (busy-polls objs->consumer_pe), extracted so both
 * the baseline and the event-driven worker can reuse it. Does not return. */
void dmesh_doca_run_datapath(struct objects *objs);

#endif // DPU_WORKER_H