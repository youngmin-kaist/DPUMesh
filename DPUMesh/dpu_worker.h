#ifndef DPU_WORKER_H
#define DPU_WORKER_H

struct objects;

/* Multi-connection DPU worker, busy-poll variant: shared infra built up front,
 * then both PEs are busy-polled while per-connection state machines advance. */
void run_dpu_worker(struct objects *objs);

/* Event-driven (on-demand) variant: epoll on both PE notification fds; the
 * process sleeps until the control or data path actually has work. */
void run_dpu_worker_event_driven(struct objects *objs);

#endif // DPU_WORKER_H