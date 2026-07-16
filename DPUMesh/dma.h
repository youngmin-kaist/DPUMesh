#ifndef DMA_H_
#define DMA_H_

#include <doca_error.h>
#include <doca_buf.h>
#include <doca_dev.h>
#include <doca_mmap.h>

struct objects;
struct doca_dma_task_memcpy;

/* Size of the local DMA buffer allocated on the DPU for PCI export. */
#define BUFFER_SIZE (1024 * 1024)

doca_error_t
alloc_buffer_and_set_mmap(struct doca_mmap **mmap, struct doca_dev *dev,
                        void **buffer, size_t buffer_size, uint32_t access_mask);

doca_error_t
init_dma_resources(struct objects *objs);

doca_error_t
init_dma_tasks(struct objects *objs, int num_tasks);

void
cleanup_dma_tasks(struct objects *objs);

doca_error_t
submit_dma_task(struct objects *objs, const struct doca_buf *src, struct doca_buf *dst);

doca_error_t
enqueue_dma_task(struct objects *objs, const struct doca_buf *src, struct doca_buf *dst);

/* Submit up to max_tasks queued tasks. A max_tasks value of zero processes the entire queue. */
doca_error_t
progress_dma_submission_queue(struct objects *objs, int max_tasks, int *num_submitted);

struct doca_dma_task_memcpy *
get_free_dma_task(struct objects *objs);

doca_error_t
put_free_dma_task(struct objects *objs, struct doca_dma_task_memcpy *dma_task);

struct doca_dma_task_memcpy *
get_submission_dma_task(struct objects *objs);

doca_error_t
put_submission_dma_task(struct objects *objs, struct doca_dma_task_memcpy *dma_task);

int
get_num_free_dma_tasks(const struct objects *objs);

int
get_num_submission_dma_tasks(const struct objects *objs);

doca_error_t
send_dma_request_to_dpa(struct objects *objs);
#endif  /* DMA_H_ */
