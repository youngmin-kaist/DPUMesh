#ifndef DMA_H_
#define DMA_H_

#include <doca_error.h>
#include <doca_dev.h>
#include <doca_mmap.h>

struct objects;

/* Size of the local DMA buffer allocated on the DPU for PCI export. */
#define BUFFER_SIZE (1024 * 1024)

doca_error_t
alloc_buffer_and_set_mmap(struct doca_mmap **mmap, struct doca_dev *dev,
                        void **buffer, size_t buffer_size, uint32_t access_mask);

doca_error_t
init_dma_resources(struct objects *objs);

doca_error_t
send_dma_request_to_dpa(struct objects *objs);
#endif  /* DMA_H_ */