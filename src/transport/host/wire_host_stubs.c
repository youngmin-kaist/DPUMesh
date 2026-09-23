/* Server-only exports referenced by shared DPUMesh host sources. A host
 * process never owns a Comch server, so these paths cannot run here. */
#include "object.h"
#include "comch_server.h"
#include "dma.h"

doca_error_t server_send_msg_conn(struct objects *objs, struct doca_comch_connection *connection,
                                  const char *msg, size_t len)
{
    (void)objs; (void)connection; (void)msg; (void)len;
    return DOCA_ERROR_NOT_SUPPORTED;
}
void cleanup_dma_tasks(struct dmesh_conn *conn) { (void)conn; }
