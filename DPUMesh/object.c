#include "object.h"

#include <string.h>

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>

#include "dma.h"

DOCA_LOG_REGISTER(OBJECT);

struct dmesh_conn *
dmesh_conn_get(struct objects *objs, struct doca_comch_connection *connection)
{
    int i;

    if (connection == NULL)
        return NULL;

    for (i = 0; i < DMESH_MAX_CONNECTIONS; i++) {
        if (objs->conns[i].state != DMESH_CONN_FREE &&
            objs->conns[i].connection == connection)
            return &objs->conns[i];
    }
    return NULL;
}

struct dmesh_conn *
dmesh_conn_open(struct objects *objs, struct doca_comch_connection *connection)
{
    struct dmesh_conn *conn;
    int i;

    conn = dmesh_conn_get(objs, connection);
    if (conn != NULL)
        return conn;

    for (i = 0; i < DMESH_MAX_CONNECTIONS; i++) {
        conn = &objs->conns[i];
        if (conn->state == DMESH_CONN_FREE) {
            memset(conn, 0, sizeof(*conn));
            conn->objs = objs;
            conn->connection = connection;
            conn->state = DMESH_CONN_NEW;
            DOCA_LOG_INFO("Bound connection %p to slot %d", (void *)connection, i);
            return conn;
        }
    }

    DOCA_LOG_WARN("No free connection slot (max %d)", DMESH_MAX_CONNECTIONS);
    return NULL;
}

void
dmesh_conn_close(struct objects *objs, struct doca_comch_connection *connection)
{
    struct dmesh_conn *conn = dmesh_conn_get(objs, connection);

    if (conn == NULL)
        return;

    /* NOTE: consumer/msgq/mmaps/buf_arr of this slot are not torn down yet
     * (no full per-connection teardown exists); the slot is only unbound. */
    conn->state = DMESH_CONN_FREE;
    conn->connection = NULL;
    DOCA_LOG_INFO("Unbound connection %p from slot %ld", (void *)connection, conn - objs->conns);
}

void
cleanup_objects(struct objects *objs)
{
    doca_error_t result;

    cleanup_dma_tasks(objs);

    if (objs->cc_server) {
        result = doca_comch_server_destroy(objs->cc_server);
        if(result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to destroy cc server properly with error = %s", doca_error_get_name(result));
        }   
        objs->cc_server = NULL;
    }

    if (objs->pe) {
        result = doca_pe_destroy(objs->pe);
        if(result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to destroy pe properly with error = %s", doca_error_get_name(result));
        }
        objs->pe = NULL;
    }

    if (objs->rep_dev) {
        result = doca_dev_rep_close(objs->rep_dev);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to close rep device properly with error = %s", doca_error_get_name(result));
        }
        objs->rep_dev = NULL;
    }

    if (objs->dev) {
        result = doca_dev_close(objs->dev);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to close device properly with error = %s", doca_error_get_name(result));
        }
        objs->dev = NULL;
    }
}
