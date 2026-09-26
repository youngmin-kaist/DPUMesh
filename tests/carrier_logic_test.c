#include "../src/core/carrier.c"
#include <assert.h>
#include <stdio.h>
static unsigned arm_calls;
static int arm_result;

int channel_conn_arm(struct channel_conn *conn)
{
    (void)conn;
    ++arm_calls;
    return arm_result;
}

int channel_dev_host_dpa(const struct channel_dev *dev)
{
    (void)dev;
    return 1; /* The control-only wakeup regression affects idle host-DPA. */
}

static void test_shared_control_poll_tick(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0);
    assert(dmesh_native_stripe_arm(t, -1) == 0);
    assert(dmesh_native_stripe_arm(t, SLOTS) == 0);
    assert(dmesh_native_stripe_arm(t, 0) == 0); /* no live flow */
    s->state = SLOT_OPEN;
    assert(dmesh_native_stripe_arm(t, 0) == 1);
    assert(s->armed && arm_calls == 1);
    /* No TX ticket or reverse DMA completion can wake the EQ. A session
     * ERROR/disconnect still needs polling after every subsequent empty poll. */
    for (int i = 0; i < 4; ++i)
        assert(dmesh_native_stripe_arm(t, 0) == 1);
    assert(arm_calls == 1); /* preserve one-shot doorbell arming */
    s->armed = 0;
    arm_result = -1;
    assert(dmesh_native_stripe_arm(t, 0) == 1 && !s->armed);
    s->state = SLOT_CLOSED;
    assert(dmesh_native_stripe_arm(t, 0) == 0);
    s->fin_pending = 1;
    assert(dmesh_native_stripe_arm(t, 0) == 1);
    pthread_mutex_destroy(&s->lock);
    free(t);
}

int main(void)
{
    test_shared_control_poll_tick();
    uint32_t p[2];
    assert(carrier_chunks(0, p) == 0);
    assert(carrier_chunks(1, p) == 1 && p[0] == 1);
    assert(carrier_chunks(128, p) == 1 && p[0] == 128);
    assert(carrier_chunks(129, p) == 2 && p[0] == 128 && p[1] == 1);
    assert(carrier_chunks(300, p) == 2 && p[0] == 256 && p[1] == 44);
    assert(carrier_chunks(8064, p) == 1 && p[0] == 8064);
    assert(carrier_chunks(8100, p) == 2 && p[0] == 8064 && p[1] == 36);
    assert(carrier_chunks(8192, p) == 2 && p[0] == 8064 && p[1] == 128);
    struct carrier_rx_window w; carrier_window_init(&w);
    uint64_t seq, bytes;
    assert(!carrier_window_advance(&w, &seq, &bytes) && seq == 0 && bytes == 0);
    assert(carrier_window_add(&w, 1, 0, 100) == 0);
    assert(carrier_window_add(&w, 2, 128, 200) == 0);
    assert(carrier_window_add(&w, 3, 384, 300) == 0);
    assert(carrier_window_release(&w, 128) == 0);           /* out of order */
    assert(!carrier_window_advance(&w, &seq, &bytes) && seq == 0);
    assert(carrier_window_release(&w, 0) == 0);
    assert(carrier_window_advance(&w, &seq, &bytes) && seq == 2 && bytes == 300);
    assert(carrier_window_release(&w, 999) == -1);
    assert(carrier_window_release(&w, 384) == 0);
    assert(carrier_window_advance(&w, &seq, &bytes) && seq == 3 && bytes == 600);
    assert(carrier_window_add(&w, 1 + CHANNEL_DESC_N, 0, 8) == 0);  /* slot reuse after retirement */
    puts("carrier logic: chunking, release window, shared-control polling: PASS");
    return 0;
}
