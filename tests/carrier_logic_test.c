#include "src/core/carrier_logic.h"
#include <assert.h>
#include <stdio.h>
int main(void)
{
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
    puts("carrier push logic: chunking and release window: PASS");
    return 0;
}
