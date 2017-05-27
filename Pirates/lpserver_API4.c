//  Lazy Pirate server
//  Binds REQ socket to tcp://*:5555
//  Like hwserver except:
//   - echoes request as-is
//   - randomly runs slowly, or exits to simulate a crash.

#include "czmq.h"
#include <unistd.h>

int main (void)
{
    srandom ((unsigned) time (NULL));

    void *context = zmq_ctx_new ();
    zsock_t *server = zsock_new_rep ("tcp://*:5555");

    int cycles = 0;
    while (true) {
        char *request = zstr_recv (server);
        cycles++;

        //  Simulate various problems, after a few cycles
        if (cycles > 3 && randof (3) == 0) {
            printf ("I: simulating a crash\n");
            break;
        }
        else
        if (cycles > 3 && randof (3) == 0) {
            printf ("I: simulating CPU overload\n");
            zclock_sleep (2 * 1000);
        }
        printf ("I: normal request (%s)\n", request);
        zclock_sleep (1000);              //  Do some heavy work
        zstr_send (server, request);
        free (request);
    }
    zsock_destroy (&server);
    zmq_ctx_destroy (context);
    return 0;
}