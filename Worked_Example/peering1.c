//  Broker peering simulation (part 1)
//  Prototypes the state flow

#include "czmq.h"

int main (int argc, char *argv [])
{
    //  First argument is this broker's name
    //  Other arguments are our peers' names
    //
    if (argc < 2) {
        printf ("syntax: peering1 me {you}…\n");
        return 0;
    }
    char *self = argv [1];
    printf ("I: preparing broker at %s…\n", self);
    srandom ((unsigned) time (NULL));

    int rc;
    void *ctx = zmq_ctx_new ();
    
    //  Bind state backend to endpoint
    zsock_t *statebe = zsock_new (ZMQ_PUB);
    rc = zsock_bind (statebe, "ipc://%s-state.ipc", self);
    assert(rc == 0);

    printf("statebe %s socket bound to %s\n", 
        zsock_type_str(statebe),
        zsock_last_endpoint(statebe));
    
    //  Connect statefe to all peers
    zsock_t *statefe = zsock_new (ZMQ_SUB);
    zsock_set_subscribe (statefe, "");
    int argn;
    for (argn = 2; argn < argc; argn++) {
        char *peer = argv [argn];
        printf ("I: connecting to state backend at '%s'\n", peer);
        rc = zsock_connect (statefe, "ipc://%s-state.ipc", peer);
        assert(rc == 0);
    }
    //  The main loop sends out status messages to peers, and collects
    //  status messages back from peers. The zmq_poll timeout defines
    //  our own heartbeat:


    // zmq_poll is low level, we need a socket handle:
    void *statefe_h = zsock_resolve(statefe);
    while (true) {
        //  Poll for activity, or 1 second timeout

        zmq_pollitem_t items [] = { { statefe_h, 0, ZMQ_POLLIN, 0 } };
        rc = zmq_poll (items, 1, 1000 * ZMQ_POLL_MSEC);
        if (rc == -1) {
            errno = zmq_errno();
            printf("Error %d: %s\n", errno, zmq_strerror(errno));
            break;              //  Interrupted
        }

        //  Handle incoming status messages
        if (items [0].revents & ZMQ_POLLIN) {
            char *peer_name = zstr_recv (statefe);
            char *available = zstr_recv (statefe);
            printf ("%s - %s workers free\n", peer_name, available);
            free (peer_name);
            free (available);
        }
        else {
            //  Send random values for worker availability
            zstr_sendm (statebe, self);
            zstr_sendf (statebe, "%d", randof (10));
        }
    }

    zsock_destroy(&statebe);
    zsock_destroy(&statefe);
    zmq_ctx_destroy (&ctx);
    return EXIT_SUCCESS;
}
