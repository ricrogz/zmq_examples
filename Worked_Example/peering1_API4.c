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

    void *ctx = zmq_ctx_new ();

    //  Bind state backend to endpoint
    zsock_t *statebe = zsock_new_pub(zsys_sprintf("ipc://%s-state.ipc", self));
    assert( zsock_is(statebe) );

    //  Connect statefe to all peers
    zsock_t *statefe = zsock_new_sub(NULL, "");
    int argn;
    for (argn = 2; argn < argc; argn++) {
        char *peer = argv [argn];
        printf ("I: connecting to state backend at '%s'\n", peer);
        int rc = zsock_connect (statefe, "ipc://%s-state.ipc", peer);
        assert(rc == 0);
    }


    //  The main loop sends out status messages to peers, and collects
    //  status messages back from peers.
    //  We use a zpoller for polling

    zpoller_t *poller = zpoller_new(statefe, NULL);

    // Keep working until poller is terminated
    while (! zpoller_terminated(poller)) {

        //  Poll for activity, or 1 second timeout
        void *which = zpoller_wait(poller, 1000 * ZMQ_POLL_MSEC);

        // Activity detected, handle incoming status messages
        if (which != NULL) {
            zmsg_t *msg = zmsg_recv (which);
            char *peer_name = zmsg_popstr (msg);
            char *available = zmsg_popstr (msg);
            printf ("%s - %s workers free\n", peer_name, available);
            free (peer_name);
            free (available);
            zmsg_destroy (&msg);
        }

        // No activity: notify self availability
        else if (zpoller_expired(poller)) {
            //  Send random values for worker availability
            zmsg_t *msg = zmsg_new();
            zmsg_addstr(msg, self);
            zmsg_addstr(msg, zsys_sprintf("%d", randof (10)));
            zmsg_send(&msg, statebe);
            zmsg_destroy(&msg);
        }

        // notify error state
        else if (errno){
            printf("\nError %d: %s\n", errno, zmq_strerror(errno));
            break;
        }
    }

    zsock_destroy(&statebe);
    zsock_destroy(&statefe);
    zmq_ctx_destroy (&ctx);
    return EXIT_SUCCESS;
}
