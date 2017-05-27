//  Simple Pirate broker
//  This is identical to load-balancing pattern, with no reliability
//  mechanisms. It depends on the client for recovery. Runs forever.

#include "czmq.h"
#define WORKER_READY   "\001"      //  Signals worker is ready

int main (void)
{
    void *ctx = zmq_ctx_new ();
    zsock_t *frontend = zsock_new_router("tcp://*:5555");    //  For clients
    zsock_t *backend = zsock_new_router("tcp://*:5556");    //  For workers

    //  Queue of available workers
    zlist_t *workers = zlist_new ();
    
    //  The body of this example is exactly the same as lbbroker2.
    while (true) {
        zpoller_t *poller = zpoller_new(backend,
            zlist_size (workers) ? frontend : NULL, NULL);

        void *which = zpoller_wait(poller, -1);

        if (zpoller_terminated(poller))
            break;              //  Interrupted

        zmsg_t *msg = zmsg_recv (which);
        if (!msg)
            break;          //  Interrupted

        //  Handle worker activity on backend
        if (which == backend) {
            //  Use worker identity for load-balancing
            zframe_t *identity = zmsg_unwrap (msg);
            zlist_append (workers, identity);

            //  Forward message to client if it's not a READY
            zframe_t *frame = zmsg_first (msg);
            if (memcmp (zframe_data (frame), WORKER_READY, 1) != 0)
                zmsg_send (&msg, frontend);
        }
        else if (which == frontend) {
            //  Get client request, route to first available worker
            zmsg_wrap (msg, (zframe_t *) zlist_pop (workers));
            zmsg_send (&msg, backend);
        }
        else
            zmsg_destroy (&msg);
    }
    //  When we're done, clean up properly
    while (zlist_size (workers)) {
        zframe_t *frame = (zframe_t *) zlist_pop (workers);
        zframe_destroy (&frame);
    }
    zlist_destroy (&workers);
    zsock_destroy (&frontend);
    zsock_destroy (&backend);
    zmq_ctx_destroy (&ctx);
    return 0;
}