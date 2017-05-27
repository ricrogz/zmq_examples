//  Broker peering simulation (part 2)
//  Prototypes the request-reply flow

#include "czmq.h"
#define NBR_CLIENTS 10
#define NBR_WORKERS 5
#define WORKER_READY   "\001"      //  Signals worker is ready

//  Our own name; in practice this would be configured per node
static char *self;

//  The client task does a request-reply dialog using a standard
//  synchronous REQ socket:

static void
client_task (zsock_t *pipe, void *args)
{
    zsock_signal (pipe, 0);

    void *ctx = zmq_ctx_new ();
    zsock_t *client = zsock_new_req(zsys_sprintf("ipc://%s-localfe.ipc", self));

    char client_id[6];
    sprintf (client_id, "C%04X", randof (0x10000));
    zsock_set_identity(client, client_id);

    zpoller_t *poller = zpoller_new(pipe, client, NULL);

    while (!zsys_interrupted) {
        //  Send request, get reply
        char req_id[6];
        sprintf (req_id, "%05X", randof (0x100000));
        zstr_send (client, zsys_sprintf("HELLO from %s / %s", client_id, req_id));

        void *which = zpoller_wait(poller, -1);

        // Handle termination
        if (zsys_interrupted || zpoller_terminated(poller) || which == pipe) {
            zsys_interrupted = 1;
            break;
        }

        // Handle worker message
        char *reply = zstr_recv (which);
        if (!reply)
            break;              //  Interrupted

        printf("Client %s: %s\n", client_id, reply);
        free(reply);
        zclock_sleep(1);
    }
    zsock_destroy(&client);
    zmq_ctx_destroy (&ctx);
}

//  The worker task plugs into the load-balancer using a REQ
//  socket:

static void
worker_task (zsock_t *pipe, void *args)
{
    zsock_signal (pipe, 0);

    void *ctx = zmq_ctx_new ();
    zsock_t *worker = zsock_new_req(zsys_sprintf("ipc://%s-localbe.ipc", self));

    char worker_id[6];
    sprintf (worker_id, "W%04X", randof (0x10000));
    zsock_set_identity(worker, worker_id);

    //  Tell broker we're ready for work
    zframe_t *frame = zframe_new (WORKER_READY, 1);
    zframe_send (&frame, worker, 0);

    zpoller_t *poller = zpoller_new(pipe, worker, NULL);

    //  Process messages as they arrive
    while (!zsys_interrupted) {

        void *which = zpoller_wait(poller, -1);

        // Handle termination
        if (zsys_interrupted || zpoller_terminated(poller) || which == pipe) {
            zsys_interrupted = 1;
            break;
        }

        zmsg_t *msg = zmsg_recv (worker);
        if (!msg)
            break;              //  Interrupted

        zframe_print (zmsg_last (msg), zsys_sprintf("Worker %s: ", worker_id));

        char req_id[6];
        char client_id[6];

        char *data = (char *) zframe_data (zmsg_last (msg));
        sscanf(data, "HELLO from %5s / %5s", client_id, req_id);

        zframe_reset (zmsg_last (msg), zsys_sprintf("OK %s from %s", req_id, worker_id), 19);
        zmsg_send (&msg, worker);
    }
    zsock_destroy(&worker);
    zmq_ctx_destroy (&ctx);
}

//  The main task begins by setting-up its frontend and backend sockets
//  and then starting its client and worker tasks:

int main (int argc, char *argv [])
{
    //  First argument is this broker's name
    //  Other arguments are our peers' names
    //
    if (argc < 2) {
        printf ("syntax: peering2 me {you}…\n");
        return 0;
    }
    self = argv [1];
    printf ("I: preparing broker at %s…\n", self);
    srandom ((unsigned) time (NULL));

    void *ctx = zmq_ctx_new ();

    //  Bind cloud frontend to endpoint
    zsock_t *cloudfe = zsock_new_router(zsys_sprintf("ipc://%s-cloud.ipc", self));
    zsock_set_identity (cloudfe, self);

    //  Connect cloud backend to all peers
    zsock_t *cloudbe = zsock_new_router(NULL);
    zsock_set_identity (cloudbe, self);
    int argn;
    for (argn = 2; argn < argc; argn++) {
        char *peer = argv [argn];
        printf ("I: connecting to cloud frontend at '%s'\n", peer);
        int rc = zsock_connect (cloudbe, "ipc://%s-cloud.ipc", peer);
        assert( rc == 0 );
    }
    //  Prepare local frontend and backend
    zsock_t *localfe = zsock_new_router(zsys_sprintf("ipc://%s-localfe.ipc", self));
    zsock_t *localbe = zsock_new_router(zsys_sprintf("ipc://%s-localbe.ipc", self));

    //  Get user to tell us when we can start…
    printf ("Press Enter when all brokers are started: ");
    getchar ();

    //  Start local workers
    int worker_nbr;
    zlist_t *wactors = zlist_new();
    for (worker_nbr = 0; worker_nbr < NBR_WORKERS; worker_nbr++)
        zlist_append(wactors, zactor_new (worker_task, NULL));

    //  Start local clients
    int client_nbr;
    zlist_t *cactors = zlist_new();
    for (client_nbr = 0; client_nbr < NBR_CLIENTS; client_nbr++)
        zlist_append(cactors, zactor_new (client_task, NULL));

    // Interesting part
    //  Here, we handle the request-reply flow. We're using load-balancing
    //  to poll workers at all times, and clients only when there are one //
    //  or more workers available.//

    //  Least recently used queue of available workers
    int capacity = 0;
    zlist_t *workers = zlist_new ();

    zpoller_t *be_poller = zpoller_new(localbe, cloudbe, NULL);
    zpoller_t *fe_poller = zpoller_new(localfe, cloudfe, NULL);

    while (!zsys_interrupted) {

        //  Poll for activity, or 1 second timeout
        void *be_which = zpoller_wait(be_poller, capacity ? 1000 * ZMQ_POLL_MSEC : -1);

        // Handle termination
        if (zsys_interrupted || zpoller_terminated(be_poller) || zpoller_terminated(fe_poller)) {
            printf("\nCaught interruption singal, cleaning up and quitting...\n");
            zsys_interrupted = 1;
            break;
        }

        zmsg_t *msg = NULL;

        //  Handle reply from local worker
        if (be_which == localbe) {
            msg = zmsg_recv (localbe);
            if (!msg)
                break;          //  Interrupted
            zframe_t *identity = zmsg_unwrap (msg);
            zlist_append (workers, identity);
            capacity++;

            //  If it's READY, don't route the message any further
            zframe_t *frame = zmsg_first (msg);
            if (memcmp (zframe_data (frame), WORKER_READY, 1) == 0)
                zmsg_destroy (&msg);
        }

        //  Handle reply from peer broker
        else if (be_which == cloudbe) {
            msg = zmsg_recv (cloudbe);
            if (!msg)
                break;          //  Interrupted

            //  We don't use peer broker identity for anything
            zframe_t *identity = zmsg_unwrap (msg);
            zframe_destroy (&identity);
        }
        else if (!zpoller_expired(be_poller) && errno){
            printf("\nError %d: %s\n", errno, zmq_strerror(errno));
            break;
        }


        //  Route reply to cloud if it's addressed to a broker
        for (argn = 2; msg && argn < argc; argn++) {
            char *data = (char *) zframe_data (zmsg_first (msg));
            size_t size = zframe_size (zmsg_first (msg));
            if (size == strlen (argv [argn])
                &&  memcmp (data, argv [argn], size) == 0)
                zmsg_send (&msg, cloudfe);
        }
        //  Route reply to client if we still need to
        if (msg)
            zmsg_send (&msg, localfe);

        //  Now we route as many client requests as we have worker capacity
        //  for. We may reroute requests from our local frontend, but not from //
        //  the cloud frontend. We reroute randomly now, just to test things
        //  out. In the next version, we'll do this properly by calculating
        //  cloud capacity://

        while (capacity) {

            // Handle termination
            if (zsys_interrupted || zpoller_terminated(be_poller) || zpoller_terminated(fe_poller)) {
                printf("\nCaught interruption singal, cleaning up and quitting...\n");
                zsys_interrupted = 1;
                break;
            }

            int reroutable = 0;
            void *fe_which = zpoller_wait(fe_poller, 0);

            //  We'll do peer brokers first, to prevent starvation
            if (fe_which == cloudfe) {
                msg = zmsg_recv (cloudfe);
                reroutable = 0;
            }
            else if (fe_which == localfe) {
                msg = zmsg_recv (localfe);
                reroutable = 1;
            }
            else
                break;      //  No work, go back to backends

            //  If reroutable, send to cloud 20% of the time
            //  Here we'd normally use cloud status information
            //
            if (reroutable && argc > 2 && randof (5) == 0) {
                //  Route to random broker peer
                int peer = randof (argc - 2) + 2;
                zmsg_pushmem (msg, argv [peer], strlen (argv [peer]));
                zmsg_send (&msg, cloudbe);
            }
            else {
                zframe_t *frame = (zframe_t *) zlist_pop (workers);
                zmsg_wrap (msg, frame);
                zmsg_send (&msg, localbe);
                capacity--;
            }
        }
    }

    //  When we're done, clean up properly
    while (zlist_size (workers)) {
        zframe_t *frame = (zframe_t *) zlist_pop (workers);
        zframe_destroy (&frame);
    }
    zlist_destroy (&workers);

    // Cleanup actors
    while (zlist_size (cactors)) {
        zactor_t *actor = (zactor_t *) zlist_pop(cactors);
        zactor_destroy(&actor);
    }
    while (zlist_size (wactors)) {
        zactor_t *actor = (zactor_t *) zlist_pop(wactors);
        zactor_destroy(&actor);
    }
    zlist_destroy (&wactors);
    zlist_destroy (&cactors);

    zsock_destroy(&cloudfe);
    zsock_destroy(&cloudbe);
    zsock_destroy(&localfe);
    zsock_destroy(&localbe);

    zmq_ctx_destroy (&ctx);
    return EXIT_SUCCESS;
}
