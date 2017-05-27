//  Broker peering simulation (part 3)
//  Prototypes the full flow of status and tasks

#include "czmq.h"
#define NBR_CLIENTS 10
#define NBR_WORKERS 5
#define WORKER_READY   "\001"      //  Signals worker is ready

//  Our own name; in practice, this would be configured per node
static char *self;

//  This is the client task. It issues a burst of requests and then
//  sleeps for a few seconds. This simulates sporadic activity; when
//  a number of clients are active at once, the local workers should
//  be overloaded. The client uses a REQ socket for requests and also
//  pushes statistics to the monitor socket:

static void
client_task (zsock_t *pipe, void *args)
{
    zsock_signal (pipe, 0);

    void *ctx = zmq_ctx_new ();
    zsock_t *client = zsock_new_req(zsys_sprintf("ipc://%s-localfe.ipc", self));
    zsock_t *monitor = zsock_new_push(zsys_sprintf("ipc://%s-monitor.ipc", self));

    zpoller_t *poller = zpoller_new(pipe, client, NULL);

    while (!zsys_interrupted) {
        zclock_sleep ((uint) randof (5));
        int burst = randof (15);
        while (burst--) {
            char task_id [5];
            sprintf (task_id, "%04X", randof (0x10000));

            //  Send request with random hex ID
            zstr_send (client, task_id);

            //  Wait max ten seconds for a reply, then complain
            void *which = zpoller_wait(poller, 10 * 1000 * ZMQ_POLL_MSEC);

            // Termination: exit
            if (zsys_interrupted || zpoller_terminated(poller) || which == pipe) {
                zsys_interrupted = 1;
                break;
            }

            // No answer -- leave
            else if (zpoller_expired(poller)) {
                zstr_sendf (monitor, "E: CLIENT EXIT - lost task %s", task_id);
                break;
            }

            // Got a message from client: get and process it
            char *msg = zstr_recv (which);
            if (!msg)
                break;              //  Interrupted

            //  Worker is supposed to answer us with our task id
            assert (streq (msg, task_id));
            zstr_sendf (monitor, "%s", msg);
            free (msg);
        }
    }
    zpoller_destroy(&poller);
    zsock_destroy(&client);
    zsock_destroy(&monitor);
    zmq_ctx_destroy (&ctx);
}

//  This is the worker task, which uses a REQ socket to plug into the
//  load-balancer. It's the same stub worker task that you've seen in
//  other examples:

static void
worker_task (zsock_t *pipe, void * args)
{
    zsock_signal (pipe, 0);

    void *ctx = zmq_ctx_new ();
    zsock_t *worker = zsock_new_req(zsys_sprintf("ipc://%s-localbe.ipc", self));

    //  Tell broker we're ready for work
    zframe_t *frame = zframe_new (WORKER_READY, 1);
    zframe_send (&frame, worker, 0);

    zpoller_t *poller = zpoller_new(pipe, worker, NULL);

    //  Process messages as they arrive
    while (!zsys_interrupted) {

        void *which = zpoller_wait(poller, -1);

        if (zsys_interrupted || zpoller_terminated(poller) || which == pipe) {
            zsys_interrupted = 1;
            break;
        }

        // Message is from client: process
        zmsg_t *msg = zmsg_recv (which);
        if (!msg)
            break;              //  Interrupted

        //  Workers are busy for 0/1 seconds
        zclock_sleep((uint) randof (2));
        zmsg_send(&msg, worker);
    }
    zpoller_destroy(&poller);
    zsock_destroy(&worker);
    zmq_ctx_destroy (&ctx);
}

//  The main task begins by setting up all its sockets. The local frontend
//  talks to clients, and our local backend talks to workers. The cloud
//  frontend talks to peer brokers as if they were clients, and the cloud
//  backend talks to peer brokers as if they were workers. The state
//  backend publishes regular state messages, and the state frontend
//  subscribes to all state backends to collect these messages. Finally,
//  we use a PULL monitor socket to collect printable messages from tasks:

int main (int argc, char *argv [])
{
    //  First argument is this broker's name
    //  Other arguments are our peers' names
    if (argc < 2) {
        printf ("syntax: peering3 me {you}…\n");
        return 0;
    }
    self = argv [1];
    printf ("I: preparing broker at %s…\n", self);
    srandom ((unsigned) time (NULL));

    //  Prepare local frontend and backend
    void *ctx = zmq_ctx_new ();
    zsock_t *localfe = zsock_new_router (zsys_sprintf("ipc://%s-localfe.ipc", self));
    zsock_t *localbe = zsock_new_router (zsys_sprintf("ipc://%s-localbe.ipc", self));

    //  Bind cloud frontend to endpoint
    zsock_t *cloudfe = zsock_new_router (zsys_sprintf("ipc://%s-cloud.ipc", self));
    zsock_set_identity (cloudfe, self);

    //  Connect cloud backend to all peers
    zsock_t *cloudbe = zsock_new_router (NULL);
    zsock_set_identity (cloudbe, self);

    int argn;
    for (argn = 2; argn < argc; argn++) {
        char *peer = argv [argn];
        printf ("I: connecting to cloud frontend at '%s'\n", peer);
        int rc = zsock_connect (cloudbe, "ipc://%s-cloud.ipc", peer);
        assert(rc == 0);
    }
    //  Bind state backend to endpoint
    zsock_t *statebe = zsock_new_pub (zsys_sprintf("ipc://%s-state.ipc", self));

    //  Connect state frontend to all peers
    zsock_t *statefe = zsock_new_sub(NULL, "");
    for (argn = 2; argn < argc; argn++) {
        char *peer = argv [argn];
        printf ("I: connecting to state backend at '%s'\n", peer);
        int rc = zsock_connect (statefe, "ipc://%s-state.ipc", peer);
        assert(rc == 0);
    }

    //  Prepare monitor socket
    zsock_t *monitor = zsock_new_pull (zsys_sprintf("ipc://%s-monitor.ipc", self));

    //  After binding and connecting all our sockets, we start our child
    //  tasks - workers and clients:

    int worker_nbr;
    zlist_t *wactors = zlist_new();
    for (worker_nbr = 0; worker_nbr < NBR_WORKERS; worker_nbr++)
        zlist_append(wactors, zactor_new (worker_task, NULL));

    //  Start local clients
    int client_nbr;
    zlist_t *cactors = zlist_new();
    for (client_nbr = 0; client_nbr < NBR_CLIENTS; client_nbr++)
        zlist_append(cactors, zactor_new (client_task, NULL));

    //  Queue of available workers
    int local_capacity = 0;
    int cloud_capacity = 0;
    zlist_t *workers = zlist_new ();

    //  The main loop has two parts. First, we poll workers and our two service
    //  sockets (statefe and monitor), in any case. If we have no ready workers,
    //  then there's no point in looking at incoming requests. These can remain //
    //  on their internal 0MQ queues://

    zpoller_t *primary_poller = zpoller_new(localbe, cloudbe, statefe, monitor, NULL);

    while (!zsys_interrupted) {

        void *primary_which = zpoller_wait(primary_poller, local_capacity? 1000 * ZMQ_POLL_MSEC: -1);

        if (zsys_interrupted || zpoller_terminated(primary_poller)) {
            printf("\nCaught interruption singal, cleaning up and quitting...\n");
            zsys_interrupted = 1;
            break;
        }

        //  Track if capacity changes during this iteration
        int previous = local_capacity;
        zmsg_t *msg = NULL;     //  Reply from local worker

        if (primary_which == localbe) {
            msg = zmsg_recv (localbe);
            if (!msg)
                break;          //  Interrupted
            zframe_t *identity = zmsg_unwrap (msg);
            zlist_append (workers, identity);
            local_capacity++;

            //  If it's READY, don't route the message any further
            zframe_t *frame = zmsg_first (msg);
            if (memcmp (zframe_data (frame), WORKER_READY, 1) == 0)
                zmsg_destroy (&msg);
        }

        //  Or handle reply from peer broker
        else if (primary_which == cloudbe) {
            msg = zmsg_recv (cloudbe);
            if (!msg)
                break;          //  Interrupted
            //  We don't use peer broker identity for anything
            zframe_t *identity = zmsg_unwrap (msg);
            zframe_destroy (&identity);
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

        //  If we have input messages on our statefe or monitor sockets, we
        //  can process these immediately:

        if (primary_which == statefe) {
            char *peer = zstr_recv (statefe);
            char *status = zstr_recv (statefe);
            cloud_capacity = atoi (status);
            free (peer);
            free (status);
        }
        if (primary_which == monitor) {
            char *status = zstr_recv (monitor);
            printf ("%s\n", status);
            free (status);
        }
        //  Now route as many clients requests as we can handle. If we have
        //  local capacity, we poll both localfe and cloudfe. If we have cloud
        //  capacity only, we poll just localfe. We route any request locally
        //  if we can, else we route to the cloud.

        while (local_capacity + cloud_capacity) {
            zpoller_t *secondary_poller = zpoller_new(localfe, local_capacity ? cloudfe : NULL, NULL);

            void *secondary_which = zpoller_wait(secondary_poller, 0);

            // Check interruption
            if (zsys_interrupted || zpoller_terminated(secondary_poller)) {
                printf("\nCaught interruption singal, cleaning up and quitting...\n");
                zsys_interrupted = 1;
                break;
            }

            // No messages
            else if (zpoller_expired(secondary_poller))
                break; //  No work, go back to primary


            // Read message
            msg = zmsg_recv (secondary_which);

            if (local_capacity) {
                zframe_t *frame = (zframe_t *) zlist_pop (workers);
                zmsg_wrap (msg, frame);
                zmsg_send (&msg, localbe);
                local_capacity--;
            }
            else {
                //  Route to random broker peer
                int peer = randof (argc - 2) + 2;
                zmsg_pushmem (msg, argv [peer], strlen (argv [peer]));
                zmsg_send (&msg, cloudbe);
            }
        }
        //  We broadcast capacity messages to other peers; to reduce chatter,
        //  we do this only if our capacity changed.

        if (local_capacity != previous) {
            //  We stick our own identity onto the envelope
            zstr_sendm (statebe, self);
            //  Broadcast new capacity
            zstr_sendf (statebe, "%d", local_capacity);
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

    zsock_destroy(&localfe);
    zsock_destroy(&localbe);
    zsock_destroy(&cloudfe);
    zsock_destroy(&cloudbe);
    zsock_destroy(&statefe);
    zsock_destroy(&statebe);
    zsock_destroy(&monitor);

    zmq_ctx_destroy (&ctx);
    return EXIT_SUCCESS;
}