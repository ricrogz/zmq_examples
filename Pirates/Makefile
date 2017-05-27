all: std api4

std: lpclient lpserver spqueue spworker ppqueue ppworker

api4: lpclient_API4 lpserver_API4 spqueue_API4 spworker_API4 ppqueue_API4 ppworker_API4


lpclient: lpclient.c
	icc -O3 lpclient.c -lczmq -lzmq -o lpclient

lpserver: lpserver.c
	icc -O3 lpserver.c -lczmq -lzmq -o lpserver

spqueue: spqueue.c
	icc -O3 spqueue.c -lczmq -lzmq -o spqueue

spworker: spworker.c
	icc -O3 spworker.c -lczmq -lzmq -o spworker

ppqueue: ppqueue.c
	icc -O3 ppqueue.c -lczmq -lzmq -o ppqueue

ppworker: ppworker.c
	icc -O3 ppworker.c -lczmq -lzmq -o ppworker


lpclient_API4: lpclient_API4.c
	icc -O3 lpclient_API4.c -lczmq -lzmq -o lpclient_API4

lpserver_API4: lpserver_API4.c
	icc -O3 lpserver_API4.c -lczmq -lzmq -o lpserver_API4

spqueue_API4: spqueue_API4.c
	icc -O3 spqueue_API4.c -lczmq -lzmq -o spqueue_API4

spworker_API4: spworker_API4.c
	icc -O3 spworker_API4.c -lczmq -lzmq -o spworker_API4

ppqueue_API4: ppqueue_API4.c
	icc -O3 ppqueue_API4.c -lczmq -lzmq -o ppqueue_API4

ppworker_API4: ppworker_API4.c
	icc -O3 ppworker_API4.c -lczmq -lzmq -o ppworker_API4


clean:
	rm -f *client *server *queue *worker *_API4
