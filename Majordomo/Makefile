all: mdclient mdworker mdbroker mdclient2

mdbroker: mdbroker.c
	icc -O3 mdbroker.c -lczmq -lzmq -o mdbroker

mdworker: mdworker.c mdwrkapi.c
	icc -O3 mdworker.c -lczmq -lzmq -o mdworker

mdclient: mdclient.c mdcliapi.c
	icc -O3 mdclient.c -lczmq -lzmq -o mdclient

mdclient2: mdclient2.c mdcliapi2.c
	icc -O3 mdclient2.c -lczmq -lzmq -o mdclient2


clean:
	rm -f *client *worker *broker *client2
