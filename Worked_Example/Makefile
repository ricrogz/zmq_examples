all: std api4

std: test1 test2 test3

api4: test1_API4 test2_API4 test3_API4

test1:
	icc -O3 peering1.c -lczmq -lzmq -o peering1

test2:
	icc -O3 peering2.c -lczmq -lzmq -o peering2

test3:
	icc -O3 peering3.c -lczmq -lzmq -o peering3


test1_API4:
	icc -O3 peering1_API4.c -lczmq -lzmq -o peering1_API4

test2_API4:
	icc -O3 peering2_API4.c -lczmq -lzmq -o peering2_API4

test3_API4:
	icc -O3 peering3_API4.c -lczmq -lzmq -o peering3_API4

clean:
	rm -f *[123] *_API4
