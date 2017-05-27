all: std api4

std: peering1 peering2 peering3

api4: peering1_API4 peering2_API4 peering3_API4

peering1: peering1.c
	icc -O3 peering1.c -lczmq -lzmq -o peering1

peering2: peering2.c
	icc -O3 peering2.c -lczmq -lzmq -o peering2

peering3: peering3.c
	icc -O3 peering3.c -lczmq -lzmq -o peering3


peering1_API4: peering1_API4.c
	icc -O3 peering1_API4.c -lczmq -lzmq -o peering1_API4

peering2_API4: peering2_API4.c
	icc -O3 peering2_API4.c -lczmq -lzmq -o peering2_API4

peering3_API4: peering3_API4.c
	icc -O3 peering3_API4.c -lczmq -lzmq -o peering3_API4

clean:
	rm -f *[123] *_API4
