CC=gcc
CFLAGS= -Wall -Wextra -Wpedantic -Werror -O3 -lpthread

all: test

map.o: map.c map.h

test: map.o test.c

.PHONY:
clean:
	rm -f test *.o
