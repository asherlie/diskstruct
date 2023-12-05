CC=gcc
CFLAGS= -Wall -Wextra -Wpedantic -Werror -O3 -lpthread

all: test

map.o: map.c map.h

test: map.o test.c
test_gprof: test.c map.c
	gcc -Wall -pg test.c map.c -o test_gprof

gmon.out: test_gprof
	./test_gprof

gprof_results: gmon.out
	gprof test_gprof gmon.out > gprof_results

.PHONY:
clean:
	rm -f test test_gprof gmon.out gprof_results *.o
