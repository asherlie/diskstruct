CC=gcc
CFLAGS= -Wall -Wextra -Wpedantic -Werror -g3 -lpthread

all: test

ins_queue.o: ins_queue.c ins_queue.h

map.o: map.c map.h

test: map.o ins_queue.o test.c
test_gprof: test.c map.c
	gcc -Wall -pg test.c map.c -o test_gprof

gmon.out: test_gprof
	./test_gprof

gprof_results: gmon.out
	gprof test_gprof gmon.out > gprof_results

.PHONY:
clean:
	rm -f test test_gprof gmon.out gprof_results *.o
