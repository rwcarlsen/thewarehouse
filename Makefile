
CC ?= gcc
CXX ?= g++

src := $(shell ls *.cc)
sqlitelib := libsqlite3.a

tw: $(src) $(sqlitelib)
	$(CXX) -std=c++11 -O3 -g $(CXXFLAGS) $^ -o $@

$(sqlitelib): sqlite/sqlite3.c
	$(CC) -O3 -c $< -o sqlite.o
	ar rcs $@ sqlite.o

.PHONY: clean

clean:
	rm -f tw $(sqlitelib) sqlite.o
