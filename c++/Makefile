CXX=clang++
CXXFLAGS=-Wall -Werror -Wextra -pedantic -std=c++17 -lpthread

test: test.cc memstore.h memstore.cc kvstore.h paxos.cc paxos.h consensus.h command.h replicant.cc replicant.h
	$(CXX) $(CXXFLAGS) test.cc replicant.cc paxos.cc memstore.cc -o test
