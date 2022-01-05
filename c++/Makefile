CXX=clang++
CXXFLAGS=-Wall -Werror -Wextra -pedantic -std=c++17

test: test.cc memstore.h memstore.cc kvstore.h paxos.cc paxos.h consensus.h command.h
	$(CXX) $(CXXFLAGS) test.cc paxos.cc memstore.cc -o test
