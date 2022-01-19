cmake_minimum_required(VERSION 3.6)

project(replicant CXX)

add_compile_options(-Wall -Wextra -pedantic -Werror -std=c++17)

add_executable(replicant replicant.cc paxos.cc memstore.cc main.cc)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)
target_link_libraries(replicant Threads::Threads)
