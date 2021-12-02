#!/bin/bash
# Help
Help(){
    echo "Syntax: batch workload run_count destination_file  "
}

if (( $# != 3 )); then
    Help
    exit
fi

workload=$1
N=$2
destfile=$3

run_cmd="~/go-ycsb/bin/go-ycsb run mpaxos -P $workload"
# call load once
load_cmd="~/go-ycsb/bin/go-ycsb load mpaxos -P $workload"
echo "Load : "
eval $load_cmd | awk '/^Run finished, takes /{print $NF}' | tee -a  $destfile
echo "Run : "
for ((i = 0; i < N; i++)); do  
    eval $run_cmd | awk '/^Run finished, takes /{print $NF}' | tee -a $destfile
done
