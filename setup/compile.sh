#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

CACHE_SIZE=(0.10)
SYSTEM_NAMES=("C")
POLICY_TYPES=("access_rate")
ACCESS_RATE=(100)

execute_cmd_with_timeout() {
    local num_clients=$1
    local num_threads=$2
    local num_clients_per_thread=$3
    local step=$4
    local distribution=$5
    local system_name=$6
    local policy=$7
    local cache_size_for_this_run=$8
    local access_rate=$9

    echo "Executing for cache size: $cache_size_for_this_run and policy: $policy and system: $system_name and access rate: $access_rate and workload: $WORKLOAD"
    CMD="$PROGRAM_PATH $IDENTITY_FILE $USERNAME $GIT_SSH_KEY_PATH --step $step $NUM_SERVERS \
    --num_threads $num_threads --policy $policy --system_type $system_name --distribution $distribution --num_clients $num_clients \
    --num_clients_per_thread $num_clients_per_thread --git_branch $GIT_BRANCH --cache_size $cache_size_for_this_run --access_rate $access_rate --workload $WORKLOAD"
    echo "Executing: $CMD"
    eval $CMD

}

execute_cmd() {
    local num_clients=$1
    local num_threads=$2
    local num_clients_per_thread=$3
    local step=$4
    local distribution=$5
    local system_name=$6
    local policy=$7
    local access_rate=$8

    #itr throu
    for cache_size_for_this_run in "${CACHE_SIZE[@]}"; do
        execute_cmd_with_timeout $num_clients $num_threads $num_clients_per_thread $step $distribution $system_name $policy $cache_size_for_this_run $access_rate
    done
}

# Function to iterate through ranges and execute commands
iterate_and_execute_setup() {
    local num_clients_start=$1
    local num_clients_end=$2
    local num_threads_start=$3
    local num_threads_end=$4
    local num_clients_per_thread_start=$5
    local num_clients_per_thread_end=$6
    local distribution=$7
    local system_name=$8
    local policy=$9
    local access_rate=${10}
    
    for ((num_clients=num_clients_start; num_clients<=num_clients_end; num_clients++)); do
        for ((num_threads=num_threads_start; num_threads<=num_threads_end; num_threads++)); do
            for ((num_clients_per_thread=num_clients_per_thread_start; num_clients_per_thread<=num_clients_per_thread_end; num_clients_per_thread++)); do
                execute_cmd $num_clients $num_threads $num_clients_per_thread 11 $distribution $system_name $policy $access_rate
            done
        done
    done
}

iterate_and_execute_compile() {
    local num_clients_start=$1
    local num_clients_end=$2
    local num_threads_start=$3
    local num_threads_end=$4
    local num_clients_per_thread_start=$5
    local num_clients_per_thread_end=$6
    local distribution=$7
    local system_name=$8
    local policy=$9
    local access_rate=${10}
    
    for ((num_clients=num_clients_start; num_clients<=num_clients_end; num_clients++)); do
        for ((num_threads=num_threads_start; num_threads<=num_threads_end; num_threads++)); do
            for ((num_clients_per_thread=num_clients_per_thread_start; num_clients_per_thread<=num_clients_per_thread_end; num_clients_per_thread++)); do
                execute_cmd $num_clients $num_threads $num_clients_per_thread 11 $distribution $system_name $policy $access_rate
            done
        done
    done
}

iterate_and_execute() {
    local num_clients_start=$1
    local num_clients_end=$2
    local num_threads_start=$3
    local num_threads_end=$4
    local num_clients_per_thread_start=$5
    local num_clients_per_thread_end=$6
    local distribution=$7
    local system_name=$8
    local policy=$9
    local access_rate=${10}
    
    for ((num_clients=num_clients_start; num_clients<=num_clients_end; num_clients++)); do
        for ((num_threads=num_threads_start; num_threads<=num_threads_end; num_threads++)); do
            for ((num_clients_per_thread=num_clients_per_thread_start; num_clients_per_thread<=num_clients_per_thread_end; num_clients_per_thread++)); do
                execute_cmd $num_clients $num_threads $num_clients_per_thread 11 $distribution $system_name $policy $access_rate
            done
        done
    done
}


WORKLOAD="YCSB"
iterate_and_execute_setup 3 3 8 8 2 2 "hotspot" "C" "access_rate_dynamic" 50