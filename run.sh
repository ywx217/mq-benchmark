#!/bin/bash

IP=1.1.1.1
BROKER_IP=2.2.2.2

function usage(){
	echo "$0 kiil"
	echo "$0 run process_num base_port"
	echo "$0 runrr process_num base_port"
}

if [ $# -lt 1 ]; then
	usage
	exit 1
fi

if [ $1 = 'kill' ]; then
	ps aux | grep bench | grep -v grep | awk '{print $2}' | xargs kill
fi

if [ $1 = 'run' ]; then
	if [ $# -lt 3 ]; then
		usage
		exit 2
	fi
	proc_num=$2
	base_port=$3
	for port in $(seq $base_port 2 $((base_port + (proc_num-1)*2))); do
		python zeromq_bench.py --host $BROKER_IP --port 19600 --bin --ip ${IP} --recv-port $port --send-port $((port+1)) > "bench_${port}.log"
	done
elif [ $1 = 'runrr' ]; then
	proc_num=$2
	base_port=$3
	for port in $(seq $base_port 1 $((base_port + proc_num - 1))); do
		python zeromq_rr_bench.py --host $BROKER_IP --port 19600 --bin --ip ${IP} --my-port $port > "bench_${port}.log"
	done
else
	usage
	exit 3
fi
