#!/bin/bash
start_time=$1
end_time=$2
input=$3
halog_out_fqn=$4
echo "status,srv" > ${halog_out_fqn}
echo "the first one"
/usr/sbin/halog -H -time ${start_time}:${end_time} < ${input}  | grep "cadc-ws/" | awk '{print $11" "$20}' | awk -F\/ '{print $1","$2"/"$3}' | awk -F\? '{print $1}' >> ${halog_out_fqn} 2> /dev/null
echo "the second one"
/usr/sbin/halog -H -time ${start_time}:${end_time} < ${input}  | grep "cadc-app/" | awk '{print $11" "$20}' | awk -F\/ '{print $1","$3"/"$4}' | awk -F\? '{print $1}' >> ${halog_out_fqn} 2> /dev/null
