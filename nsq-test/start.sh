#!/bin/sh
killall -9 nsqlookupd nsqd nsqadmin nsq_tail tail
nsqlookupd  >nsqlookupd.log 2>&1 &
nsqadmin --lookupd-http-address localhost:4161 >nsqadmin.log 2>&1 &
nsqd -lookupd-tcp-address localhost:4160 >nsqd.log 2>&1 &
sleep 1
nsq_tail --topic events --lookupd-http-address localhost:4161  >>topic_events &
nsq_tail --topic receivers --lookupd-http-address localhost:4161 >>topic_receivers &
tail -f topic_* &
