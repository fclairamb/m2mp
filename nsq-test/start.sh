#!/bin/sh
killall -9 nsqlookupd nsqd nsqadmin
nsqlookupd & 
nsqadmin --lookupd-http-address localhost:4161 &
nsqd -lookupd-tcp-address localhost:4160 &

