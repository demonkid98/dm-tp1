#!/bin/sh
while true
do
	while read p; do
  		echo $p
  		sleep $2
	done <$1
done