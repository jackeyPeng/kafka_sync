#!/bin/sh

echo "" | awk 'END{print "topic\toldest_current_newest \t offset" }'
redis-cli -p 9999 info offset |  awk '{if(NR%2 == 1) { a = $0} else{ print a,$0}}' | sort -n -k 1 | awk -F[_] '{print $0, $3-$2}'
