#!/bin/sh

echo "" | awk 'END{print "topic\tlag\toldest\t\tcurrent\t\tnewest" }'
if [ $# -eq 0 ]
then
  redis-cli -p 9999 offset |  awk '{if(NR%2 == 1) { a = $0} else{ printf("%d_%s\n", a,$0)}}' | sort -n -k 1 | awk -F[_] '{printf("%s\t%d\t%s\t%s\t%s\n", $1, $4-$3, $2, $3, $4)}'
else
  redis-cli -h $1 -p 9999 offset |  awk '{if(NR%2 == 1) { a = $0} else{ printf("%d_%s\n", a,$0)}}' | sort -n -k 1 | awk -F[_] '{printf("%s\t%d\t%s\t%s\t%s\n", $1, $4-$3, $2, $3, $4)}'
fi
