#!/bin/bash 
echo $1 
echo $2 
echo $3 
echo $4 
unzip SNEE.jar -d extracted
 rm -f SNEE.jar
cd extracted 
jre1.6.0_27/bin/java uk/ac/manchester/snee/client/CondorReliableChannelClient $1 $2 $3 $4 $5
for d in *; do if test -d "$d"; then tar czf "$d".tgz "$d"; fi; done
mv output.tgz .. 
 exit 0