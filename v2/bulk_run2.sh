#!/bin/bash

#mapfile -t a < $1
#while IFS= read -r line
a='out.csv'
while read -r line
do
    echo "Caterpillar working on: $line"
    go run server.go $line
    sleep 2
done < "$a"
