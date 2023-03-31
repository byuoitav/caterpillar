#!/bin/bash

mapfile -t a < $1
for i in "${a[@]}"; do
    echo "Caterpillar working on:  $i"
    go run server.go $i
    sleep 2
done
