#!/bin/bash

DEST_DIR=/tmp/tutorial/storm

# Move input files to accessible location
mkdir -p $DEST_DIR
cp ../src/main/resources/truck-input.txt $DEST_DIR
cp ../src/main/resources/traffic-input.txt $DEST_DIR
