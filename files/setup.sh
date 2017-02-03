#!/bin/bash

PROJ_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
DEST_DIR=/tmp/storm-tutorial

# Move input files to accessible locations
mkdir -p -m 777 $DEST_DIR
cp $PROJ_DIR/src/main/resources/truck-input.txt $DEST_DIR
cp $PROJ_DIR/src/main/resources/traffic-input.txt $DEST_DIR

echo "A file now exists at $DEST_DIR/truck-input.txt"
echo "A file now exists at $DEST_DIR/traffic-input.txt"
