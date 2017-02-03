#!/bin/bash

PROJ_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
DEST_DIR=/tmp/tutorial/storm

# Move input files to accessible location
mkdir -p $DEST_DIR
cp $PROJ_DIR/src/main/resources/truck-input.txt $DEST_DIR
cp $PROJ_DIR/src/main/resources/traffic-input.txt $DEST_DIR
