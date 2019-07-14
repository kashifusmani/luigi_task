#!/bin/bash
if [ $# -ne 1 ]; then
    echo Only 1 argument is allowed
    exit 1
fi

if ! [ -e "$1" ]; then
    echo Please provide a valid file
    exit 1
fi

pip install -r code/requirements.txt

export PYTHONPATH=`pwd`/code

luigi --module my_script ComputeSimilarity --local-scheduler --input-file-path $1  --output-file-path `pwd`/similarity.csv

