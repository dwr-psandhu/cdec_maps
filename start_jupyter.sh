#!/bin/bash -l
conda activate jupyter 
jupyter notebook 2>&1 > /dev/null &

