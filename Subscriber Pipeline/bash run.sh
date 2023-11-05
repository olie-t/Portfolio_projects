#!/bin/sh

python Pipeline_main.py

prod_path=$"/prod/"
time_stamp=$(date +%Y-%m-%d-%T)
mkdir -p "${prod_path}/${time_stamp}"

mv subscriber_pipeline.csv /dev/dev/ "${prod_path}/${time_stamp}"