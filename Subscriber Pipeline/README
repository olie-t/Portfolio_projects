Pipeline Project README
===

The Pipeline_main.py file is the primary file used to run the pipeline.

At the bottom of that file are the variables to set for the input DB, output DB and logging destinations.

Once those are set then the destinations for the output files (paths) must be set in bash run.sh. Then the file can be
used to run the pipeline.

The run file will execute the script, then take the output csv from the dev environment and move it to a folder in the
prod environment for anaylsis to be run.

For readability and ease of use the majority of the actual logic is contained in the subscriber_pipeline_functions.py
file.

Descriptions of variables -
__

Input DB - source of data to be transformed and cleaned for analysis.

Output DB - secondary DB used to easily compare what data is new and what is old.

subscriber_pipeline_log - logs all activity during the run of the pipeline. Quite verbose.

subscriber_change_log - logs changes to the output DB. Useful for seeing how much new data was found.



