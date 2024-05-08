Developed by Miguel Marcondes

Data modeling using Amazon Redshift. Staging tables are stored in S3, then transformed in Redshift to a data-ready format.

&nbsp;

An ETL pipeline is built using Python which will transform data from staging tables to dimension and fact tables using star schema.

&nbsp;

Staging Tables:
staging_events: reads from events logs data files.
staging_songs: reads from songs data files.

&nbsp;

Dimension Tables:
users: contains users in the music app. &nbsp;
songs: contains songs in the database. &nbsp;
artists: contains artists in the database. &nbsp;
time: timestamp of records in songplays broken down into specific units. &nbsp;

&nbsp;

Fact Table:
songplays: records in the log data associated with song plays.

&nbsp;

Config file:
dwh.cfg: contains database and IAM role info.

&nbsp;

ETL Pipeline:
transfers data from two local directories (data/song_data, data/log_data) into the tables using SQL and Python.

&nbsp;

run create_tables.py to create database and tables.

run etl.py to execute the pipeline to read data from data files and transfer to respective tables.
