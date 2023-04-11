Developed by Miguel Marcondes

Data modeling using Amazon Redshift. Staging tables are stored in S3, then transformed in Redshift to a data-ready format.
staging tables are creating in Redshift to load data from data files.

An ETL pipeline is built using Python which will transform data from staging tables to dimension and fact tables using "star" schema.

Staging Tables:
staging_events: reads from events logs data files.
staging_songs: reads from songs data files.

Dimension Tables:
users: contains users in the music app.
songs: contains songs in the database.
artists: contains artists in the database.
time: timestamp of records in songplays broken down into specific units.

Fact Table:
songplays: records in the log data associated with song plays.

Config file:
dwh.cfg: contains database and IAM role info.

ETL Pipeline:
transfers data from two local directories (data/song_data, data/log_data) into the tables using SQL and Python.

run create_tables.py to create database and tables.

run etl.py to execute the pipeline to read data from data files and transfer to respective tables.
