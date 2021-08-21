# Modeling Sparkify Database with Postgres

## Project Summary
This project models & transforms the song & user data for the Sparkify company using Redshift so that it can be analyzed by their analytical team. It uses a star-schema with Dimension & Fact tables to make it easy to write analytical SQL queries on the database. It uses a Python script to ETL the data from the song & log files to a couple of Staging tables & then, eventually to the final tables in the Redshift database.

## How to run the project
Please run the following Python scripts :
1. create_tables.py --> It creates the staging & the final tables for the Sparkify database
1. etl.py --> It loads the data into the database from the song & log files

## Explanation of files
You'll find the following files in the repo :
1. create_tables.py drops and creates the tables. You run this file to reset your tables before each time you run your ETL scripts.
1. etl.py reads and processes files from song_data and log_data and loads them into the tables.
1. sql_queries.py contains all the sql queries to create, drop tables & insert data, and is imported into the files above.
1. README.md provides discussion on your project.
