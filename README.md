# AMAZON DATA WAREHOUSE


## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. The data resides in an S3 bucket, in a directory of JSON logs on the app's user activity, and a directory with JSON metadata on their app's songs.

The task is to build an ETL pipeline that extracts data from S3, stage them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs users are listening.

## Project Description

The project creates a data warehouse with AWS to build an ETL pipeline for a database hosted on Redshift. The data is loaded from an S3 bucket to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables. The project's resources are EC2, IAM, S3, Redshift, and python using programmatic access.

## Schema

#### Fact Table

1. **songplays** - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

2. **users** - users in the app
    - user_id, first_name, last_name, gender, level

3. **songs** - songs in music database
    - song_id, title, artist_id, year, duration

4. **artists** - artists in music database
    - artist_id, name, location, lattitude, longitude

5. **time** - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## Files

- **create_cluster.ipynb** - Contains logic to execute project.
- **create_tables.py** - Script that creates and drops all tables in db.
- **dwh.cfg** - Configuration file.
- **etl.py** - Script that executes etl logic.
- **sql_queries.py** - All SQL table logic.

## Project Execution

- Create AWS user with programmatic access

- Populate KEY & SECRET to **dwh.cfg** configuration file

- Install python 3.8.3

- Install libraries 
    - psycopg2
    - boto3

- Open **create_cluster.ipynb**    
    - Execute SECTION 1 within **create_cluster.ipynb**
    - Execute SECTION 2 & extract ARN & HOST save values to **dwh.cfg**
    - Execute SECTION 3 Will run **create_tables.py** & **etl.py** scripts
    - Execute SECTION 4 Will run test queries for each table
    - Execute SECTION 5 Will delete the IAM & Redshift Cluster