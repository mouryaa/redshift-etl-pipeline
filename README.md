# Creating a data warehouse from S3 data

## Project description

Create

## How to run

1. Install the libraries from the requirements file.
2. Run the *create_cluster* script to set up the needed infrastructure for this project.
3. Run the *create_tables* script to set up the database staging and analytical tables
4. Finally, run the *etl* script to extract data from the files in S3, stage it in redshift, and finally store it in the dimensional tables.

## Project structure

This project includes five script files:

- analytics.py runs a few queries on the created star schema to validate that the project has been completed successfully.
- create_cluster.py is where the AWS components for this project are created programmatically
- create_table.py is where fact and dimension tables for the star schema in Redshift are created.
- etl.py is where data gets loaded from S3 into staging tables on Redshift and then processed into the analytics tables on Redshift.
- sql_queries.py where SQL statements are defined, which are then used by etl.py, create_table.py and analytics.py.
- README.md is current file.
- requirements.txt with python dependencies needed to run the project


#### Staging Tables
- staging_events
- staging_songs

####  Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong -
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
- users - users in the app -
*user_id, first_name, last_name, gender, level*
- songs - songs in music database -
*song_id, title, artist_id, year, duration*
- artists - artists in music database -
*artist_id, name, location, lattitude, longitude*
- time - timestamps of records in songplays broken down into specific units -
*start_time, hour, day, week, month, year, weekday*
