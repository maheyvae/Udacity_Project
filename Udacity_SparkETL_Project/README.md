# Data Lake with Spark
### Introduction

Sparkify, a music streaming start-up, is in their ramp-up phase intrested in getting more insights related to their growing user and song database. Because the database has even grown more than expected Sparkify want to move their data from a datawarehouse to a data lake

As a data engineer our task is to create an ETL pipeline, that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Data available

For this project I'll be working with two datasets, stored on S3

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song.

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

### Database schema

For this project we created a star schema with 1 fact table and 4 dimension tables:

Fact table:

    songplays - records in event data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension tables:

    users - users in the app
    user_id, first_name, last_name, gender, level
    
    songs - songs in music database
    song_id, title, artist_id, year, duration
    
    artists - artists in music database
    artist_id, name, location, lattitude, longitude
    
    time - timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday
    
### Project Steps

1. etl.py
    reads data from S3, processes that data using Spark, and writes them back to S3
3. dl.cfg
    contains your AWS credentials

### Sources used
https://sparkbyexamples.com/spark/spark-select-vs-selectexpr-with-examples/
https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
https://www.geeksforgeeks.org/isoformat-method-of-datetime-class-in-python/
https://sparkbyexamples.com/spark/spark-sql-date-and-time-functions/
https://stackoverflow.com/questions/43406887/spark-dataframe-how-to-add-a-index-column-aka-distributed-data-index
https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html