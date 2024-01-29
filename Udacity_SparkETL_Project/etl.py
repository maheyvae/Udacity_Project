import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Extract data from song_data json-file and store respective data in songs 
    and artists tables which will be uploaded again in S3"""
    
    # get filepath to song data file
    song_data= input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr("song_id", "title", "artist_id", "duration").orderBy("song_id")
    
    songs_table.createOrReplaceTempView("songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet(output_data + "songs").partitionBy('year', 'artist_id')

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude").orderBy("artist_id")
    
    artist_table.createOrReplaceTempView("artists")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data): 
    """Extract data from log_data json-file and store respective data in time,
    users and songplays table (to be joined by song_df) which will be uploaded
    again in S3"""
        
    # get filepath to log data file
    os.path.join(input_data, 'log_data/*/*/*.json')
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page = 'NextSong'")

    # extract columns for users table    
    user_table =  df.selectExpr("userID as user_id", "firstName as first_name", "lastName as lastname", "gender", "level").orderBy("user_id")
    
    user_table.createOrReplaceTempView("users")
    
    # write users table to parquet files
    user_table.write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0))
    
    df = df.withColumn('start_time', get_timestamp('ts'))

    
    # extract columns to create time table
    time_table = df.select('start_time')
    time_table = time_table \
    .withColumn('hour', F.hour('start_time')) \
    .withColumn('day', F.dayofyear('start_time')) \
    .withColumn('week', F.weekofyear('start_time')) \
    .withColumn('month', F.month('start_time')) \
    .withColumn('year', F.year('start_time')) \
    .withColumn('weekday', F.dayofweek('start_time'))
    
    time_table.createOrReplaceTempView('calender')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + "calender").partitionBy('year', 'month')

    # read in song data to use for songplays table
    song_data= input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView('songs')

    # extract columns from joined song and log datasets to create songplays table 
    events_table = df.selectExpr("start_time", "userId as user_id", "level", "sessionId as session_id", "location", "userAgent as user_agent", "artist", "length", "song")
    events_table = events_table.withColumn('songplay_id', F.monotonically_increasing_id())
    events_table.createOrReplaceTempView('events')
    
   
    songplays_table = spark.sql("""
                       SELECT
                       e.songplay_id,
                       e.start_time,
                       e.user_id,
                       e.level,
                       s.song_id,
                       s.artist_id,
                       e.session_id,
                       e.location,
                       e.user_agent
                       FROM events e
                       JOIN songs s 
                       ON s.duration = e.length AND s.title = e.song AND s.artist_name = e.artist
                       """)
    
    songplays_table.createOrReplaceTempView('songplays')
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(output_data + "songplays").partitionBy('year', 'month')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-matthias/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
