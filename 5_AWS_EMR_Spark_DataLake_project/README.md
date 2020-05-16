# Project: Data Lake with PySpark (AWS EMR, S3)

## Introduction

Our music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project will extract data from the log files on S3, transform it using Spark and store the data back in dimensional form into a S3 data lake using the parquet format.

After setting up an AWS EMR cluster with Spark installation, the task is building an ETL pipeline that:

- extracts the JSON data from S3
- processed them using Spark, and
- loads them back to S3 as a set of dimensional analytics tables.

The big difference to the projects before is that we completely do away with the need for databases. We fetch the unstructured data from S3, process it on the fly ("schema-on-read") on a temporary EMR cluster with Spark and store the dimensional data back to (cheap) S3. From there it can be accessed for analytics. This method is cost efficient and makes much sense if we want to have flexibility on how our data is analyzed for analytics.

## Input data

Data is collected for song metadata and user activities, using JSON files. The data comes in the same structure as in projects 1 and 4, but now resides in AWS S3. Here are the S3 links for each:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

### Song dataset format

These files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset:

- `song_data/A/B/C/TRABCEI128F424C983.json`
- `song_data/A/A/B/TRAABJL12903CDCF1A.json`

(See README of project 1 for details on the structure of a single entry.)

### Log dataset format

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

`log_data/2018/11/2018-11-12-events.json`
`log_data/2018/11/2018-11-13-events.json`

(See README of project 1 for details on the structure of a single entry.)

## Target Schema

Final fact and dimension tables follow the same star schema as in projects 1 and 4. It is defined as follows:

![ERD](../Song_ERD.png)

## Schema

Final fact and dimension tables should be following a star schema with an analytics focus. It is defined as follows:

### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension Tables

**users** - users in the app
*user_id, first_name, last_name, gender, level*

**songs** - songs in music database
*song_id, title, artist_id, year, duration*

**artists** - artists in music database
*artist_id, name, location, lattitude, longitude*

**time** - timestamps of records in songplays broken down into specific units
*start_time, hour, day, week, month, year, weekday*

## Build

This project ran on an **AWS EMR Cluster 6.0** (Hadoop 3.2, Spark 2.4, Python 3).

## Config

Copy `dwh.cfg.example` to `dwh.cfg`, and fill the settings for S3 access.

## Run

To execute the pipeline, run from EMR:

``` sh
/usr/bin/spark-submit --master yarn etl.py
```
