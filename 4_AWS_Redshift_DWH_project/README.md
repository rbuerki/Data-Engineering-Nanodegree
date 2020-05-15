# Project: Cloud Data Warehouse (AWS Redshift)

## Introduction

Our music streaming startup, Sparkify, has grown their user base and song database and move their processes and data onto the cloud. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

After setting up an AWS Redshift cluster, the task is building an ETL pipeline that:

- extracts the JSON data from S3
- stages them in Redshift, and
- transforms them into a star schema with a set of dimensional tables.

The big difference to the first project (with a local postgres database) is, that the, much bigger, data is now distributed over different nodes. So you have to think about how to optimize the partitioning and table design.

(Two demo notebooks are included to give some advice / background on this topic. They are not directly related to the project. See also resources in the end of this Readme.)

## Input data

Data is collected for song metadata and user activities, using JSON files. The data comes basically in the same format as in project 1, but now resides in AWS S3. Here are the S3 links for each:

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

Final fact and dimension tables follow the same star schema as in the first project. It is defined as follows:

![ERD](../Song_ERD.png)

## Build

This project runs with **Python 3.6** or higher on an **AWS Redshift Cluster**.

The project python dependencies can be installed with help of the envirment.yml (conda) or requirements.txt (pip) stored in project 1.

## Config

Copy `dwh.cfg.example` to `dwh.cfg`, and fill the settings for:

- Cluster Creation on `[DWH]` and `[AWS]`
- Cluster Connection on `[CLUSTER]`
- Your ARN to provide S3 access from Redshift on `[IAM_ROLE]`

## Run

Script to create the database tables or to reset the database:

``` sh
./create_tables.py
```

ETL pipeline to populate data into the tables:

``` sh
./etl.py
```

(For creating and deleting the cluster before beginning and after finishing the project the `1-Create_Redshift_Cluster.ipynb` was used.)

## Additional Resources

Some good resources:
https://aws.amazon.com/blogs/big-data/top-8-best-practices-for-high-performance-etl-processing-using-amazon-redshift/
https://aws.amazon.com/blogs/big-data/how-i-built-a-data-warehouse-using-amazon-redshift-and-aws-services-in-record-time/
https://panoply.io/data-warehouse-guide/redshift-etl/
https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html
http://www.sqlhaven.com/amazon-redshift-what-you-need-to-think-before-defining-primary-key/
https://d1.awsstatic.com/whitepapers/enterprise-data-warehousing-on-aws.pdf