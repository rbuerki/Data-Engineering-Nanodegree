# Project: Data Modeling with Postgres

The goal of this project ist to develop an ETL pipeline for analyzing data for a music streaming app. This project creates a **star-schema Postgres database** from a directory of JSON files, containing logs on user activity and metadata on the songs in the app.

## Input data

Data is collected for song metadata and user activities, using JSON files. The data is stored in two directories, `data/log_data` and `data/song_data`.

### Song dataset format

```json
{
  "num_songs": 1,
  "artist_id": "ARGSJW91187B9B1D6B",
  "artist_latitude": 35.21962,
  "artist_longitude": -80.01955,
  "artist_location": "North Carolina",
  "artist_name": "JennyAnyKind",
  "song_id": "SOQHXMF12AB0182363",
  "title": "Young Boy Blues",
  "duration": 218.77506,
  "year": 0
}
```

### Log dataset format

```json
{
  "artist": "Survivor",
  "auth": "Logged In",
  "firstName": "Jayden",
  "gender": "M",
  "itemInSession": 0,
  "lastName": "Fox",
  "length": 245.36771,
  "level": "free",
  "location": "New Orleans-Metairie, LA",
  "method": "PUT",
  "page": "NextSong",
  "registration": 1541033612796,
  "sessionId": 100,
  "song": "Eye Of The Tiger",
  "status": 200,
  "ts": 1541110994796,
  "userAgent": "\"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\"",
  "userId": "101"
}
```

(Note: data is not included in this repository.)

## Schema

Fact and dimension tables following a star schema with an analytics focus.

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

This project runs with **Python 3.6** or higher and **PosgreSQL Database**.

The project python dependencies can be installed with help of the `envirment.yml` (conda) or `requirements.txt` (pip).

## Run

To create the database tables or to reset the database, run:

``` sh
./create_tables.py
```

To populate data into  the tables run the ETL pipeline:

``` sh
./etl.py
```

(One way to verify the data is using the provided `test.ipynb` jupyter notebook in the development folder.)

``` sh
./z_dev_notebooks/jupyter notebook
```
