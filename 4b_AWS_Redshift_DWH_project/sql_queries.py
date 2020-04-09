import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# Postgres' SERIAL command is not supported in Redshift. The equivalent is IDENTITY(0,1)
# Read more here: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events
                                                    artist VARCHAR NOT NULL,
                                                    auth VARCHAR NOT NULL,
                                                    firstName VARCHAR NOT NULL,
                                                    gender CHAR(1) NOT NULL,
                                                    itemInSession INT NOT NULL,
                                                    lastName VARCHAR NOT NULL,
                                                    length DECIMAL NOT NULL,
                                                    level VARCHAR NOT NULL,
                                                    location VARCHAR NOT NULL,
                                                    method VARCHAR NOT NULL,
                                                    page VARCHAR NOT NULL,
                                                    registration VARCHAR(15) NOT NULL,
                                                    sessionId INT NOT NULL,
                                                    song VARCHAR NOT NULL,
                                                    status INT NOT NULL,
                                                    ts TIMESTAMP NOT NULL,
                                                    userAgent VARCHAR,
                                                    userId INT NOT NULL
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                                (num_songs IDENTITY(0,1),
                                                 artist_id  VARCHAR(18) NOT NULL,
                                                 artist_latitude DECIMAL,
                                                 artist_longitude DECIMAL,
                                                 artist_location VARCHAR,
                                                 artist_name VARCHAR NOT NULL,
                                                 song_id VARCHAR(18) NOT NULL,
                                                 title VARCHAR NOT NULL,
                                                 duration DECIMAL NOT NULL,
                                                 year INT NOT NULL
                                                 );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                                          (songplay_id SERIAL PRIMARY KEY,
                                           start_time TIMESTAMP NOT NULL,
                                           user_id INT NOT NULL,
                                           level VARCHAR NOT NULL,
                                           song_id VARCHAR(18),
                                           artist_id VARCHAR(18),
                                           session_id INT NOT NULL,
                                           location VARCHAR NOT NULL,
                                           user_agent VARCHAR NOT NULL
                                           );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                                   (user_id INT PRIMARY KEY,
                                    first_name VARCHAR NOT NULL,
                                    last_name VARCHAR NOT NULL,
                                    gender CHAR(1) NOT NULL,
                                    level VARCHAR NOT NULL
                                    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                                   (song_id VARCHAR(18) PRIMARY KEY,
                                    title VARCHAR NOT NULL,
                                    artist_id VARCHAR(18) NOT NULL,
                                    year INT NOT NULL,
                                    duration DECIMAL NOT NULL
                                    );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                                   (artist_id VARCHAR(18) PRIMARY KEY,
                                    name VARCHAR NOT NULL,
                                    location VARCHAR,
                                    latitude DECIMAL,
                                    longitude DECIMAL
                                    );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                                   (start_time TIMESTAMP NOT NULL,
                                    hour INT NOT NULL,
                                    day INT NOT NULL,
                                    week INT NOT NULL,
                                    month INT NOT NULL,
                                    year INT NOT NULL,
                                    weekday INT NOT NULL
                                    );
""")

# STAGING TABLES

staging_events_copy = (f"""
            COPY staging_events
            FROM {LOG_DATA}    --------------------- what is this for: LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
            credentials 'aws_iam_role={ARN}'
            gzip delimiter ';' region 'us-west-2';  ---------------------------- CHECK THE REGION!
""")

staging_songs_copy = (f"""
            COPY staging_songs
            FROM {SONG_DATA}
            credentials 'aws_iam_role={ARN}'
            gzip delimiter ';' region 'us-west-2';
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create
                        ]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop
                      ]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy
                      ]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert
                        ]
