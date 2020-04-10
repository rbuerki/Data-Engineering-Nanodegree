import configparser

# CONFIG (return dwh configuration in ini format)
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


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

# Staging Tables

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events
                                                   (artist VARCHAR,
                                                    auth VARCHAR,
                                                    firstName VARCHAR,
                                                    gender CHAR(1),
                                                    itemInSession INT,
                                                    lastName VARCHAR,
                                                    length DECIMAL,
                                                    level VARCHAR,
                                                    location VARCHAR,
                                                    method VARCHAR,
                                                    page VARCHAR,
                                                    registration VARCHAR(15),
                                                    sessionId INT,
                                                    song VARCHAR,
                                                    status INT,
                                                    ts TIMESTAMP,
                                                    userAgent VARCHAR DISTKEY,  -- not sure, same as for songplays
                                                    userId INT
                                                    );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                                (artist_id  VARCHAR(18),
                                                 artist_latitude DECIMAL,
                                                 artist_longitude DECIMAL,
                                                 artist_location VARCHAR,
                                                 artist_name VARCHAR,
                                                 song_id VARCHAR(18) DISTKEY,
                                                 title VARCHAR,
                                                 duration DECIMAL,
                                                 year INT
                                                 );
""")

# Fact Table

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                                          (songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                                           start_time TIMESTAMP,
                                           user_id INT NOT NULL DISTKEY,  -- good starting point for queries
                                           level VARCHAR,
                                           song_id VARCHAR(18),
                                           artist_id VARCHAR(18),
                                           session_id INT NOT NULL,
                                           location VARCHAR,
                                           user_agent VARCHAR
                                           );
""")

# Dim Tables

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                                   (user_id INT PRIMARY KEY,
                                    first_name VARCHAR,
                                    last_name VARCHAR,
                                    gender CHAR(1),
                                    level VARCHAR
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
                                   (start_time TIMESTAMP NOT NULL PRIMARY KEY,
                                    hour INT NOT NULL,
                                    day INT NOT NULL,
                                    week INT NOT NULL,
                                    month INT NOT NULL,
                                    year INT NOT NULL,
                                    weekday INT NOT NULL
                                    );
""")

# COPY INTO STAGING TABLES

# COPY loads data into a table from data files or from an Amazon DynamoDB table.
# Read more here: https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
# and here: https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html#copy-json-jsonpaths

staging_events_copy = (f"""
            COPY staging_events
            FROM {LOG_DATA}
            CREDENTIALS 'aws_iam_role={ARN}'
            FORMAT AS JSON {LOG_JSONPATH}
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            REGION 'us-west-2';
""")

staging_songs_copy = (f"""
            COPY staging_songs
            FROM {SONG_DATA}
            CREDENTIALS 'aws_iam_role={ARN}'
            JSON 'auto'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            REGION 'us-west-2';
""")

# INSERT INTO FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays
                                (start_time,
                                 user_id,
                                 level,
                                 song_id,
                                 artist_id,
                                 session_id,
                                 location,
                                 user_agent)
                            SELECT
                                 DISTINCT e.ts,
                                 e.userId,
                                 e.level,
                                 s.song_id,
                                 s.artist_id,
                                 e.sessionId,
                                 e.location,
                                 e.userAgent
                            FROM staging_events AS e
                            JOIN staging_songs AS s
                                ON e.artist = s.artist_name
                            WHERE e.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users
                            (user_id,
                             first_name,
                             last_name,
                             gender,
                             level)
                        SELECT
                             DISTINCT e.userId,
                             e.firstName,
                             e.lastName,
                             e.gender,
                             e.level
                        FROM staging_events AS e
                        WHERE e.page = 'NextSong'
                        AND e.userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs
                            (song_id,
                             title,
                             artist_id,
                             year,
                             duration)
                        SELECT
                             DISTINCT s.song_id,
                             s.title,
                             s.artist_id,
                             s.year,
                             s.duration
                        FROM staging_songs AS s
                        WHERE s.song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists
                            (artist_id,
                             name,
                             location,
                             latitude,
                             longitude)
                          SELECT
                             DISTINCT s.artist_id,
                             s.artist_name,
                             s.artist_location,
                             s.artist_latitude,
                             s.artist_longitude
                          FROM staging_songs AS s
                          WHERE s.artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time
                            (start_time,
                             hour,
                             day,
                             week,
                             month,
                             year,
                             weekday)
                        SELECT
                             DISTINCT e.ts,
                             EXTRACT(hour FROM e.ts),
                             EXTRACT(day FROM e.ts),
                             EXTRACT(week FROM e.ts),
                             EXTRACT(month FROM e.ts),
                             EXTRACT(year FROM e.ts),
                             EXTRACT(weekday FROM e.ts)
                        FROM staging_events AS e
                        WHERE page = 'NextSong'
                        AND ts IS NOT NULL
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
