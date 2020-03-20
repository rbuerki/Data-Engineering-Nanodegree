# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                                          (songplay_id SERIAL,
                                           start_time TIMESTAMP NOT NULL,
                                           user_id INT NOT NULL,
                                           level VARCHAR NOT NULL,
                                           song_id VARCHAR(18),
                                           artist_id VARCHAR(18),
                                           session_id INT NOT NULL,
                                           location VARCHAR NOT NULL,
                                           user_agent VARCHAR NOT NULL
                                           PRIMARY KEY (songplay_id)
                                           );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                                   (user_id INT,
                                    first_name VARCHAR NOT NULL,
                                    last_name VARCHAR NOT NULL,
                                    gender CHAR(1) NOT NULL,
                                    level VARCHAR NOT NULL,
                                    PRIMARY KEY (user_id)
                                    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                                   (song_id VARCHAR(18),
                                    title VARCHAR NOT NULL,
                                    artist_id VARCHAR(18) NOT NULL,
                                    year INT NOT NULL,
                                    duration DECIMAL NOT NULL,
                                    PRIMARY KEY (song_id)
                                    );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                                   (artist_id VARCHAR(18),
                                    name VARCHAR NOT NULL,
                                    location VARCHAR,
                                    latitude DECIMAL,
                                    longitude DECIMAL,
                                    PRIMARY KEY (artist_id)
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

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays
                                (start_time,
                                 user_id,
                                 level,
                                 song_id,
                                 artist_id,
                                 session_id,
                                 location,
                                 user_agent)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""INSERT INTO users
                            (user_id,
                             first_name,
                             last_name,
                             gender,
                             level)
                        VALUES (%s, %s, %s, %s, %s);
""")

song_table_insert = ("""INSERT INTO songs
                            (song_id,
                             title,
                             artist_id,
                             year,
                             duration)
                        VALUES (%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""INSERT INTO artists
                            (artist_id,
                             name,
                             location,
                             latitude,
                             longitude)
                        VALUES (%s, %s, %s, %s, %s);
""")

time_table_insert = ("""INSERT INTO time
                            (start_time,
                             hour,
                             day,
                             week,
                             month,
                             year,
                             weekday)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""SELECT s.song_id,
                         s.artist_id
                  FROM   songs AS s
                  JOIN   artists AS at
                    ON   at.artist_id = s.artist_id
                  WHERE  s.title = %s
                   AND   at.name = %s
                   AND   s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        ]
drop_table_queries = [songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop,
                      ]
