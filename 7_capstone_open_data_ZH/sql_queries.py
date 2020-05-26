
set distkeys! in the very end

import configparser

# CONFIG (return dwh configuration in ini format)
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# ARN = config.get("IAM_ROLE", "ARN")
# LOG_DATA = config.get("S3", "LOG_DATA")
# LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
# SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

drop_dimDate = "DROP TABLE IF EXISTS dimDate;"
drop_dimLocation = "DROP TABLE IF EXISTS dimLocation;"
drop_dimTime = "DROP TABLE IF EXISTS dimTime;"
drop_factCounts = "DROP TABLE IF EXISTS factCounts;"
drop_stagingNonMotCounts = "DROP TABLE IF EXISTS stagingNonMotCounts;"
drop_stagingNonMotLocations = "DROP TABLE IF EXISTS stagingNonMotLocations;"


# CREATE TABLES

# Postgres' SERIAL command is not supported in Redshift. The equivalent is IDENTITY(0,1)
# Read more here: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

# Staging Tables

create_stagingNonMotCounts = (
    """
    CREATE TABLE IF NOT EXISTS staging_NonMotCounts(
        fk_zaehler VARCHAR(20)
        fk_standort SMALLINT,
        datum DATETIME,
        velo_in SMALLINT,
        velo_out SMALLINT,
        fuss_in SMALLINT,
        fuss_out SMALLINT,
        ost INT,
        nord INT
    );
    """
)

create_stagingNonMotLocations = (
    """
    CREATE TABLE IF NOT EXISTS staging_NonMotLocations(
        abkuerzung CHAR(8),
        bezeichnung VARCHAR(50),
        bis TIMESTAMP,
        fk_zaehler VARCHAR(20),
        id1 SMALLINT, 
        richtung_in VARCHAR(50),
        richtung_out VARCHAR(50),
        von TIMESTAMP,
        objectid SMALLINT,
        korrekturfaktor FLOAT
    );
    """
)

# Fact Table

create_factCounts = (
    """
    CREATE TABLE IF NOT EXISTS factCounts(
        counts_id INT IDENTITY(0,1) PRIMARY KEY,
        date_pk INT REFERENCES dimDate (date_pk),
        time_pk INT REFERENCES dimTime (time_pk),
        location_pk SMALLINT REFERENCES dimLocation (location_pk),
        type CHAR(1),  ---------------------------------------------------------- TO DO
        count_total SMALLINT,
        count_in SMALLINT,
        count_out SMALLINT
    );
    """
)

# Dim Tables

create_dimLocation = (
    """
    CREATE TABLE IF NOT EXISTS dimLocation(
        location_pk SERIAL PRIMARY KEY,
        location_id SMALLINT NOT NULL
        location_name VARCHAR(50) NOT NULL,
        location_code VARCHAR(8) NOT NULL,
        location_type CHAR(3) NOT NULL,
        latitude DECIMAL NOT NULL,
        longitude DECIMAL NOT NULL,
        active_from DATE NOT NULL,
        active_to DATE NOT NULL,
        still_active BOOLEAN NOT NULL
    );
    """
)

create_dimDate = (
    """
    CREATE TABLE IF NOT EXISTS dimDate(
        date_pk INT PRIMARY KEY,
        date DATE NOT NULL,
        year SMALLINT NOT NULL,
        quarter SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        month_name  VARCHAR(9) NOT NULL,
        week_of_year SMALLINT NOT NULL,
        day_of_year SMALLINT NOT NULL,
        day_of_quarter SMALLINT NOT NULL,
        day_of_month SMALLINT NOT NULL,
        day_of_week SMALLINT NOT NULL,
        day_name VARCHAR(9) NOT NULL,
        is_weekend BOOLEAN NOT NULL,
        is_holiday BOOLEAN NOT NULL,
        first_day_of_week DATE NOT NULL,
        last_day_of_week DATE NOT NULL,
        first_day_of_month DATE NOT NULL,
        last_day_of_month DATE NOT NULL,
        first_day_of_quarter DATE NOT NULL,
        last_day_of_quarter DATE NOT NULL,
        first_day_of_year DATE NOT NULL,
        last_day_of_year DATE NOT NULL,
    );
    """
)

create_dimTime = (
    """
    CREATE TABLE IF NOT EXISTS dimTime(
        time_pk INT PRIMARY KEY,
        time_of_day CHAR(5) NOT NULL,
        hour SMALLINT NOT NULL,
        half_hour CHAR(13) NOT NULL,
        quarter_hour CHAR(13) NOT NULL,
        minute SMALLINT NOT NULL
    );
    """
)


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

# Note: I use DISTINCT statement to handle duplicates

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
                                 ON e.song = s.title
                                   AND e.artist = s.artist_name
                                   AND e.length = s.duration
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

insert_dimDate = ("""
    INSERT INTO dimDate
    SELECT
        TO_CHAR(datum,'yyyymmdd')::INT AS date_pk,
        datum AS date,
        EXTRACT(year FROM datum) AS year,
        EXTRACT(quarter FROM datum) AS quarter,
        EXTRACT(MONTH FROM datum) AS month,
        TO_CHAR(datum, 'TMMonth') AS month_name,  -- localized month name
        EXTRACT(doy FROM datum) AS day_of_year,
        datum - DATE_TRUNC('quarter',datum)::DATE +1 AS day_of_quarter,
        EXTRACT(DAY FROM datum) AS day_of_month,
        EXTRACT(isodow FROM datum) AS day_of_week,
        TO_CHAR(datum,'TMDay') AS day_name,  -- localized day name
       	EXTRACT(week FROM datum) AS week_of_year,
        CASE
            WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        CASE
            WHEN to_char(datum, 'MMDD') IN
                ('0101', '0501', '0801', '1225', '1226') THEN TRUE
            ELSE FALSE
        END AS is_holiday,
        datum +(1 -EXTRACT(isodow FROM datum))::INT AS first_day_of_week,
        datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_day_of_week,
        datum +(1 -EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
        (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
        DATE_TRUNC('quarter',datum)::DATE AS first_day_of_quarter,
        (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
        TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
        TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year
    FROM (SELECT '2010-01-01'::DATE+ SEQUENCE.DAY AS datum
        FROM GENERATE_SERIES (0, 5475) AS SEQUENCE (DAY)
        GROUP BY SEQUENCE.DAY) DQ
    ORDER BY 1;
""")


insert_dimTime = ("""
    INSERT INTO dimTime
    SELECT
        -- Minute of the day (0 - 1439)
        EXTRACT(HOUR FROM MINUTE)*60 + EXTRACT(MINUTE FROM MINUTE) AS time_pk,
        to_char(MINUTE, 'hh24:mi') AS time_of_day,
        EXTRACT(HOUR FROM MINUTE) AS hour,
        to_char(MINUTE - (EXTRACT(MINUTE FROM MINUTE)::INTEGER % 30 || 'minutes')::INTERVAL, 'hh24:mi') ||
        ' – ' ||
        to_char(MINUTE - (EXTRACT(MINUTE FROM MINUTE)::INTEGER % 30 || 'minutes')::INTERVAL + '29 minutes'::INTERVAL, 'hh24:mi')
            AS half_hour,
        to_char(MINUTE - (EXTRACT(MINUTE FROM MINUTE)::INTEGER % 15 || 'minutes')::INTERVAL, 'hh24:mi') ||
        ' – ' ||
        to_char(MINUTE - (EXTRACT(MINUTE FROM MINUTE)::INTEGER % 15 || 'minutes')::INTERVAL + '14 minutes'::INTERVAL, 'hh24:mi')
            AS quarter_hour,
        -- Minute (0 - 59)
        EXTRACT(MINUTE FROM MINUTE) AS minute
    FROM (SELECT '0:00'::TIME + (SEQUENCE.MINUTE || ' minutes')::INTERVAL AS MINUTE
        FROM generate_series(0,1439) AS SEQUENCE(MINUTE)
        GROUP BY SEQUENCE.MINUTE
        ) DQ
    ORDER BY 1;
""")

# QUERY LISTS

create_table_queries = [
    create_dimDate,
    create_dimLocation,
    create_dimTime,
    create_factCounts,
    create_stagingNonMotCounts,
    create_stagingNonMotLocations
]

drop_table_queries = [
    drop_dimDate,
    drop_dimLocation,
    drop_dimTime,
    drop_factCounts,
    drop_stagingNonMotCounts,
    drop_stagingNonMotLocations
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
