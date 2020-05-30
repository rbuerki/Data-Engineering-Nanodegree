
import configparser

# CONFIG (return dwh configuration in ini format)
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
LOC_DATA = config.get("S3", "LOC_DATA")
COUNT_DATA_NON_MOT = config.get("S3", "COUNT_DATA_NON_MOT")
COUNT_DATA_MOT = config.get("S3", "COUNT_DATA_MOT")


# DROP TABLES

drop_dimDate = "DROP TABLE IF EXISTS dimDate;"
drop_dimLocation = "DROP TABLE IF EXISTS dimLocation;"
drop_dimTime = "DROP TABLE IF EXISTS dimTime;"
drop_factCount = "DROP TABLE IF EXISTS factCount;"
drop_stagingNonMotCount = "DROP TABLE IF EXISTS stagingNonMotCount;"
drop_stagingNonMotLocation = "DROP TABLE IF EXISTS stagingNonMotLocation;"


# CREATE TABLES

# Postgres' SERIAL command is not supported in Redshift. The equivalent is IDENTITY(0,1)
# Read more here: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

# Staging Tables

create_stagingNonMotCount = (
    """
    CREATE TABLE IF NOT EXISTS staging_NonMotCount(
        fk_zaehler VARCHAR(20),
        fk_standort SMALLINT,
        datum TIMESTAMP SORTKEY DISTKEY,
        velo_in SMALLINT,
        velo_out SMALLINT,
        fuss_in SMALLINT,
        fuss_out SMALLINT,
        ost INT,
        nord INT
    )
    DISTSTYLE KEY
    """
)

create_stagingNonMotLocation = (
    """
    CREATE TABLE IF NOT EXISTS staging_NonMotLocation(
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
    )
    DISTSTYLE ALL
    """
)

# Dim Tables

create_dimLocation = (
    """
    CREATE TABLE IF NOT EXISTS dimLocation(
        location_key INT IDENTITY(0,1) PRIMARY KEY, -- counts_id SERIAL PRIMARY KEY,
        location_id SMALLINT NOT NULL,
        location_name VARCHAR(50) NOT NULL,
        location_code VARCHAR(8) NOT NULL,
        count_type CHAR(3) NOT NULL,
        coord_east DECIMAL NOT NULL,
        coord_nord DECIMAL NOT NULL,
        active_from DATE NOT NULL,
        active_to DATE NOT NULL,
        still_active BOOLEAN NOT NULL
    )
    DISTSTYLE ALL
    """
)

create_dimDate = (
    """
    CREATE TABLE IF NOT EXISTS dimDate(
        date_key INT PRIMARY KEY SORTKEY,
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
        last_day_of_year DATE NOT NULL
    )
    DISTSTYLE AUTO
    """
)

create_dimTime = (
    """
    CREATE TABLE IF NOT EXISTS dimTime(
        time_key INT PRIMARY KEY SORTKEY,
        time_of_day CHAR(5) NOT NULL,
        hour SMALLINT NOT NULL,
        half_hour CHAR(13) NOT NULL,
        quarter_hour CHAR(13) NOT NULL,
        minute SMALLINT NOT NULL
    )
    DISTSTYLE ALL
    """
)

# Fact Table

create_factCount = (
    """
    CREATE TABLE IF NOT EXISTS factCount(
        counts_id INT IDENTITY(0,1) PRIMARY KEY, -- counts_id SERIAL PRIMARY KEY,
        date_key INT REFERENCES dimDate (date_key) SORTKEY DISTKEY,
        time_key INT REFERENCES dimTime (time_key),
        location_key SMALLINT REFERENCES dimLocation (location_key),
        type CHAR(1),
        count_total SMALLINT,
        count_in SMALLINT,
        count_out SMALLINT
    )
    DISTSTYLE KEY
    """
)

# COPY INTO STAGING TABLES

# COPY loads data into a table from data files or from an Amazon DynamoDB table.
# Read more here: https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html

copy_stagingNonMotLocation = (
    f"""
    COPY staging_NonMotLocation
    FROM {LOC_DATA}
    CREDENTIALS 'aws_access_key_id={KEY};aws_secret_access_key={SECRET}'
    CSV DELIMITER ','
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    REGION 'eu-west-1';
    """
)

copy_stagingNonMotCount = (
    f"""
    COPY staging_nonMotCount
    FROM 's3://raph-dend-zh-data/data/raw/verkehrszaehlungen/non_mot/test.csv' --{COUNT_DATA_NON_MOT}
    CREDENTIALS 'aws_access_key_id={KEY};aws_secret_access_key={SECRET}'
    CSV DELIMITER ','
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    REGION 'eu-west-1';
    """
)

# INSERT INTO FINAL TABLES

# Note: I use DISTINCT statement to handle possible duplicates

insert_dimLocation = (
    """
    INSERT INTO dimLocation(
        location_key,
        location_id,
        location_name,
        location_code,
        count_type,
        coord_east,
        coord_nord,
        active_from,
        active_to,
        still_active
    )
    SELECT
        DISTINCT sl.id1 AS location_id,
        sl.bezeichnung AS location_name,
        sl.abkuerzung AS location_code,
        LEFT(sl.abkuerzung, 1) AS count_type,
        sl.ost AS coord_east,
        sl.nord AS coord_nord,
        sl.von AS active_from
        sl.bis AS active_to ---------------------- not good
        -- CASE ??? AS still_active --------------- not inside yet
    FROM stagingNoMotLocation as sl
    """
)

insert_factCount = (
    """
    INSERT INTO factCount(
        date_key,
        time_key,
        location_key,
        count_type,
        count_total,
        count_in,
        count_out
    )
    SELECT
        TO_CHAR(sc.datum,'yyyymmdd')::INT AS date_key,
        EXTRACT(HOUR FROM sc.datum)*100 + EXTRACT(MINUTE FROM sc.datum) AS time_key,
        sc.fk_standort AS sc.location_key,
        dl.count_type AS count_type,
        sc.velo_in + sc.velo_out + sc.fuss_in + sc.fuss_out AS count_total,
        sc.velo_in + sc.fuss_in AS count_in
        sc.velo_out + sc.fuss_out AS count_out
    FROM stagingNonMotCount AS sc
    JOIN dimLocation AS dl
        ON dl.location_id = sc.fk_standort
    """
)

insert_dimDate = (
    """
    INSERT INTO dimDate
    SELECT
        TO_CHAR(datum,'yyyymmdd')::INT AS date_key,
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
    ORDER BY 1
    """
)

insert_dimTime = (
    """
    INSERT INTO dimTime
    SELECT
        EXTRACT(HOUR FROM MINUTE)*60 + EXTRACT(MINUTE FROM MINUTE) AS time_key,
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
    ORDER BY 1
    """
)

# QUERY LISTS

create_table_queries = [
    create_dimDate,
    create_dimLocation,
    create_dimTime,
    create_factCount,
    create_stagingNonMotCount,
    create_stagingNonMotLocation
]

drop_table_queries = [
    drop_factCount,
    drop_dimDate,
    drop_dimLocation,
    drop_dimTime,
    drop_stagingNonMotCount,
    drop_stagingNonMotLocation
]

copy_table_queries = [
    copy_stagingNonMotCount,
    copy_stagingNonMotLocation
]

insert_table_queries = [
    insert_dimDate,
    insert_dimLocation,
    insert_dimTime,
    insert_factCount,
]
