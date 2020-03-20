import os
import glob
import pandas as pd
import sql_queries as sql
from utils import get_connection


def process_song_file(cur, filepath):
    """Process a given song file and load data to database."""
    # Open song file
    df = pd.read_json(filepath, lines=True)

    # Insert a song record
    song_cols = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = df[song_cols].values[0].tolist()
    cur.execute(sql.song_table_insert, song_data)

    # Insert an artist record
    artist_cols = ["artist_id",
                   "artist_name",
                   "artist_location",
                   "artist_latitude",
                   "artist_longitude",
                   ]
    artist_data = df[artist_cols].values[0].tolist()
    cur.execute(sql.artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Process a given log file and load data to database."""
    # Open log file
    df = pd.read_json(filepath, lines=True)
    # Filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]
    # Convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")

    # Extract data for time table
    time_data = [df["ts"],
                 df["ts"].dt.hour,
                 df["ts"].dt.day,
                 df["ts"].dt.week,
                 df["ts"].dt.month,
                 df["ts"].dt.year,
                 df["ts"].dt.weekday,
                 ]
    cols = ["timestamp",
            "hour",
            "day",
            "week",
            "month",
            "year",
            "weekday",
            ]
    time_df = pd.DataFrame(dict(zip(cols, time_data)))
    # Insert data for time table
    for i, row in time_df.iterrows():
        cur.execute(sql.time_table_insert, list(row))

    # Load user table (simply select the respective columns)
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    # Insert user records
    for i, row in user_df.iterrows():
        cur.execute(sql.user_table_insert, row)

    # Insert songplay records
    for index, row in df.iterrows():
        # Get songid and artistid from song and artist tables
        cur.execute(sql.song_select, (row["song"],
                                      row["artist"],
                                      row["length"]
                                      )
                    )
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # Insert songplay record
        songplay_data = [row["ts"],
                         row["userId"],
                         row["level"],
                         songid,
                         artistid,
                         row["sessionId"],
                         row["location"],
                         row["userAgent"],
                         ]
        cur.execute(sql.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Process each data file in a give filepath using the passed function."""
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}.")

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        # conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    conn, cur = get_connection()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
