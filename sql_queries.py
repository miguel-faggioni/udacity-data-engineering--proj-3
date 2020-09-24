#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_data;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_data;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_log_data (
    user_id      int,
    ts           timestamp,
    artist_name  text,
    first_name   text,
    last_name    text,
    gender       text,
    duration     decimal,
    song_name    text,
    level        text,
    session_id   int          NOT NULL,
    location     text,
    user_agent   text,
    page         text,
    migrated_user     boolean      DEFAULT false,
    migrated_time     boolean      DEFAULT false,
    migrated_songplay     boolean      DEFAULT false
)
diststyle all
;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_song_data (
    song_id      text,
    artist_id    text,
    latitude     numeric(9,6),
    longitude    numeric(9,6),
    location     text,
    title        text,
    year         int,
    duration     decimal,
    artist_name  text,
    migrated_song     boolean      DEFAULT false,
    migrated_artist     boolean      DEFAULT false
)
diststyle all
;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    start_time   timestamp    NOT NULL        REFERENCES time(start_time),
    user_id      int          NOT NULL        REFERENCES users(user_id),
    song_id      text         NOT NULL        REFERENCES songs(song_id),
    artist_id    text         NOT NULL        REFERENCES artists(artist_id),
    level        text,
    session_id   int          NOT NULL,
    location     text,
    user_agent   text,
    songplay_id  int          PRIMARY KEY     IDENTITY(0,1)
)
diststyle all
;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id      int          PRIMARY KEY,
    first_name   text,
    last_name    text,
    gender       text,
    level        text
)
diststyle all
;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id      text         PRIMARY KEY,
    title        text,
    artist_id    text,
    year         int,
    duration     decimal
)
diststyle all
;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id    text         PRIMARY KEY,
    name         text,
    location     text,
    latitude     numeric(9,6),
    longitude    numeric(9,6)
)
diststyle all
;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time   timestamp    PRIMARY KEY,
    hour         int          NOT NULL,
    day          int          NOT NULL,
    week         int          NOT NULL,
    month        int          NOT NULL,
    year         int          NOT NULL,
    weekday      int          NOT NULL
)
diststyle all
;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_log_data 
(user_id, ts, artist_name, first_name, last_name, gender, duration, song_name, level, session_id, location, user_agent, page)
FROM '{}'
REGION 'us-west-2'
CREDENTIALS 'aws_iam_role={}'
JSON '{}'
TIMEFORMAT 'epochmillisecs'
;
""").format(
    config.get('S3','log_data'),
    config.get('IAM_ROLE','arn'),
    config.get('S3','staging_log_data_jsonpath')
)

staging_songs_copy = ("""
COPY staging_song_data
(song_id, artist_id, latitude, longitude, location, title, year, duration, artist_name)
FROM '{}'
REGION 'us-west-2'
CREDENTIALS 'aws_iam_role={}'
JSON '{}'
;
""").format(
    config.get('S3','song_data'),
    config.get('IAM_ROLE','arn'),
    config.get('S3','staging_song_data_jsonpath')
)

# FINAL TABLES

songplay_table_insert = ("""
BEGIN TRANSACTION;

INSERT INTO songplays (
  SELECT staging_log_data.ts as start_time,
         staging_log_data.user_id,
         staging_song_data.song_id,
         staging_song_data.artist_id,
         staging_log_data.level,
         staging_log_data.session_id,
         staging_log_data.location,
         staging_log_data.user_agent
  FROM staging_log_data
  JOIN staging_song_data
   ON staging_log_data.artist_name = staging_song_data.artist_name 
  AND staging_log_data.song_name   = staging_song_data.title
  AND staging_log_data.duration    = staging_song_data.duration
  WHERE staging_log_data.migrated_songplay = false 
    AND staging_log_data.page              = 'NextSong'
);

UPDATE staging_log_data
SET migrated_songplay = true
FROM songplays
WHERE staging_log_data.user_id           = songplays.user_id
  AND staging_log_data.session_id        = songplays.session_id
  AND staging_log_data.ts                = songplays.start_time
  AND staging_log_data.migrated_songplay = false
  AND staging_log_data.page              = 'NextSong';

END TRANSACTION;
""")

user_table_insert = ("""
BEGIN TRANSACTION;

INSERT INTO users (
  WITH unique_users AS (
          SELECT MAX(ts) AS ts, user_id
          FROM staging_log_data
          WHERE staging_log_data.page = 'NextSong'
          GROUP BY user_id
      )
  SELECT staging_log_data.user_id, first_name, last_name, gender, level
  FROM staging_log_data
  JOIN unique_users on unique_users.ts = staging_log_data.ts AND unique_users.user_id = staging_log_data.user_id
  WHERE staging_log_data.page          = 'NextSong'
    AND staging_log_data.migrated_user = false
);

UPDATE staging_log_data
SET migrated_user = true
FROM users
WHERE staging_log_data.user_id       = users.user_id
  AND staging_log_data.migrated_user = false
  AND staging_log_data.page          = 'NextSong';

END TRANSACTION;
""")

song_table_insert = ("""
BEGIN TRANSACTION;

INSERT INTO songs (
  SELECT song_id, title, artist_id, year, duration
  FROM staging_song_data
  WHERE staging_song_data.migrated_song = false 
);

UPDATE staging_song_data
SET migrated_song = true
FROM songs
WHERE staging_song_data.song_id       = songs.song_id
  AND staging_song_data.migrated_song = false;

END TRANSACTION;
""")

artist_table_insert = ("""
BEGIN TRANSACTION;

INSERT INTO artists (
  SELECT artist_id, artist_name as name, location, latitude, longitude
  FROM staging_song_data
  WHERE staging_song_data.migrated_artist = false 
);

UPDATE staging_song_data
SET migrated_artist = true
FROM artists
WHERE staging_song_data.artist_id       = artists.artist_id
  AND staging_song_data.migrated_artist = false;

END TRANSACTION;
""")

time_table_insert = ("""
BEGIN TRANSACTION;

INSERT INTO time (
  SELECT ts as start_time,
         date_part('hour',ts)  as hour,
         date_part('day',ts)   as day,
         date_part('week',ts)  as week,
         date_part('month',ts) as month,
         date_part('year',ts)  as year,
         date_part('dow',ts)   as weekday
  FROM staging_log_data
  WHERE staging_log_data.migrated_time = false 
    AND staging_log_data.page          = 'NextSong'
);

UPDATE staging_log_data
SET migrated_time = true
FROM time
WHERE staging_log_data.ts            = time.start_time
  AND staging_log_data.migrated_time = false;

END TRANSACTION;
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert
]
