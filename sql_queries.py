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
    migrated     boolean      DEFAULT false
)
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
    migrated     boolean      DEFAULT false
)
;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id  int          PRIMARY KEY     IDENTITY(0,1),
    start_time   timestamp    NOT NULL        REFERENCES time(start_time)     SORTKEY,
    user_id      int          NOT NULL        REFERENCES users(user_id),
    song_id      text         NOT NULL        REFERENCES songs(song_id)       DISTKEY,
    artist_id    text         NOT NULL        REFERENCES artists(artist_id),
    level        text,
    session_id   int          NOT NULL,
    location     text,
    user_agent   text
)
diststyle key
;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id      int          PRIMARY KEY     SORTKEY,
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
    song_id      text         PRIMARY KEY     SORTKEY DISTKEY,
    title        text,
    artist_id    text,
    year         int,
    duration     decimal
)
diststyle key
;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id    text         PRIMARY KEY     SORTKEY,
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
    start_time   timestamp    PRIMARY KEY     SORTKEY,
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
(user_id, ts, artist_name, first_name, last_name, gender, duration, song_name, level, session_id, location, user_agent)
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
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
