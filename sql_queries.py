import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = " DROP TABLE IF EXISTS users;"
song_table_drop = " DROP TABLE IF EXISTS songs;"
artist_table_drop = " DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """ CREATE TABLE  IF NOT EXISTS staging_events(
artistname        varchar,
auth              varchar,
firstname         varchar,
gender            varchar(1),
iteminsession     int,
lastname          varchar,
length            decimal,
level             varchar,
location          varchar,
method            varchar,
page              varchar,
registration      varchar,
sessionid         int, 
song              varchar,
status            int,
ts                bigint,
useragent         varchar,
userid            int);
"""

staging_songs_table_create = """ CREATE TABLE IF NOT EXISTS staging_songs(
         num_songs          INT,
         artist_id           TEXT,
         artist_latitude     FLOAT,
         artist_longitude    FLOAT,
         artist_location     TEXT,
         artist_name         VARCHAR(255),
         song_id             TEXT ,
         title              TEXT,
         duration           FLOAT,
         year               INT);
"""


user_table_create = """ CREATE TABLE IF NOT EXISTS users (
userid     int  PRIMARY KEY     distkey,
firstName  varchar,
lastName   varchar,
gender     varchar,
level      varchar sortkey NOT NULL);
"""


song_table_create = """ CREATE TABLE IF NOT EXISTS songs (
songid     varchar PRIMARY KEY  sortkey,
title      varchar  NOT NULL,
artistid   varchar  NOT NULL,
year       int      ,
duration   decimal  );
"""

artist_table_create = """ CREATE TABLE IF NOT EXISTS artists(
artistid        varchar PRIMARY KEY sortkey,
artistname      varchar NOT NULL,
artistlocation  varchar,
artistlatitude  decimal, 
artistlongitude decimal);
"""

time_table_create = """ CREATE TABLE IF NOT EXISTS time (
start_time        timestamp  PRIMARY KEY sortkey,
hour              int       NOT NULL,
day               int       NOT NULL,
week              int       NOT NULL,
month             int       NOT NULL,
year              int       NOT NULL,
weekday           int       NOT NULL);
"""

songplay_table_create = """ CREATE TABLE  IF NOT EXISTS songplays (
songplayid   integer identity(0,1) PRIMARY KEY ,
start_time   timestamp    REFERENCES time(start_time) sortkey,
userid       int          REFERENCES users(userid)  distkey,
level        varchar      ,
songid       varchar      REFERENCES songs(songid),
artistid     varchar      REFERENCES artists(artistid),
sessionid    int          NOT NULL,
location     varchar,
useragent    varchar); 
"""


# STAGING TABLES

staging_events_copy = (
    """                 COPY staging_events FROM {}
                        CREDENTIALS    {}
                        format as json  {}
                        region         'us-west-2';
"""
).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = (
    """                 COPY staging_songs FROM {} 
                        CREDENTIALS       {}
                        format as json    'auto'
                        region            'us-west-2';
"""
).format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (             start_time,
                                        userid,
                                        level,
                                        songid,
                                        artistid,
                                        sessionid,
                                        location,
                                        useragent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userid                   AS userid,
            se.level                    AS level,
            ss.song_id                  AS songid,
            ss.artist_id                AS artistid,
            se.sessionid                AS sessionid,
            se.location                 AS location,
            se.useragent                AS useragent
    FROM staging_events AS se
    JOIN staging_songs AS ss
    ON (se.artistname = ss.artist_name)
    WHERE se.page = 'NextSong';
"""

user_table_insert = """
    INSERT INTO users (                 userid,
                                        firstname,
                                        lastname,
                                        gender,
                                        level)
    SELECT  DISTINCT se.userid          AS userid,
            se.firstName                AS firstname,
            se.lastName                 AS lastname,
            se.gender                   AS gender,
            se.level                    AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong' and se.userid IS NOT NULL AND ts = (select max(ts) FROM staging_events s WHERE s.userid = se.userid)
    ORDER BY userId DESC;
"""

song_table_insert = """
    INSERT INTO songs (                songid,
                                       title,
                                       artistid,
                                       year,
                                       duration)
    SELECT  DISTINCT ss.song_id        AS songid,
            ss.title                   AS title,
            ss.artist_id               AS artistid,
            ss.year                    AS year,
            ss.duration                AS duration
    FROM staging_songs AS ss
    WHERE ss.song_id IS NOT NULL;
"""

artist_table_insert = """
    INSERT INTO artists (               artistid,
                                        artistname,
                                        artistlocation,
                                        artistlatitude,
                                        artistlongitude)
    SELECT  DISTINCT ss.artist_id       AS artistid,
            ss.artist_name              AS artistname,
            ss.artist_location          AS artistlocation,
            ss.artist_latitude          AS artistlatitude,
            ss.artist_longitude         AS artistlongitude
    FROM staging_songs AS ss
    WHERE ss.artist_id IS NOT NULL;
"""

time_table_insert = """
    INSERT INTO time (                  start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS se
    WHERE se.page = 'NextSong';
"""

# QUERY LISTS

create_table_queries = [
    time_table_create,
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    songplay_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
