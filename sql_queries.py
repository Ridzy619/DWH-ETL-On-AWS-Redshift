import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
    artist text,
    auth VARCHAR(20),
    firstName VARCHAR(30),
    gender VARCHAR(1),
    itemInSession SMALLINT,
    lastName VARCHAR(30),
    length TEXT,
    level VARCHAR(30),
    location TEXT,
    method VARCHAR(10),
    page VARCHAR(20),
    registration TIMESTAMP,
    sessionId SMALLINT,
    song TEXT,
    status INTEGER,
    ts TIMESTAMP,
    userAgent TEXT,
    userId INTEGER
)
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs SMALLINT,
    artist_id VARCHAR(30),
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location TEXT,
    artist_name TEXT,
    song_id VARCHAR(30),
    title TEXT,
    duration NUMERIC,
    year SMALLINT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS song_plays
(
    songplay_id VARCHAR(30) PRIMARY KEY,
    start_time TIMESTAMP SORTKEY,
    user_id INTEGER NOT NULL DISTKEY,
    level VARCHAR (30),
    song_id VARCHAR(30),
    artist_id VARCHAR(30) NOT NULL,
    session_id INTEGER,
    location TEXT,
    user_agent TEXT

)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id INTEGER PRIMARY KEY SORTKEY DISTKEY,
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    gender VARCHAR(1),
    level VARCHAR(30)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR(30) PRIMARY KEY,
    title TEXT,
    artist_id VARCHAR(30),
    year SMALLINT SORTKEY,
    duration NUMERIC
)
    diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR(30) PRIMARY KEY SORTKEY,
    name TEXT,
    location TEXT,
    lattitude NUMERIC,
    longitude NUMERIC
)
    diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    time_id VARCHAR(15) PRIMARY KEY SORTKEY,
    start_time TIMESTAMP,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday SMALLINT
)
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM '{}'
iam_role '{}'
region 'us-west-2'
JSON '{}'
TIMEFORMAT AS 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM '{}'
iam_role '{}'
region 'us-west-2'
JSON 'auto'
TIMEFORMAT AS 'epochmillisecs';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO song_plays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
SELECT 
        ts,
        userId,
        level,
        song_id,
        artist_id,
        sessionid,
        location,
        userAgent
        
FROM staging_events e
JOIN staging_songs s
    ON e.artist=s.artist_name
        AND s.duration=e.length
        AND s.title=e.song
    
WHERE page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users
    SELECT 
        userid,
        firstname,
        lastname,
        gender,
        level
    
    FROM staging_events
    WHERE userid IS NOT NULL

""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists 
SELECT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude

FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time

SELECT
    TO_CHAR(ts, 'yyyymmdd'),
    ts,
    EXTRACT(HOUR FROM ts),
    EXTRACT(DAY FROM ts),
    EXTRACT(WEEK FROM ts),
    EXTRACT(MONTH FROM ts),
    EXTRACT(YEAR FROM ts),
    EXTRACT(DOW FROM ts)

FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
