import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
 CREATE TABLE staging_events(
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          CHAR(1),
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          FLOAT,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    FLOAT,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          VARCHAR 
                            )                             
""")

staging_songs_table_create = ("""
 CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )                             
""")

songplay_table_create = ("""
 CREATE TABLE songplay(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time  TIMESTAMP,
        user_id     VARCHAR,
        level       VARCHAR,
        song_id     VARCHAR,
        artist_id   VARCHAR,
        session_id  INTEGER,
        location    VARCHAR,
        user_agent  VARCHAR
    )
""")

user_table_create = ("""
CREATE TABLE users(
        user_id     VARCHAR PRIMARY KEY,
        first_name  VARCHAR,
        last_name   VARCHAR,
        gender      VARCHAR,
        level       VARCHAR
    )
""")

song_table_create = ("""
CREATE TABLE songs(
        song_id     VARCHAR PRIMARY KEY,
        title       VARCHAR,
        artist_id   VARCHAR,
        year        INTEGER,
        duration    FLOAT
    )
""")

artist_table_create = ("""
CREATE TABLE artists(
        artist_id   VARCHAR  PRIMARY KEY,
        name        VARCHAR,
        location    VARCHAR,
        latitude    FLOAT,
        longitude   FLOAT
    )
""")

time_table_create = ("""
CREATE TABLE time(
        start_time  TIMESTAMP   NOT NULL PRIMARY KEY,
        hour        INTEGER     NOT NULL,
        day         INTEGER     NOT NULL,
        week        INTEGER     NOT NULL,
        month       INTEGER     NOT NULL,
        year        INTEGER     NOT NULL,
        weekday     VARCHAR(20) NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {}
    region '{}'
    timeformat 'epochmillisecs';                      
""").format('staging_events',
    config.get('S3','LOG_DATA'),
    config.get('IAM_ROLE','ARN'),
    config.get('S3','LOG_JSONPATH'),
    config.get('CLUSTER','REGION')
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto'
    region '{}'
    compupdate off
    EMPTYASNULL
    BLANKSASNULL;
""").format('staging_songs',
    config.get('S3','SONG_DATA'),
    config.get('IAM_ROLE','ARN'),
    config.get('CLUSTER','REGION')
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
    SELECT
        se.ts as start_time,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId as session_id,
        se.location,
        se.userAgent as user_agent
    FROM staging_events se
    LEFT JOIN staging_songs ss ON
        se.song = ss.title AND
        se.artist = ss.artist_name AND
        se.length = ss.duration
    WHERE
        se.page = 'NextSong' AND ss.song_id IS NOT NULL AND se.userId IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users(
       user_id,
       first_name,
       last_name,
       gender,
       level)
    SELECT DISTINCT
        userId as  user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM staging_events se 
    WHERE page = 'NextSong' AND se.userId IS NOT NULL;               
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL               
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekDay)
    SELECT DISTINCT
        start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(dayofweek from start_time)
    FROM songplay                  
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
