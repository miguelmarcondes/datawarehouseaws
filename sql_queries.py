import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist VARCHAR, \
        auth VARCHAR, \
        firstName VARCHAR, \
        gender VARCHAR, \
        itemInSession INT, \
        lastName VARCHAR, \
        length FLOAT, \
        level VARCHAR, \
        location VARCHAR, \
        method VARCHAR, \
        page VARCHAR, \
        registration FLOAT, \
        session_id INT, \
        song VARCHAR, \
        status INT, \
        ts BIGINT, \
        userAgent VARCHAR, \
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs INT, \
        artist_id VARCHAR, \
        artist_latitude FLOAT, \
        artist_longitude FLOAT, \
        artist_location VARCHAR, \
        artist_name VARCHAR, \
        song_id VARCHAR, \
        title VARCHAR, \
        duration FLOAT, \
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY, \
        start_time TIMESTAMP NOT NULL, \
        user_id INT NOT NULL, \
        level VARCHAR, \
        song_id VARCHAR, \
        artist_id VARCHAR, \
        session_id INT, \
        location VARCHAR, \
        user_agent VARCHAR \
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id INT IDENTITY(0,1) PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id VARCHAR NOT NULL PRIMARY KEY, 
        title VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        year INT, 
        duration FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id VARCHAR NOT NULL PRIMARY KEY, 
        name VARCHAR NOT NULL, 
        location VARCHAR, 
        latitude FLOAT, 
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} 
iam_role '{}'
format AS json '{}'
compupdate off;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs from {} 
iam_role '{}'
format AS json 'auto'
compupdate off;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second', se.userId, se.level, ss.song_id, ss.artist_id, se.sesion_id, se.location, se.user_agent
    FROM staging_events as se JOIN staging_songs as ss 
    ON se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT se.user_id, se.firstName, se.lastName, se.gender, se.level
    FROM staging_events as se;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging_songs as ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
    FROM staging_songs as ss;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS start_time,
           extract(hour from start_time) as hour,
           extract(day from start_time) as day,
           extract(week from start_time) as week,
           extract(month from start_time) as month,
           extract(year from start_time) as year,
           extract(dow from start_time) as week_day
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
