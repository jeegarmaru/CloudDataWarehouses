import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                        CREATE TABLE staging_events (
                            artist varchar,
                            auth varchar,
                            firstName varchar,
                            gender varchar,
                            itemInSession int,
                            lastName varchar,
                            length float,
                            level varchar,
                            location varchar,
                            method varchar,
                            page varchar,
                            registration varchar,
                            sessionId int,
                            song varchar,
                            status int,
                            ts bigint, 
                            userAgent varchar,
                            userId int)
""")

staging_songs_table_create = ("""
                        CREATE TABLE staging_songs (
                            num_songs int,
                            artist_id varchar,
                            artist_latitude float,
                            artist_longitude float,
                            artist_location varchar,
                            artist_name varchar,
                            song_id varchar,
                            title varchar,
                            duration float,
                            year int)
                        """)

songplay_table_create = ("""
                        CREATE TABLE songplays (
                            songplay_id bigint IDENTITY(0,1) PRIMARY KEY sortkey distkey,
                            start_time bigint NOT NULL,
                            user_id int NOT NULL,
                            level varchar,
                            song_id varchar,
                            artist_id varchar,
                            session_id int,
                            location varchar,
                            user_agent varchar)
                        """)

user_table_create = ("""
                    CREATE TABLE users (
                        user_id int PRIMARY KEY,
                        first_name varchar,
                        last_name varchar,
                        gender varchar,
                        level varchar)
                        diststyle all;
                    """)

song_table_create = ("""
                    CREATE TABLE songs(
                        song_id varchar PRIMARY KEY,
                        title varchar,
                        artist_id varchar,
                        year int,
                        duration float)
                        diststyle all;
                    """)

artist_table_create = ("""
                      CREATE TABLE artists (
                          artist_id varchar PRIMARY KEY,
                          name varchar,
                          location varchar,
                          latitude float,
                          longitude float)
                          diststyle all;
                      """)

time_table_create = ("""
                    CREATE TABLE time (
                        start_time timestamp PRIMARY KEY,
                        hour int,
                        day int,
                        week int,
                        month int,
                        year int,
                        weekday int)
                        diststyle all;
                    """)

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent)
                        (SELECT DISTINCT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, s.artist_location, e.userAgent
                           FROM staging_events e
                           JOIN staging_songs s ON e.song=s.title AND e.artist=s.artist_name AND e.length=s.duration)
                        """)

user_table_insert = ("""
                    INSERT INTO users (
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
                    (SELECT DISTINCT userId, firstName, lastName, gender, level
                       FROM staging_events
                      WHERE userId IS NOT NULL)
                    """)

song_table_insert = ("""
                    INSERT INTO songs (
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration)
                    (SELECT DISTINCT song_id, title, artist_id, year, duration
                       FROM staging_songs
                      WHERE song_id IS NOT NULL)
                    """)

artist_table_insert = ("""
                      INSERT INTO artists (
                          artist_id, 
                          name, 
                          location, 
                          latitude, 
                          longitude)
                      (SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                         FROM staging_songs
                        WHERE artist_id IS NOT NULL)
                      """)


time_table_insert = ("""
                    INSERT INTO time (
                        start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
                    (SELECT DISTINCT (timestamp 'epoch' + ts/1000 * interval '1 second'),
                            EXTRACT(hour from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                            EXTRACT(day from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                            EXTRACT(week from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                            EXTRACT(month from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                            EXTRACT(year from (timestamp 'epoch' + ts/1000 * interval '1 second')),
                            EXTRACT(weekday from (timestamp 'epoch' + ts/1000 * interval '1 second'))
                       FROM staging_events
                      WHERE ts IS NOT NULL)
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
