import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS staging_songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist varchar,auth varchar,firstName varchar,gender varchar,itemInSession int,lastName varchar,length float,level varchar,location varchar,method varchar,page varchar,registration float,sessionId int,song varchar,status int,ts bigint,userAgent varchar,userId varchar)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(artist_id varchar,artist_latitude float,artist_location varchar,artist_longitude float,artist_name varchar,duration float,num_songs int,song_id varchar,title varchar,year int)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1) PRIMARY KEY,user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id varchar NOT NULL, location varchar NOT NULL, start_time bigint,user_agent varchar)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(user_id varchar PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar NOT NULL)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float NOT NULL )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY, name varchar, location varchar NOT NULL, latitude float, longitude float )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time varchar,hour int, day int, week int, month int, year int, weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} iam_role {} compupdate off region 'us-west-2' JSON {} TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs from {} iam_role {} compupdate off region 'us-west-2' JSON 'auto';""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT into songplays(user_id , level , song_id , artist_id , session_id , location ,start_time, user_agent)
SELECT events.userId,events.level,songs.song_id,songs.artist_id,events.sessionId,events.location,events.ts,events.userAgent 
from staging_events AS events join staging_songs AS songs 
on (events.artist = songs.artist_name)
AND (events.song = songs.title)
AND (events.length = songs.duration)
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT events.userId,events.firstName,events.lastName,events.gender,events.level
from staging_events AS events
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,title,artist_id,year,duration from staging_songs
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
from staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT a.start_time,
EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time),
EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),
EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) FROM
(SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplays) a;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
