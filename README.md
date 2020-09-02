#Summary

This project aims to test the understanding and workflow of data warehousing techniques.
Amazon Redshift gives you the best of high performance data warehouses with the unlimited flexibility and we have used Python for coding and Amazon s3 for storage. 
The task incorporates setting up the Sparkify application which has all the songs data which the users are listening to.

##Source Data and Schema

Following are the fact and dimension tables made for this project:

Dimension Tables:
users
columns: user_id, first_name, last_name, gender, level
songs
columns: song_id, title, artist_id, year, duration
artists
columns: artist_id, name, location, lattitude, longitude
time
columns: start_time, hour, day, week, month, year, weekday
Fact Table:
songplays
columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## To run the project:

Run command to install requirements.
```bash
pip install -r requirements.txt
```

Update the dwh.cfg file with you Amazon Redshift cluster credentials and IAM role that can access the cluster.

Use the python scripts in the order to create tables and insert data

```python
python sql_queries.py
python create_tables.py
python etl.py
```

## Testing

Check on Redshift Data Query Editor.

```bash
SELECT * FROM songplays LIMIT 5 ;
SELECT * FROM users LIMIT 5 ;
SELECT * FROM songs LIMIT 5 ;
SELECT * FROM artists LIMIT 5 ;
SELECT * FROM time LIMIT 5 ;
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
Nishtha Bhattacharjee(nishthabhattacharjee94@gmail.com)