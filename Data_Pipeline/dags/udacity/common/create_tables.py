class CreateTables:
    tables = """
        BEGIN;  
        CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstname VARCHAR,
        gender CHAR,
        itemInSession INTEGER,
        lastname VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
        );
        COMMIT;
        BEGIN;  
        CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR, 
        num_songs INTEGER,
        title VARCHAR(1000), 
        artist_name VARCHAR(1000), 
        artist_latitude FLOAT, 
        year INTEGER, 
        duration FLOAT, 
        artist_id VARCHAR, 
        artist_longitude FLOAT, 
        artist_location VARCHAR(1000)
        );
        COMMIT;
        BEGIN;  
        CREATE TABLE IF NOT EXISTS songplays (
        songplay_id VARCHAR PRIMARY KEY, 
        start_time TIMESTAMP, 
        user_id INTEGER, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INTEGER, 
        location VARCHAR, 
        user_agent VARCHAR
        )
        DISTSTYLE KEY
        DISTKEY (songplay_id) SORTKEY (songplay_id);
        COMMIT;
        BEGIN;  
        CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender CHAR, 
        level VARCHAR
        )
        SORTKEY (user_id);
        COMMIT;
        BEGIN;  
        CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year INTEGER, 
        duration FLOAT
        )
        SORTKEY (song_id);
        COMMIT;
        BEGIN;  
        CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR(1000), 
        location VARCHAR(1000), 
        lattitude FLOAT, 
        longitude FLOAT
        )
        SORTKEY (artist_id);
        COMMIT;
        BEGIN;  
        CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INTEGER, 
        day INTEGER, 
        week INTEGER, 
        month INTEGER, 
        year INTEGER, 
        weekday VARCHAR
        )
        DISTSTYLE KEY 
        DISTKEY (start_time) SORTKEY (start_time);
        COMMIT;
    """