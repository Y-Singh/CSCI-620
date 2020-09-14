CREATE TABLE movies (
    title_id INTEGER PRIMARY KEY,
    primary_title VARCHAR(255),
    original_title VARCHAR(255),
    region VARCHAR(127),
    language VARCHAR(127),
    release_year INTEGER,
    runtime_minutes INTEGER,
    genre_one VARCHAR(63),
    genre_two VARCHAR(63),
    genre_three VARCHAR(63),
    ratings DECIMAL,
    votes INTEGER
);

CREATE TABLE actors (
    actor_id INTEGER PRIMARY KEY,
    first_name VARCHAR(127),
    last_name VARCHAR(127),
    birth_year INTEGER,
    death_year INTEGER
);

CREATE TABLE directors (
    director_id INTEGER PRIMARY KEY,
    first_name VARCHAR(127),
    last_name VARCHAR(127),
    birth_year INTEGER,
    death_year INTEGER
);

CREATE TABLE writers (
    writer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(127),
    last_name VARCHAR(127),
    birth_year INTEGER,
    death_year INTEGER
);

CREATE TABLE producers (
    producer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(127),
    last_name VARCHAR(127),
    birth_year INTEGER,
    death_year INTEGER
);

CREATE TABLE movie_actor_relation (
    title_id INTEGER, 
    actor_id INTEGER,
    PRIMARY KEY (title_id, actor_id),
    FOREIGN KEY (title_id)
        REFERENCES movies (title_id),
    FOREIGN KEY (actor_id)
        REFERENCES actors (actor_id)
);

CREATE TABLE movie_director_relation (
    title_id INTEGER, 
    director_id INTEGER,
    PRIMARY KEY (title_id, director_id),
    FOREIGN KEY (title_id)
        REFERENCES movies (title_id),
    FOREIGN KEY (director_id)
        REFERENCES directors (director_id)
);

CREATE TABLE movie_writer_relation (
    title_id INTEGER, 
    writer_id INTEGER,
    PRIMARY KEY (title_id, writer_id),
    FOREIGN KEY (title_id)
        REFERENCES movies (title_id),
    FOREIGN KEY (writer_id)
        REFERENCES writers (writer_id)
);

CREATE TABLE movie_producer_relation (
    title_id INTEGER, 
    producer_id INTEGER,
    PRIMARY KEY (title_id, producer_id),
    FOREIGN KEY (title_id)
        REFERENCES movies (title_id),
    FOREIGN KEY (producer_id)
        REFERENCES producers (producer_id)
);