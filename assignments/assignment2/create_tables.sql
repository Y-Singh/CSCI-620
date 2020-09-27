CREATE TABLE Title (
    id INTEGER PRIMARY KEY,
    type VARCHAR(255),
    title VARCHAR(512),
    originalTitle VARCHAR(512),
    startYear INTEGER,
    endYear INTEGER,
    runtime VARCHAR(32),
    avgRatings DECIMAL,
    numVotes INTEGER
);

CREATE TABLE Genre (
    id INTEGER PRIMARY KEY,
    genre VARCHAR(255)
);

CREATE TABLE Title_Genre(
    titleID INTEGER, 
    genreID INTEGER,
    PRIMARY KEY (titleID, genreID),
    FOREIGN KEY (titleID)
        REFERENCES Title (id),
    FOREIGN KEY (genreID)
        REFERENCES Genre (id)
);

CREATE TABLE Member (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    birthYear INTEGER,
    deathYear INTEGER 
);

CREATE TABLE Title_Actor (
    titleID INTEGER,
    actorID INTEGER,
    PRIMARY KEY (titleID, actorID),
    FOREIGN KEY (titleID)
        REFERENCES Title (id),
    FOREIGN KEY (actorID)
        REFERENCES Member (id)
);

CREATE TABLE Title_Writer (
    titleID INTEGER,
    writerID INTEGER,
    PRIMARY KEY (titleID, writerID),
    FOREIGN KEY (titleID)
        REFERENCES Title (id),
    FOREIGN KEY (writerID)
        REFERENCES Member (id)
);

CREATE TABLE Title_Director (
    titleID INTEGER,
    directorID INTEGER,
    PRIMARY KEY (titleID, directorID),
    FOREIGN KEY (titleID)
        REFERENCES Title (id),
    FOREIGN KEY (directorID)
        REFERENCES Member (id)
);

CREATE TABLE Title_Producer (
    titleID INTEGER,
    producerID INTEGER,
    PRIMARY KEY (titleID, producerID),
    FOREIGN KEY (titleID)
        REFERENCES Title (id),
    FOREIGN KEY (producerID)
        REFERENCES Member (id)
);

CREATE TABLE Role (
    id INTEGER PRIMARY KEY,
    role VARCHAR(255)
);

CREATE TABLE Actor_Title_Role(
    titleID INTEGER,
    actorID INTEGER,
    roleID INTEGER,
    PRIMARY KEY (titleID, actorID, roleID),
    FOREIGN KEY (titleID)
        REFERENCES Title (id),
    FOREIGN KEY (actorID)
        REFERENCES Member (id),
    FOREIGN KEY (roleID)
        REFERENCES Role (id)
);
