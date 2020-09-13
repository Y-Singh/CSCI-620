# import psycopg2
from psycopg2 import errors, sql, connect
import csv
import os
import re
from pdb import set_trace

class IMDB_DB:
    def __init__(self, path):
        self.conn = connect(
            database="imdb", user='postgres', password='admin', host='127.0.0.1', port='5432'
        )
        self.cur = self.conn.cursor()
        self.path = path
        self.movies_query = []
        self.actors_query = []
        self.directors_query = []

    def read_movies_basic(self):
        tsv_file = open(path+"\\title.basics.tsv\\data.tsv", 'r', encoding='utf-8')
        movies_tsv = csv.DictReader(tsv_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in movies_tsv:
            if row['titleType'] == "movie":
                if row['isAdult'] == '0':
                    params = row
                    genres = params['genres'].split(',')
                    gs = [None] * 3
                    for i in range(0, len(genres)):
                        gs[i] = genres[i]
                    params.pop('genres', None)
                    params['genre_one'] = gs[0]
                    params['genre_two'] = gs[1]
                    params['genre_three'] = gs[2]
                    params['tconst'] = row['tconst'][2:]
                    self.create_movie(params)
        tsv_file.close()

    def create_movie(self,  params:dict):
        qry = """
            INSERT INTO movies 
            (title_id, primary_title, original_title, runtime_minutes, genre_one, genre_two, genre_three)
            VALUES
            (%(tconst)s, %(primaryTitle)s, %(originalTitle)s, %(runtimeMinutes)s , %(genre_one)s, %(genre_two)s, %(genre_three)s)
        """
        self.movies_query.append((qry, params))
    
    def read_names_basic(self):
        tsv_file = open(path + "\\name.basics.tsv\\data.tsv", 'r', encoding='utf-8')
        names_tsv = csv.DictReader(tsv_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in names_tsv:
            professions = row['primaryProfession'].split(',')
            professions = [job.upper() for job in professions]
            jobs = self.check_actor_dups(professions)
            params = dict()
            name = row['primaryName'].split(" ", 1)
            params['id'] = row['nconst'][2:]
            params['first_name'] = name[0]
            params['last_name'] = name[1]
            params['birth_year'] = row['birthYear']
            params['death_year'] = row['deathYear']
            for job in jobs:
                if job == "ACTOR" or job == "ACTRESS":
                    params['table_name'] = 'actors'
                    params['id_type'] = 'actor_id'
                    self.create_showbiz_person(params)
                elif job == "DIRECTOR":
                    params['table_name'] = 'directors'
                    params['id_type'] = 'director_id'
                    self.create_showbiz_person(params)
                elif job == "WRITER":
                    params['table_name'] = 'writers'
                    params['id_type'] = 'writer_id'
                    self.create_showbiz_person(params)
                elif job == "PRODUCERS":
                    params['table_name'] = 'producers'
                    params['id_type'] = 'producer_id'
                    self.create_showbiz_person(params)
    
    def check_actor_dups(self, professions:list):
        sub = "ACT"
        x = [i for i in professions if sub in i]
        if len(x) == 2:
            professions.remove(x[-1])
        return professions
    
    def check_params(self, params:dict):
        for key, value in params.items():
            if params[key] == "\\N":
                params[key] = None
        return params

    def create_showbiz_person(self, params:dict):
        params = self.check_params(params)
        qry = sql.SQL(
            """
            INSERT INTO {table}
            ({id_type}, first_name, last_name, birth_year, death_year)
            VALUES
            (%(id)s, %(first_name)s, %(last_name)s, %(birth_year)s, %(death_year)s);
            """
            ).format(
                table=sql.Identifier(params['table_name']),
                id_type=sql.Identifier(params['id_type'])
                )        
        try:
            self.cur.execute(qry, params)
        except errors.UniqueViolation as err:
            print("[ERROR] : Actor already exists")
            print(err)
            self.conn.rollback()
        else:
            self.conn.commit()

    def create_actor(self, params:dict):
        params = self.check_params(params)
        qry = """
            INSERT INTO actors
            (actor_id, first_name, last_name, birth_year, death_year)
            VALUES
            (%(id)s, %(first_name)s, %(last_name)s, %(birth_year)s, %(death_year)s);
        """
        try:
            self.cur.execute(qry, params)
        except errors.UniqueViolation as err:
            print("[ERROR] : Actor already exists")
            print(err)
            self.conn.rollback()
        else:
            self.conn.commit()
    
    def create_director(self, params:dict):
        params = self.check_params(params)
        qry = """
            INSERT INTO directors
            (director_id, first_name, last_name, birth_year, death_year)
            VALUES
            (%(id)s, %(first_name)s, %(last_name)s, %(birth_year)s, %(death_year)s);
        """ 
        try:
            self.cur.execute(qry, params)
        except errors.UniqueViolation as err:
            print("[ERROR] : Actor already exists")
            print(err)
            self.conn.rollback()
        else:
            self.conn.commit()

    def create_writer(self, params:dict):
        params = self.check_params(params)
        qry = """
            INSERT INTO writers
            (writer_id, first_name, last_name, birth_year, death_year)
            VALUES
            (%(id)s, %(first_name)s, %(last_name)s, %(birth_year)s, %(death_year)s)
        """ 
        try:
            self.cur.execute(qry, params)
        except errors.UniqueViolation as err:
            print("[ERROR] : Actor already exists")
            print(err)
            self.conn.rollback()
        else:
            self.conn.commit()
    
        


if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    dB = IMDB_DB(path)
    dB.read_names_basic()
    