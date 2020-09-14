from psycopg2 import errors, sql, connect
import csv
import os
import timeit
import re
import threading

class IMDB_DB:
    def __init__(self, path):
        self.conn = connect(
            database="imdb", user='postgres', password='admin', host='127.0.0.1', port='5432'
        )
        self.cur = self.conn.cursor()
        self.path = path
        self.logger = []

        self.movies_qry = []
        self.actors_qry = []
        self.directors_qry = []
        self.producers_qry = []
        self.writers_qry = []

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
                    params['release_year'] = row['startYear']

                    self.create_movie(params)
        tsv_file.close()

    def create_movie(self,  params:dict):
        params = self.check_params(params)
        qry = """
            INSERT INTO movies 
            (title_id, primary_title, original_title, release_year, runtime_minutes, genre_one, genre_two, genre_three)
            VALUES
            (%(tconst)s, %(primaryTitle)s, %(originalTitle)s, %(release_year)s, %(runtimeMinutes)s , %(genre_one)s, %(genre_two)s, %(genre_three)s)
        """
        try:
            self.cur.execute(qry, params)
        except errors.UniqueViolation as err:
            print("[ERROR] : Entry already exists")
            print(err)
            self.logger.append(err)
            self.conn.rollback()
        else:
            self.conn.commit()
    
    def read_movie_ratings(self):
        tsv_file = open(path+"\\title.ratings.tsv\\data.tsv", 'r', encoding='utf-8')
        ratings_tsv = csv.DictReader(tsv_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in ratings_tsv:
            if self.is_movie(row['tconst'][2:]):
                row['tconst'] = row['tconst'][2:]
                self.update_movie_ratings(row)
        tsv_file.close()
    
    def update_movie_ratings(self, params):
        qry = """
            UPDATE movies
            SET 
                ratings = %(averageRating)s,
                votes = %(numVotes)s
            WHERE title_id = %(tconst)s
        """
        try:
            self.cur.execute(qry, params)
        except errors.UniqueViolation as err:
            print("[ERROR] : Unable to update movie")
            print(err)
            self.logger.append(err)
            self.conn.rollback()
        else:
            self.conn.commit()
    
    def is_movie(self, title_id):
        qry = ('SELECT COUNT(1) FROM movies WHERE title_id = {}').format(title_id)
        self.cur.execute(qry)
        return bool(self.cur.fetchall()[0][0])

    def read_names_basic(self):
        tsv_file = open(path + "\\name.basics.tsv\\data.tsv", 'r', encoding='utf-8')
        names_tsv = csv.DictReader(tsv_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in names_tsv:
            professions = row['primaryProfession'].split(',')
            professions = [job.upper() for job in professions]
            jobs = self.check_actor_dups(professions)
            params = dict()
            params['id'] = row['nconst'][2:]

            name = row['primaryName'].split(" ", 1)
            params['first_name'] = name[0]
            if len(name) > 1:
                params['last_name'] = name[1]
            else:
                params['last_name'] = None

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
                elif job == "PRODUCER":
                    params['table_name'] = 'producers'
                    params['id_type'] = 'producer_id'
                    self.create_showbiz_person(params)
        tsv_file.close()

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
            print("[ERROR] : Entry already exists")
            print(err)
            self.logger.append(err)
            self.conn.rollback()
        else:
            self.conn.commit()
    
    def read_title_crew(self):
        """
        Creates the relationship between directors/writers to their respective movie
        """
        tsv_file = open(path + "\\title.crew.tsv\\data.tsv", 'r', encoding='utf-8')
        crew_tsv = csv.DictReader(tsv_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in crew_tsv:
            row = self.clean_row(row)
            if self.is_movie(row['tconst']):
                params = dict()
                params['title_id'] = row['tconst']

                params['table_name'] = 'movie_director_relation'
                params['crew_id_type'] = 'director_id'
                directors = row['directors'].split(',')
                for director in directors:
                    if director not in ['\\N', '']:
                        if self.crew_member_exists("directors", params['crew_id_type'], director):
                            params['crew_id'] = director
                            self.create_movie_crew_relation(params)
                
                params['table_name'] = 'movie_writer_relation'
                params['crew_id_type'] = 'writer_id'
                writers = row['writers'].split(',')
                for writer in writers:
                    if writer not in ['\\N', '']:
                        if self.crew_member_exists("writers", params['crew_id_type'], writer):
                            params['crew_id'] = writer
                            self.create_movie_crew_relation(params)
        tsv_file.close()
    
    def read_principals(self):
        tsv_file = open(path + "\\title.principals.tsv\\data.tsv", 'r', encoding='utf-8')
        principals = csv.DictReader(tsv_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in principals:
            row['tconst'] = re.sub("[^0123456789,]","", row['tconst'])
            row['nconst'] = re.sub("[^0123456789,]","", row['nconst'])
            if self.is_movie(row['tconst']):
                params = dict()
                params['title_id'] = row['tconst']
                if "ACT" in row['category'].upper():
                    params['table_name'] = 'movie_actor_relation'
                    params['crew_id_type'] = 'actor_id'
                    params['crew_id'] = row['nconst']
                    if self.crew_member_exists("actors", params['crew_id_type'], params['crew_id']):
                        self.create_movie_crew_relation(params)
                elif "DIRECT" in row['category'].upper():
                    params['table_name'] = 'movie_director_relation'
                    params['crew_id_type'] = 'director_id'
                    params['crew_id'] = row['nconst']
                    if self.crew_member_exists("directors", params['crew_id_type'], params['crew_id']):
                        self.create_movie_crew_relation(params)
                elif "WRITE" in row['category'].upper():
                    params['table_name'] = 'movie_writer_relation'
                    params['crew_id_type'] = 'writer_id'
                    params['crew_id'] = row['nconst']
                    if self.crew_member_exists("writers", params['crew_id_type'], params['crew_id']):
                        self.create_movie_crew_relation(params)
                elif "PROD" in row['category'].upper():
                    params['table_name'] = 'movie_producer_relation'
                    params['crew_id_type'] = 'producer_id'
                    params['crew_id'] = row['nconst']
                    if self.crew_member_exists("producers", params['crew_id_type'], params['crew_id']):
                        self.create_movie_crew_relation(params)
        tsv_file.close()
    
    def clean_row(self, row:dict):
        for key, value in row.items():
            row[key] = re.sub("[^0123456789,]","", value)
        return row
    
    def crew_member_exists(self, table_name:str, id_type:str, _id:str):
        params = dict()
        params['_id'] = _id
        qry = sql.SQL("SELECT COUNT(1) FROM {table} WHERE {id_type} = %(_id)s").format(
            table=sql.Identifier(table_name),
            id_type=sql.Identifier(id_type),
        )
        self.cur.execute(qry, params)
        return bool(self.cur.fetchall()[0][0])
        
    def create_movie_crew_relation(self, params:dict):
        qry = sql.SQL("""
            INSERT INTO {table}
            (title_id, {crew_id_type})
            VALUES
            (%(title_id)s, %(crew_id)s)
        """).format(
            table=sql.Identifier(params['table_name']),
            crew_id_type=sql.Identifier(params['crew_id_type'])
        )
        try:
            self.cur.execute(qry, params)
        except errors.UniqueViolation as err:
            # print("[ERROR] : Unable to add relation")
            # print(err)
            self.logger.append(err)
            self.conn.rollback()
        else:
            self.conn.commit()
            
    def test_transaction(self):
        qry = """
            INSERT INTO actors
            (actor_id, first_name, last_name, birth_year, death_year)
            VALUES
            ('987987987', 'Mark', 'Leckband', '1987', '2018'),
            ('987987987', 'John', 'Cena', '1987', '2018'),
            ('987987986', 'Christopher', 'Dotto', '1987', '2018')
        """
        try:
            self.cur.execute(qry)
        except Exception as err:
            print("Unable to complete transation : ", err)
            self.conn.rollback()
        
        # Because the transaction failed, none of the entries like Mark Leckband should be in
        qry = "SELECT COUNT(1) FROM actors WHERE actor_id = '987987987'"
        try:
            self.cur.execute(qry)
        except Exception as err:
            print(err)
        result = bool(self.cur.fetchall()[0][0])
        if not result:
            print("Actor ID - 987987987 (Mark Leckband) does not exist in the actors table")
        else:
            print("Actor ID - 987987987 (Mark Leckband) is in the actors table")

if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    db = IMDB_DB(path)
    print(timeit.timeit(db.read_movies_basic, number=1))
    print(timeit.timeit(db.read_movie_ratings, number=1))
    print(timeit.timeit(db.read_names_basic, number=1))
    print(timeit.timeit(db.read_principals, number=1))
    print(timeit.timeit(db.read_title_crew, number=1))
    db.test_transaction()
    if db.logger != []:
        print(db.logger)