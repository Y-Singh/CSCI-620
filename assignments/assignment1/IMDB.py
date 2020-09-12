import psycopg2
import csv
import os

from pdb import set_trace

class IMDB_DB:
    def __init__(self, path):
        self.conn = psycopg2.connect(
            database="postgres", user='postgres', password='admin', host='127.0.0.1', port='5432'
        )
        self.cur = self.conn.cursor(prepared=True)
        self.path = path
        self.movies_query = []
        self.actors_query = []

    def read_movies_basic(self):
        tsv_file = open(path+"\\title.basics.tsv\\data.tsv", 'r', encoding='utf-8')
        movies_tsv = csv.DictReader(tsv_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in movies_tsv:
            if row['titleType'] == "movie":
                if row['isAdult'] == '0':
                    print(row.values())
                    genres = row['genres'].split(',')
                    gs = [None] * 3
                    for i in range(0, len(genres)):
                        gs[i] = genres[i]
                    row.pop('genres', None)
                    row['genre_one'] = gs[0]
                    row['genre_two'] = gs[1]
                    row['genre_three'] = gs[2]
                    row['tconst'] = row['tconst'][2:]
                    self.create_movie_tmp(row)
            
        print(len(self.querys))
        print(self.querys[-1][1])
        
        tsv_file.close()

    def create_movie(
        self,
        title_id:int, p_title:str, o_title:str, region:str, lang:str, release_yr:str, duration:int, 
        ratings:float, votes:int, genre_one:str, genre_two:str=None, genre_three:str=None, 
    ):

        qry = f"""
            INSERT INTO movies 
            (title_id, primary_title, original_title, region, language, release_year, duration, ratings, votes, genre_one, genre_two, genre_three)
            VALUES
            ({title_id}, {p_title}, {o_title}, {region}, {lang}, {release_yr}, {duration}, {ratings}, {votes}, {genre_one}, {genre_two}, {genre_three})
        """
        if len(self.querys) < 100:
            self.querys.append(qry)
        else:
            self.querys.clear()
    
    def create_movie_tmp(self,  params:dict):
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
            for job in professions:
                if job.upper() == "ACTOR" or job.upper() == "ACTRESS":
                    params = row
                    self.create_actor_tmp(params)
                elif job.upper() == "DIRECTOR":
                    


    def create_actor_tmp(self, params:dict):
        qry = """
            INSERT INTO actors
            (actor_id, first_name, last_name, birth_year, death_year)
            VALUES
            (%()s, %()s, %()s, %()s, %()s)
        """
        self.actors_query.append((qry, params))
    


    
    
        


if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    dB = IMDB_DB(path)
    dB.read_movies_basic()
    