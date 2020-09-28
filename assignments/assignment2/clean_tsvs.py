import os
import threading
import pandas as pd
import timeit
import csv
import winsound

# https://medium.com/@sureshssarda/pandas-splitting-exploding-a-column-into-multiple-rows-b1b1d59ea12e
# https://stackoverflow.com/questions/34682828/extracting-specific-selected-columns-to-new-dataframe-as-a-copy
# https://datacarpentry.org/python-ecology-lesson/05-merging-data/index.html
# https://towardsdatascience.com/how-to-change-datatypes-in-pandas-in-4-minutes-677addf9a409

class Cleaning:
    def __init__(self, path):
        self.path = path
        self.logger = []

    def clean_titles(self):
        titles_tsv_df = pd.read_csv(self.path + "\\title.basics.tsv\\data.tsv", sep="\t", encoding="utf-8", low_memory=False)
        titles_tsv_df = self.merge_ratings(titles_tsv_df)
        # self.parse_genres(titles_tsv_df[['tconst', 'genres']].copy())
        g = threading.Thread(
            target = self.parse_genres,
            args = (titles_tsv_df[['tconst', 'genres']].copy(),)
        )
        g.start()
        titles_tsv_df = titles_tsv_df.drop(columns=['isAdult', 'genres'])
        titles_tsv_df = titles_tsv_df.rename(columns={
            'tconst': 'id',
        })
        titles_tsv_df['runtimeMinutes'] = pd.to_numeric(titles_tsv_df.runtimeMinutes.astype(str), errors='coerce')
        titles_tsv_df['runtimeMinutes'] = titles_tsv_df['runtimeMinutes'].astype('Int64')
        titles_tsv_df['id'] = titles_tsv_df['id'].str.replace('tt', '')
        self.write_df_to_csv(titles_tsv_df, "title.csv")
        g.join()

    def merge_ratings(self, titles_tsv_df):
        ratings_tsv_df = pd.read_csv(self.path + "\\title.ratings.tsv\\data.tsv", sep="\t", encoding='utf-8')
        df = pd.merge(left=titles_tsv_df, right=ratings_tsv_df, how='outer', left_on='tconst', right_on='tconst')
        df['numVotes'] = df['numVotes'].astype('Int64')
        return df
    
    def parse_genres(self, genres):
        genres = genres.fillna('')
        genres_df = pd.DataFrame(genres.genres.str.split(',').tolist(), index=genres.tconst).stack()
        genres_df = genres_df.reset_index([0, 'tconst'])
        genres_df.columns = ['title_id', 'genre']
        genre_ids = pd.DataFrame(genres_df.genre.unique())

        genre_ids.columns = ['genre']
        genre_ids = genre_ids[genre_ids.genre != "\\N"]
        genre_ids = genre_ids[genre_ids.genre != ""]

        genre_ids = genre_ids.reset_index()
        genre_ids = genre_ids.drop(columns=['index'])
        genre_ids = genre_ids.reset_index()
        genre_ids.columns = ['genre_id', 'genre']
        genre_ids['genre_id'] = genre_ids['genre_id'].astype('str')

        genres_df = pd.merge(left=genres_df, right=genre_ids, left_on='genre', right_on='genre', how='left')
        genres_df = genres_df.drop(columns=['genre'])
        genres_df = genres_df.dropna()
        genres_df['title_id'] = genres_df['title_id'].str.replace('tt', '')
        
        self.write_df_to_csv(genre_ids, "genre.csv")
        self.write_df_to_csv(genres_df, "title_genre.csv")
           
    def clean_names(self):
        names_tsv_df = pd.read_csv(self.path + "\\name.basics.tsv\\data.tsv", sep="\t", encoding='utf-8', low_memory=False)
        # ************** Uncomment this if using first_name, last_name instead of name ************** #
        # names_tsv_df[['firstName', 'lastName']] = names_tsv_df['primaryName'].loc[names_tsv_df['primaryName'].str.split().str.len() == 2].str.split(expand=True)
        # names_tsv_df['firstName'].fillna(names_tsv_df['primaryName'], inplace=True)
        # ******************************************************************************************* #
        names_tsv_df = names_tsv_df.drop(columns=['primaryProfession', 'knownForTitles'])
        names_tsv_df = names_tsv_df.rename(columns={
            'nconst': 'id',
            'primaryName': 'name'
        })
        names_tsv_df['id'] = names_tsv_df['id'].str.replace('nm', '')
        self.write_df_to_csv(names_tsv_df, "member.csv")
    
    def clean_principals(self):
        jobs_tsv_df = pd.read_csv(self.path + "\\title.principals.tsv\\data.tsv", sep="\t", encoding='utf-8', low_memory=False)
        jobs_tsv_df = jobs_tsv_df.rename(columns={
            'tconst': 'title_id',
        })
        jobs_tsv_df['title_id'] = jobs_tsv_df['title_id'].str.replace('tt', '')
        jobs_tsv_df['nconst'] = jobs_tsv_df['nconst'].str.replace('nm', '')
        actors_roles = jobs_tsv_df[(jobs_tsv_df['category'] == 'actor') | (jobs_tsv_df['category'] == 'actress')]
        jobs_tsv_df = jobs_tsv_df.drop(columns=['ordering', 'job', 'characters'])
        
        directors = jobs_tsv_df[jobs_tsv_df.category == 'director']
        writers = jobs_tsv_df[jobs_tsv_df.category == 'writer']
        producers = jobs_tsv_df[jobs_tsv_df.category == 'producer']
        actors = jobs_tsv_df[(jobs_tsv_df['category'] == 'actor') | (jobs_tsv_df['category'] == 'actress')]
        
        del jobs_tsv_df

        directors = directors.drop(columns=['category'])
        directors = directors.rename(columns={
            'nconst': 'director_id'
        })
        writers = writers.drop(columns=['category'])
        writers = writers.rename(columns={
            'nconst': 'writer_id'
        })
        producers = producers.drop(columns=['category'])
        producers = producers.rename(columns={
            'nconst': 'producer_id'
        })
        actors = actors.drop(columns=['category'])
        actors = actors.rename(columns={
            'nconst': 'actor_id'
        })
        
        d = threading.Thread(target=self.write_df_to_csv, args=(directors, "title_director.csv",))
        w = threading.Thread(target=self.write_df_to_csv, args=(writers, "title_writer.csv",))
        p = threading.Thread(target=self.write_df_to_csv, args=(producers, "title_producer.csv",))
        a = threading.Thread(target=self.write_df_to_csv, args=(actors, "title_actor.csv",))
        d.start()
        w.start()
        p.start()
        a.start()
        
        self.parse_characters(actors_roles)
        d.join()
        w.join()
        p.join()
        a.join()

    def parse_characters(self, actors_roles):
        actors_roles['characters'] = actors_roles.characters.str.replace('[', '')
        actors_roles['characters'] = actors_roles.characters.str.replace(']', '')
        actors_roles['characters'] = actors_roles.characters.str.replace('"', '')
        actors_roles = actors_roles.drop(columns=['ordering', 'category', 'job'])
        actors_roles = actors_roles.assign(characters=actors_roles.characters.str.split(",")).explode('characters')

        unique_roles = pd.DataFrame(actors_roles.characters.unique())
        unique_roles.columns = ['role']
        unique_roles = unique_roles[unique_roles.role != "\\N"]
        unique_roles = unique_roles.reset_index()
        unique_roles = unique_roles.drop(columns=['index'])
        unique_roles = unique_roles.reset_index()
        unique_roles.columns = ['id', 'role']
        unique_roles['id'] = unique_roles['id'].astype('str')
        # unique_roles['role'] = unique_roles.role.str.replace('"', '')

        actors_roles = pd.merge(left=actors_roles, right=unique_roles, left_on='characters', right_on='role', how='left')
        actors_roles = actors_roles.drop(columns=['characters', 'role'])
        actors_roles = actors_roles.dropna()
        actors_roles = actors_roles.rename(columns={'nconst': 'actor_id', 'id': 'role_id'})
        self.write_df_to_csv(unique_roles, "role.csv")
        self.write_df_to_csv(actors_roles, "actor_title_role.csv")

    def clean_dataframes(self, df):
        df = df.replace('\\N', "None")
        df = df.replace('', "None")
        df = df.replace()
        df = df.fillna("None")
        return df

    def write_df_to_csv(self, df, csv_name):
        df = self.clean_dataframes(df)
        df.to_csv(self.path + "\\data\\" + csv_name, encoding='utf-8', index=False, sep=',')


if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    c = Cleaning(path)
    
    runtime = timeit.timeit(c.clean_titles, number=1)
    print("It took {} seconds to clean and parse out title information".format(runtime))
    runtime = timeit.timeit(c.clean_names, number=1)
    print("It took {} seconds to clean and parse out members".format(runtime))
    runtime = timeit.timeit(c.clean_principals, number=1)
    print("It took {} seconds to clean and parse out member informations and relationships".format(runtime))

    if len(c.logger) > 0:
        print(c.logger)
    
    winsound.Beep(440, 1000)