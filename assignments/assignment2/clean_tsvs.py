import csv
import os
import threading
import pandas as pd
import timeit
from pdb import set_trace

class Cleaning:
    def __init__(self, path):
        self.path = path

        self.titles_fnames = ['id', 'titleType', 'primaryTitle', 'originalTitle', 'startYear', 'endYear', 'runtimeMinutes', 'averageRating', 'numVotes']
        
        self.titles_csv_writer = None
        self.ratings_tsv_reader = None
        self.genre_csv_writer = None
        self.ratings_tsv_df = None

    def open_files(self):
        self.titles_tsv = open(self.path + "\\title.basics.tsv\\data.tsv", 'r', encoding='utf-8')
        self.titles_csv = open(self.path + "\\data\\titles.csv", "w", newline="", encoding='utf-8')
        self.genres_csv = open(self.path + "\\data\\genres.csv", "w", newline="", encoding='utf-8')
        self.names_tsv = open(self.path + "\\name.basics.tsv\\data.tsv", 'r', encoding='utf-8')
        self.members_csv = open(self.path + "\\data\\members.csv", "w", newline="", encoding='utf-8')
    
    def close_files(self):
        self.titles_tsv.close()
        self.titles_csv.close()
        self.genres_csv.close()

    def clean_titles_basics(self):
        titles_tsv = csv.DictReader(self.titles_tsv, delimiter="\t", quoting=csv.QUOTE_NONE)
        # All keys = tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
        # Unwanted keys = isAdult
        # (id, type, title, original_title, startYear, endYear, runtime, avgRating, numVotes)
        for row in titles_tsv:
            if row['isAdult'] == '0':
                row.pop('isAdult')
                row = self.get_ratings_votes(row)
                row = self.pop_genres_csv(row)
                row['id'] = row.pop('tconst')
                self.pop_titles_csv(row)

    def get_ratings_votes(self, data:dict):
        if self.ratings_tsv_df is None:
            self.ratings_tsv_df = pd.read_csv(self.path + "\\title.ratings.tsv\\data.tsv", sep="\t", encoding='utf-8')
        try:
            r = (self.ratings_tsv_df.loc[self.ratings_tsv_df['tconst'] == data['tconst']]).to_dict(orient='records')[0]
            r.pop('tconst')
        except IndexError as err:
            r = dict()
            r['averageRating'] = "NULL"
            r['numVotes'] = "NULL"        
        data.update(r)
        return data
    
    def pop_genres_csv(self, data:dict):
        if self.genre_csv_writer is None:
            genres_fnames = ['tconst', 'genre']
            self.genre_csv_writer = csv.DictWriter(self.genres_csv, fieldnames=genres_fnames)
            self.genre_csv_writer.writeheader()
        row = dict()
        row['tconst'] = data['tconst']
        genres = data['genres'].split(',')
        for g in genres:
            row['genre'] = g
            self.genre_csv_writer.writerow(row)
        data.pop('genres')
        return data

    def pop_titles_csv(self, data:dict):
        if self.titles_csv_writer is None:
            self.titles_csv_writer = csv.DictWriter(self.titles_csv, fieldnames=self.titles_fnames)
            self.titles_csv_writer.writeheader()
        data = self.clean_data(data)
        self.titles_csv_writer.writerow(data)

    def clean_data(self, data:dict):
        for key, value in data.items():
            if data[key] == "\\N":
                data[key] = "NULL"
        return data
    
    def clean_names_basics(self):
        names_tsv = csv.DictReader(self.names_tsv, delimiter="\t",  quoting=csv.QUOTE_NONE)
        for row in names_tsv:
            row.pop('primaryProfession')
            row.pop('knownForTitles')
            row['id'] = self.pop_members_csv(row)
    
    def pop_members_csv(self, data:dict):
        if self.members_csv_writer is None:
            fnames = ['id', 'primaryName', 'birthYear', 'deathYear']
            self.members_csv_writer = csv.DictWriter(self.members_csv, fieldnames=fnames)
            self.members_csv_writer.writeheader()
        data = self.clean_data(data)
        self.members_csv_writer.writerow(data)

        
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    c = Cleaning(path)
    c.open_files()
    print(timeit.timeit(c.clean_titles_basics, number=1))
    c.close_files()
    # c.reading_ratings()