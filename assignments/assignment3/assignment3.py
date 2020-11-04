import pandas as pd
import csv
import os
import timeit
from itertools import combinations
import json

class Assignment3:
    def __init__(self, path):
        self.path = path
        self.logger = []
        self.func_dep = []

    def create_new_relation(self):
        """Creates a new CSV that will represent the new relation which is the result of
        joining Movie, Movie_Genre, Genre, Member, Movie_Actor, Actor_Movie_Role. 

        Return:
            pandas.DataFrame.to_csv() : Columns = {
                movie_id,
                type,
                startYear,
                runtime,
                avgRating,
                genreID,
                memberID,
                birthYear,
                role
            }
        """
        cols = ['id', 'titleType', 'startYear','runtimeMinutes','averageRating']
        movies = pd.read_csv(self.path + "\\data\\title.csv", usecols=cols, sep=",", encoding="utf-8", low_memory=False)
        movies = movies[movies['runtimeMinutes'] >= 90]
        movies = movies.loc[movies['titleType'] == 'movie']
        amr = pd.read_csv(self.path + "\\data\\actor_title_role.csv", sep=",", encoding="utf-8", low_memory=False)
        actor_movie_role = None

        dups = amr[amr.duplicated(['title_id', 'actor_id'])]
        non_dups = amr[~amr['title_id'].isin(dups['title_id'])]
        tbl = pd.merge(left=movies, right=non_dups, left_on='id', right_on='title_id', how='inner').drop(columns=['title_id'])

        
        genre_ids = pd.read_csv(self.path + "\\data\\title_genre.csv", sep=',', encoding='utf-8')
        tbl = pd.merge(left=tbl, right=genre_ids, left_on='id', right_on='title_id', how='left').drop(columns=['title_id'])
        genre = pd.read_csv(self.path + "\\data\\genre.csv", sep=',', encoding='utf-8')
        tbl = pd.merge(left=tbl, right=genre, left_on='genre_id', right_on='genre_id', how='left')

        members = pd.read_csv(self.path + "\\data\\member.csv", sep=',', encoding='utf-8')
        tbl = pd.merge(left=tbl, right=members, left_on='actor_id', right_on='id', how='left').drop(columns=['id_y', 'name', 'deathYear'])

        roles = pd.read_csv(self.path + "\\data\\role.csv", sep=',', encoding='utf-8')
        tbl = pd.merge(left=tbl, right=roles, left_on='role_id', right_on='id', how='left').drop(columns=['id'])
        tbl = tbl.drop(columns=['role_id'])
        tbl = tbl.rename(columns={'id_x':'movieId', 'titleType':'type', 'actor_id':'memberId', })
        
        tbl.to_csv(self.path + "\\data\\tbl.csv", encoding='utf-8', index=False, sep=',')

    
    def check_func_dep_naiv(self):
        tbl = pd.read_csv(self.path + "\\data\\tbl.csv", sep=",", encoding="utf-8", low_memory=False)
        for left in tbl:
            for right in tbl:
                if left != right:
                    func_dep = True
                    if func_dep:
                        for index, row in tbl.iterrows():
                            for index_2, row_2 in tbl.iterrows():
                                left_match = row[left] == row_2[left]
                                if left_match:
                                    right_match = row[right] == row_2[right]
                                    if right_match:
                                        continue
                                    else:
                                        func_dep = False
                                        break
                            else:
                                continue
                            break
                    if func_dep:
                        self.func_dep.append(tuple((left, right)))


    def check_func_dep_prune(self):
        tbl = pd.read_csv(self.path + "\\data\\tbl.csv", sep=",", encoding="utf-8", low_memory=False)
        doubles = [i for i in list(combinations(tbl.columns, 2))]
        parts = {}
        start_time = timeit.default_timer()
        
        for col in tbl:
            parts[col] = {key : list(tbl[col].loc[tbl[col] == key].index) for key in tbl[col].unique()}
        print("It took {} seconds to calc the equivalent classes".format(str(timeit.default_timer() - start_time)))

        for left in tbl:
            for right in tbl:
                if right != left:
                    sub_sets = []
                    for right_lbl, right_idxs in parts[right].items():
                        for left_lbl, left_idxs in parts[left].items():
                            if (set(left_idxs).issubset(set(right_idxs))):
                                if left_lbl not in sub_sets:
                                    sub_sets.append(left_lbl)
                    left_keys = list(parts[left].keys())
                    if left_keys in sub_sets:
                        self.func_dep.append(tuple((left, right)))
                        left_combos = (list(filter(lambda x: x[0] == left or x[1] == left, doubles)))
                        for cmb in left_combos:
                            self.func_dep.append(cmb + tuple((right,)))
                        
        print("Finished Single Columns")

     
    def save_func_deps(self):
        with open('functional_deps.txt', 'w') as fp:
            for fd in self.func_dep:
                fp.write(fd)
        

if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    a = Assignment3(path)
    # a.create_new_relation()
    runtime = (timeit.timeit(a.check_func_dep_prune, number=1))
    print("It took {} seconds to find the functional dependencies via Pruning with Single Left".format(runtime))
    
    a.save_func_deps()
