import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from jproperties import Properties
import subprocess
import csv
import timeit

import json
from pdb import set_trace
# Notes
# In MongoDB, a database is not created until it gets content
class Mongo:
    def __init__(self, path):
        self.path = path
        self.logger = []

        config = Properties()
        with open(path + '\\config.properties', 'rb') as config_file:
            config.load(config_file)
        self.data_path = config.get("DataPath").data
        
        self.client = MongoClient(config.get("DBUrl").data)
        self.db = self.client[config.get("DB").data]
        self.collections = {'Movies': self.db["Movies"], 'Members':self.db["Members"]}

        self.set_queries()

    def covert_dtype_to_type(self, d:dict) -> dict:
        for key, value in d.items():
            try:
                d[key] = value.item()
            except AttributeError as ae:
                # If value is already a native Python type then AE will be raised so continue
                continue
        return d
    
    def group_title_id_relationships(self, df, group_col:str, data_list_col:str):
        # ORDER OF THE DF DOES MATTER
        # ORDER SHOULD BE TITLE_ID -> GENRE/PROD/WRIT/DIR
        # First sort the df by the column you want to use to structure the data list col
        # .values then returns the df as a numpy array of 2 x ? where its like [1, 'Documentary'], [1, 'Short']
        # .T transposes the array so now we'll have an array where the title id's are going from left to right and the data under
        # the elements are its genres
        keys, values = df.sort_values(group_col).values.T # Keys are the title_ids, values are the genres
        ukeys, index = np.unique(keys, True) # For the keys, we'll grab all of the unique keys and their index
        arrays = np.split(values, index[1:]) # 
        df2 = pd.DataFrame({group_col:ukeys, data_list_col:[list(a) for a in arrays]})
        return df2

    def agg_mapping_title_id_relationships(self, df, group_col:str, data_list_col:str):
        df2 = df.groupby(group_col).agg(lambda x: tuple(x)).applymap(list).reset_index()
        return df2
        
    def get_actors_map(self):
        start_timer = timeit.default_timer()
        ta = pd.read_csv(self.data_path + "\\title_actor.csv", sep=',', encoding='utf-8')
        ta = self.group_title_id_relationships(ta, group_col='title_id', data_list_col='actor_id')
        ta = ta.set_index('title_id')
        ta = ta.to_dict(orient='index') # Titles are int keys and the list of actors are the values

        for title in ta.keys():
            actors = ta[title]['actor_id']
            del ta[title]['actor_id']
            ta[title]['actors'] = []
            for a in actors:
                ta[title]['actors'].append(
                    {'actor' : int(a)}
                    )
        
        tar = pd.read_csv(self.data_path + "\\actor_title_role.csv", sep=',', encoding='utf-8')
        roles = pd.read_csv(self.data_path + "\\role.csv", sep=',', encoding='utf-8')
        ar = pd.merge(left=tar, right=roles, left_on='role_id', right_on='id', how='left').drop(columns=['id', 'role_id'])
        for row in zip(ar['title_id'], ar['actor_id'], ar['role']):
            for a in range(len(ta[row[0]]['actors'])):
                if ta[row[0]]['actors'][a]['actor'] == int(row[1]):
                    if 'role' not in ta[row[0]]['actors'][a].keys():
                        ta[row[0]]['actors'][a]['role'] = []
                    ta[row[0]]['actors'][a]['role'].append(row[2])

        print("It took {} seconds to create a dict with each actor and their role to its respective movie".format(timeit.default_timer()-start_timer))
        return ta
    
    def merge_actors_to_titles(self, titles):
        ta = self.get_actors_map()
        start_timer = timeit.default_timer()
        for title in titles:
            if title['_id'] in ta.keys():
                title.update(ta[title['_id']])
        print("It took {} seconds to merge actors and their roles to each title".format(timeit.default_timer()-start_timer))
        return titles

    def insert_docs(self):
        start_timer = timeit.default_timer()
        movies = pd.read_csv(self.data_path + "\\title.csv", sep=',', encoding='utf-8', low_memory=False)
        movies = movies.rename(columns={'id' : '_id'})
        org_cols = list(movies.columns)
        
        tg = pd.read_csv(self.data_path + "\\title_genre.csv", sep=',', encoding='utf-8')
        genres = pd.read_csv(self.data_path + "\\genre.csv", sep=',', encoding='utf-8')
        genres = pd.merge(left=genres, right=tg, left_on='genre_id', right_on='genre_id', how='left').drop(columns='genre_id')
        genres = genres[['title_id', 'genre']]

        start_timer = timeit.default_timer()
        genres = self.group_title_id_relationships(genres, group_col='title_id', data_list_col='genre')
        print("It took {} seconds to format genre via numpy".format(timeit.default_timer()-start_timer))

        td = pd.read_csv(self.data_path + "\\title_director.csv", sep=',', encoding='utf-8')
        start_timer = timeit.default_timer()
        td = self.group_title_id_relationships(td, group_col='title_id', data_list_col='director_id')
        print("It took {} seconds to format title_director via numpy".format(timeit.default_timer()-start_timer))

        tw = pd.read_csv(self.data_path + "\\title_writer.csv", sep=',', encoding='utf-8')
        start_timer = timeit.default_timer()
        tw = self.group_title_id_relationships(tw, group_col='title_id', data_list_col='writer_id')
        print("It took {} seconds to format title_writer via numpy".format(timeit.default_timer()-start_timer))

        tp = pd.read_csv(self.data_path + "\\title_producer.csv", sep=',', encoding='utf-8')
        start_timer = timeit.default_timer()
        tp = self.group_title_id_relationships(tp, group_col='title_id', data_list_col='producer_id')
        print("It took {} seconds to format title_producer via numpy".format(timeit.default_timer()-start_timer))

        movies = pd.merge(left=movies, right=genres, left_on='_id', right_on='title_id', how='left').drop(columns='title_id')
        movies = pd.merge(left=movies, right=td, left_on='_id', right_on='title_id', how='left').drop(columns='title_id')
        movies = pd.merge(left=movies, right=tw, left_on='_id', right_on='title_id', how='left').drop(columns='title_id')
        movies = pd.merge(left=movies, right=tp, left_on='_id', right_on='title_id', how='left').drop(columns='title_id')

        movies = movies.fillna('None')

        print("It took {} seconds to merge everything in".format(timeit.default_timer()-start_timer))
        movie_list = self.to_json(movies)
        movie_list = self.merge_actors_to_titles(movie_list)
        self.import_json(movie_list, self.collections['Movies'])
        
        members = pd.read_csv(self.data_path + "\\member.csv", sep=',', encoding='utf-8')
        members = members.rename(columns={'id' : '_id'})
        members_list = self.to_json(members)
        self.import_json(members_list, self.collections['Members'])        

    def to_json(self, df):
        start_timer = timeit.default_timer()
        json_list = json.loads(df.to_json(orient='records', indent=3))
        cleaned_json = [{k:v for k,v in dic.items() if (v != 'None' and v != ['None'])} for dic in json_list]
        print("It took {} seconds to covert the dataframe to json and removed keys with None".format(timeit.default_timer()-start_timer))
        return cleaned_json
    
    def import_json(self, docs, col):
        start_timer = timeit.default_timer()
        if isinstance(docs, list):
            col.insert_many(docs)
        else:
            col.insert_one(docs)
        print("It took {} seconds to load the json file to the collection".format(timeit.default_timer()-start_timer))

    def queries_err(self):
        # qry_one = [
        #     {'$match' : {
        #         'startYear' : {'$ne' : '2014'}
        #     }},
        #     {'$match' : {
        #         'titleType' : 'movie'
        #     }},
        #     {'$unwind' : '$actors',
        #     },
        #     {'$lookup' : {
        #         'from' : 'Members',
        #         'localField' : ' actors.actor',
        #         'foreignField' : '_id',
        #         'as' : 'actors'
        #     }},
        #     {'$match' :
        #         {'actors.deathYear' : None}
        #     },
        #     {'$match' :
        #         {'actors.name' : { "$regex": "^Phi"}}
        #     }            
        # ]
        # start_timer = timeit.default_timer()
        # res = self.collections['Movies'].aggregate(qry_one)
        # print("It took {} seconds to for query (2.1)".format(timeit.default_timer()-start_timer))
        # for x in res:
        #     print(x)

        qry_five = [
            {'$unwind' : '$genre'
            },
            {'$match': 
                {'genre': "Sci-Fi"}
            },
            {'$lookup' : {
                'from' : 'Members',
                'localField': 'director_id',
                'foreignField' : '_id',
                'as' : 'directors'
            }},
            {'$match': 
                {'directors.name': 'James Cameron'}
            },
            {'$unwind' : '$actors'
            },
            {'$unwind' : '$actors.actor'
            },
            {'$lookup' : {
                'from' : 'Members',
                'localField': 'actors.actor',
                'foreignField' : '_id',
                'as' : 'acts'
            }},
            {'$unwind' : '$acts'
            },
            {'$match': 
                {'acts.name': 'Sigourney Weaver'}
            },
        ]
        start_timer = timeit.default_timer()
        res = self.collections['Movies'].aggregate(qry_five)
        res = list(res)
        print("It took {} seconds to for query (2.2)".format(timeit.default_timer()-start_timer))
        for x in range(len(res)):
            print(res[x])
    
    def set_queries(self):
        self.qry_two = [
            {'$match' :
                {'startYear' : {'$eq' : '2017'}}
            },
            {'$unwind' : '$genre'
            },
            {'$match': 
                {'genre': "Talk-Show"}
            },
            {'$lookup' : {
                'from' : 'Members',
                'localField': 'producer_id',
                'foreignField' : '_id',
                'as' : 'producers'
            }},
            {'$unwind' : '$producers'
            },
            {'$match': 
                {'producers.name': { "$regex": '.*Gill.*'}}
            },
            {'$group' : {
                '_id' : '$producers._id',
                'name' : {'$first' : '$producers.name'},
                'count' : {'$sum' : 1}
            }},
            {'$match': 
                {'count': { "$gte": 50}}
            }
        ]
        self.qry_three = [
            {'$match' : 
                {'titleType' : 'movie'}
            },
            {'$lookup' : {
                'from' : 'Members',
                'localField': 'writer_id',
                'foreignField' : '_id',
                'as' : 'writers'
            }},
            {'$match': 
                {'writers.name': { "$regex": '.*Bhardwaj.*'}}
            },
            {'$match': 
                {'writers.deathYear': None}
            },
            {'$group' : {
                '_id' : None,
                'avgRuntime' : {'$avg' : '$runtimeMinutes'}
            }}
        ]
        self.qry_four = [
            {'$match' : 
                {'runtimeMinutes' : {'$gte' : 120}}
            },
            {'$lookup' : {
                'from' : 'Members',
                'localField': 'producer_id',
                'foreignField' : '_id',
                'as' : 'producers'
            }},
            {'$match': 
                {'producers.deathYear': None}
            },
            {'$group' : {
                '_id' : '$producers._id',
                'name' : {'$first' : '$producers.name'},
                'count' : {'$sum' : 1}
            }},
            {'$sort' : {
                'count' : -1
            }}
        ]

    def queries(self):
        
        start_timer = timeit.default_timer()
        res = self.collections['Movies'].aggregate(self.qry_two)
        print("It took {} seconds to for query (2.2)".format(timeit.default_timer()-start_timer))
        res = list(res)
        for x in range(len(res)):
            print(res[x])
        exp = self.db.command('aggregate', 'Movies', pipeline=self.qry_two, explain=True)
        self.dump_explain(exp, 'qry2_2_expl')

        start_timer = timeit.default_timer()
        res = self.collections['Movies'].aggregate(self.qry_three)
        print("It took {} seconds to for query (2.3)".format(timeit.default_timer()-start_timer))
        res = list(res)
        for x in range(len(res)):
            print(res[x])
        exp = self.db.command('aggregate', 'Movies', pipeline=self.qry_three, explain=True)
        self.dump_explain(exp, 'qry2_3_expl')
        
        start_timer = timeit.default_timer()
        res = self.collections['Movies'].aggregate(self.qry_four)
        print("It took {} seconds to for query (2.4)".format(timeit.default_timer()-start_timer))
        res = list(res)
        for x in range(0, 10):
            print(res[x])
        exp = self.db.command('aggregate', 'Movies', pipeline=self.qry_four, explain=True)
        self.dump_explain(exp, 'qry2_4_expl')

    def dump_explain(self, exp, qry_name):
        with open("{}.json".format(qry_name), 'w') as f:
            json.dump(exp, f, indent=1)
    
    def create_indexes(self):
        movie_index = 'genre'
        members_index = 'name'
        self.collections['Movies'].create_index([(movie_index, 1)])
        self.collections['Movies'].create_index([('startYear', -1)])
        self.collections['Members'].create_index(members_index)
        start_timer = timeit.default_timer()
        res = self.collections['Movies'].aggregate(self.qry_two)
        print("It took {} seconds to for query (2.2)".format(timeit.default_timer()-start_timer))
        self.collections['Movies'].drop_indexes()
        self.collections['Members'].drop_indexes()

        movie_index = 'titleType'
        members_index = 'name'
        self.collections['Movies'].create_index([(movie_index, 1)])
        self.collections['Members'].create_index(members_index)
        start_timer = timeit.default_timer()
        res = self.collections['Movies'].aggregate(self.qry_three)
        print("It took {} seconds to for query (2.3)".format(timeit.default_timer()-start_timer))
        self.collections['Movies'].drop_indexes()
        self.collections['Members'].drop_indexes()

        movie_index = 'genre'
        members_index = 'name'
        self.collections['Movies'].create_index([(movie_index, 1)])
        self.collections['Movies'].create_index([('startYear', -1)])
        self.collections['Members'].create_index(members_index)
        self.collections['Members'].create_index([('deathYear', -1)])
        start_timer = timeit.default_timer()
        res = self.collections['Movies'].aggregate(self.qry_four)
        print("It took {} seconds to for query (2.4)".format(timeit.default_timer()-start_timer))
        self.collections['Movies'].drop_indexes()
        self.collections['Members'].drop_indexes()

if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    mdb = Mongo(path)
    mdb.queries_err()
