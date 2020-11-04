import timeit
import json
import os
from math import isnan
from pandas.core import generic
from pymongo import MongoClient
from jproperties import Properties
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class DataCleaning:
    def __init__(self, path) -> None:
        self.path = path
        self.logger = []

        config = Properties()
        with open(path + '\\config.properties', 'rb') as config_file:
            config.load(config_file)
        self.data_path = config.get("DataPath").data
        
        self.client = MongoClient(config.get("DBUrl").data)
        self.db = self.client[config.get("DB").data]
        self.collections = {'Movies': self.db["Movies"], 'Members':self.db["Members"]}
    
    def open_new_data(self):
        data_list = []
        with open(self.data_path + "\\extra-data.json", encoding='utf-8') as extra_data:
            for line in extra_data:
                data = {}
                for key, value in json.loads(line).items():
                    data[key] = value['value']
                data_list.append(data)
        df = pd.DataFrame(data_list)
        df = df.rename(columns={
            'IMDb_ID' : '_id',
            'box_office_currencyLabel' : 'currency_type',
            'titleLabel' : 'title',
            'distributorLabel' : 'distributor',
            'box_office' : 'box_office_revenue',
            'MPAA_film_ratingLabel' : 'film_rating'
        })
        return df

    def get_new_data_via_title(self):
        df = self.open_new_data()
        df = df.drop(columns=['_id'])
        with_titles = df.copy()
        with_titles = with_titles.dropna(subset=['title'])
        without_titles = df[~df.index.isin(with_titles.index)]
        print("There are {} new data entries that have titles. These entries can be updated".format(len(with_titles)))
        print("There are {} new data entries that do not have any title associated with them. These entries will not be able to update any documents".format(len(without_titles)))
        dups_titles = with_titles.pivot_table(index=['title'], aggfunc='size')
        print("There are {} duplicate new data entries".format(len(dups_titles[dups_titles > 1])))
        with_titles = with_titles.drop_duplicates(subset=['title'], keep='first')
        with_titles = with_titles.astype({'cost': 'float', 'box_office_revenue': 'float'})
        with_titles = with_titles.fillna(9876543210)
        
        titles_with_no_new_info = with_titles.loc[(with_titles['currency_type'] == 9876543210) & (with_titles['cost'] == 9876543210) & (with_titles['distributor'] == 9876543210) & (with_titles['box_office_revenue'] == 9876543210) & (with_titles['film_rating'] == 9876543210)]
        print("Out of {} entries that do have valid titles, {} titles do not have any new data to update.".format(len(with_titles), len(titles_with_no_new_info)))
        update_list = self.dict_remove_nans(with_titles.to_dict(orient='records'))
        update_list = [dic for dic in update_list if len(dic.keys()) > 1]
        print("Data entries that do not have any valid data to be updated have been removed. The total number of documents that have data to be updated is {}".format(len(update_list)))
        self.update_records_via_title(update_list)
        
        print("Pause")
        
    def get_new_data_via_id(self):
        df = self.open_new_data()
        
        df = df.set_index('_id').reset_index()
        df['_id'] = df['_id'].str.replace('tt', '')
        df['_id'] = df['_id'].str.replace('tt', '')
        with_ids = df.copy(deep=True)
        with_ids = with_ids.dropna(subset=['_id']) # Drops rows that do not have an id
        with_ids = with_ids[~with_ids['_id'].str.contains('nm')]
        without_ids = df[~df.index.isin(with_ids.index)]
        
        with_ids = with_ids.drop(columns=['title'])
        with_ids = with_ids.drop_duplicates(subset=['_id'], keep='first')
        with_ids = with_ids.astype({'_id' : 'int', 'cost': 'float', 'box_office_revenue': 'float'})
        with_ids = with_ids.sort_values(by=['_id'])
        with_ids = with_ids.fillna(9876543210)
        
        print("There are {} data entries that have a title id associated with them. These entries may or may not have any valid data to update.".format(len(with_ids)))
        print("There are {} data entries that do not have a title id associated with them. The movies that correspond to these entries in the extra-data will not be updated.".format(len(without_ids)))
        ids_with_no_new_info = with_ids.loc[(with_ids['currency_type'] == 9876543210) & (with_ids['cost'] == 9876543210) & (with_ids['distributor'] == 9876543210) & (with_ids['box_office_revenue'] == 9876543210) & (with_ids['film_rating'] == 9876543210)]
        print("Out of {} entries that do have valid title ids, {} ids do not have any new data to update.".format(len(with_ids), len(ids_with_no_new_info)))
        update_list = self.dict_remove_nans(with_ids.to_dict(orient='records'))
        update_list = [dic for dic in update_list if len(dic.keys()) > 1]
        print("Data entries that do not have any valid data to be updated have been removed. The total number of documents that have data to be updated is {}".format(len(update_list)))
        self.update_records_via_id(update_list)
    
    def update_records_via_id(self, update_list):
        successful = 0
        unsuccessful = 0
        for data in update_list:
            try:
                self.collections['Movies'].update_one(
                    {'_id' : data['_id']},
                    {"$set" : {k:v for k, v in data.items() if k != '_id'}}
                )
            except Exception:
                unsuccessful += 1
            else:
                successful += 1
        print("Had to update {} documents. Was able to successfully update {} documents. There were {} unsuccessful updates".format(len(update_list), successful, unsuccessful))
        
    def update_records_via_title(self, update_list):
        successful = 0
        unsuccessful = 0
        for data in update_list:
            try:
                self.collections['Movies'].update_one(
                    {'title' : data['title']},
                    {"$set" : {k:v for k, v in data.items() if k != 'title'}}
                )
            except Exception:
                unsuccessful += 1
            else:
                successful += 1
        print("Had to update {} documents. Was able to successfully update {} documents. There were {} unsuccessful updates".format(len(update_list), successful, unsuccessful))
    
    def dict_remove_nans(self, dictionaries):
        if isinstance(dictionaries, list):
            # return [{k:v for k,v in dic.items() if (v != np.nan) and (v not in [np.nan])} for dic in dictionaries]
            # return [{k:v for k,v in dic.items() if isinstance(v, str) or (not isinstance(v, str) and not isnan(v))} for dic in dictionaries]
            return [{k:v for k,v in dic.items() if v != 9876543210} for dic in dictionaries]
            
        if isinstance(dictionaries, dict):
            return {k:v for k,v in dictionaries.items() if v != np.nan}

    def query_plots(self):
        def query_one():
            genres_data = []
            genres = self.collections['Movies'].distinct("genres")
            for genre in genres:
                query_one = [
                    {'$unwind' : '$genres'
                    },
                    {'$match': 
                        {'genres': genre}
                    },
                    {'$match':
                        {'numVotes' : { "$gte": 10000}}  
                    }
                ]
                res = self.collections['Movies'].aggregate(query_one)
                avgRatings = [dic['avgRating'] for dic in res if 'avgRating' in dic.keys()]
                genres_data.append(avgRatings)
            fig1, ax1 = plt.subplots()
            ax1.set_title('Five Number Summary of Movies with Number of Votes > 10k')
            ax1.boxplot(genres_data)
            ax1.set_xticklabels(genres)
            plt.show()
            
        def query_two():
            avg_actors_per_gen = []
            genres = self.collections['Movies'].distinct("genres")
            for genre in genres:
                query = [
                    {'$match' : 
                        {'actors' : {'$ne' : None}}
                    },
                    {'$unwind' : '$genres'
                    },
                    {'$match': 
                        {'genres': genre}
                    }
                ]
                res = self.collections['Movies'].aggregate(query)
                res = list(res)
                total_actors = 0
                for movie in res:
                    if 'actors' in movie.keys():
                        total_actors += len(movie['actors'])
                total_movies = len(res)
                avg_actors_per_gen.append(total_actors/total_movies)
            plt.bar(genres, avg_actors_per_gen)
            plt.show()
                
        def query_three():
            x_axis = []
            y_axis = []
            years = self.collections['Movies'].distinct("startYear")
            for year in years:
                query = [
                    {'$match' : 
                        {'startYear' : year}
                    },
                    {'$group' : {
                        '_id' : '$_id',
                        'count' : {'$sum' : 1}
                    }},
                    {'$sort' : {
                        'count' : -1
                    }}
                ]
                res = self.collections['Movies'].aggregate(query)
                x_axis.append(year)
                y_axis.append(len(list(res)))              
            
            plt.plot(x_axis, y_axis)
            plt.title("Number of Movies Produced Each Year")
            plt.show()
            
        runtime = (timeit.timeit(query_one, number=1))
        print("It took {} seconds to perform the first query".format(runtime))
        runtime = (timeit.timeit(query_two, number=1))
        print("It took {} seconds to perform the second query".format(runtime))
        runtime = (timeit.timeit(query_three, number=1))
        print("It took {} seconds to perform the third query".format(runtime))
        
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    d = DataCleaning(path)
    
    #runtime = (timeit.timeit(d.get_new_data_via_id, number=1))
    #print("It took {} seconds to update records via id".format(runtime))
    #runtime = (timeit.timeit(d.get_new_data_via_title, number=1))
    #print("It took {} seconds to update records via title".format(runtime))
    runtime = (timeit.timeit(d.query_plots, number=1))
    print("It took {} seconds to perform all of the plots".format(runtime))