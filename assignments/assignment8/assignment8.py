import os
import random
from math import sqrt
from pymongo import MongoClient
from jproperties import Properties
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import timeit
import pandas as pd
from pdb import set_trace
class Clustering:
    def __init__(self, path):
        self.path = path
        self.logger = []

        config = Properties()
        with open(path + '\\config.properties', 'rb') as config_file:
            config.load(config_file)
        self.data_path = config.get("DataPath").data
        
        self.client = MongoClient(config.get("DBUrl").data)
        self.db = self.client[config.get("DB").data]
        self.collections = {'Movies': self.db["Movies"], 'Members':self.db["Members"], 'K-Means':self.db["Centroids"]}
        
    def kmeans_norm(self):
        res = self.get_movies()
        self.update_docs(res, self.collections['Movies'])

    def get_movies(self):
        movies_qry = [
            {
                '$match' : {
                    '$and' : [
                         {'type' : 'movie'},
                         {'numVotes' : {'$gte' : 10000}},
                         {'startYear' : {'$ne' : None}},
                         {'avgRating' : {'$ne' : None}}
                    ]
                },
            }
        ]
        res = list(self.collections['Movies'].aggregate(movies_qry))
        start_years = [x['startYear'] for x in res]
        avg_ratings = [x['avgRating'] for x in res]
        min_start_year = min(start_years)
        max_start_year = max(start_years)
        
        min_avg_ratings = min(avg_ratings)
        max_avg_ratings = max(avg_ratings)
        for x in res:
            x['kmeansNorm'] = [
                ((x['startYear'] - min_start_year) / (max_start_year - min_start_year)),
                ((x['avgRating'] - min_avg_ratings) / (max_avg_ratings - min_avg_ratings))
            ]
        return res
    
    def update_docs(self, update_list, collection):
        successful = 0
        unsuccessful = 0
        for doc in update_list:
            try:
                collection.replace_one({'_id' : doc['_id']}, doc)
            except Exception:
                unsuccessful += 1
            else:
                successful += 1
        print("Had to update {} documents. Was able to successfully update {} documents. There were {} unsuccessful updates".format(len(update_list), successful, unsuccessful))
    
    def clustering(self):
        genres = ['Action', 'Horror', 'Romance', 'Sci-Fi', 'Thriller']
        sqr_sum_errors = {}
        for g in genres:
            sqr_sum_errors[g] = {}
            for k in range(10, 55, 5):
                self.centroids(k, g)
                for i in range(0, 100):
                    self.cluster(g)
                sse = 0.0
                centroids = list(self.collections['Centroids'].find({}))
                for c in centroids:
                    get_docs = [
                        {
                            '$match' : {
                                '$and' : [
                                    {'type' : 'movie'},
                                    {'numVotes' : {'$gte' : 10000}},
                                    {'startYear' : {'$ne' : None}},
                                    {'avgRating' : {'$ne' : None}},
                                    {'genres' : g},
                                    {'cluster' : c['_id']},
                                ]
                            }
                        }
                    ]
                    documents = list(self.collections['Movies'].aggregate(get_docs))
                    for doc in documents:
                        euclian_distance = sqrt(pow(doc['kmeansNorm'][0] - c['kmeansNorm'][0], 2) + pow(doc['kmeansNorm'][1] - c['kmeansNorm'][1], 2))
                        sse += (euclian_distance ** 2)
                sqr_sum_errors[g][k] = sse
                print("Finished calculating SSE for K = {}".format(k))
            
        for genre in sqr_sum_errors.keys():
            x = list(sqr_sum_errors[genre].keys())
            y = list(sqr_sum_errors[genre].values())
            fig, ax = plt.subplots()
            ax.plot(x, y)
            ax.grid()
            plt.show()
                        
    def centroids(self, k:int, g:str):
        self.collections['Centroids'].delete_many({})
        qry = [
            {
                '$match' : {
                    '$and' : [
                         {'genres' : g},
                         {'kmeansNorm' : {'$ne' : None}}
                    ]
                }
            },
            {
                '$sample' : {
                    'size' : k
                }
            }
        ]
        res = list(self.collections['Movies'].aggregate(qry))
        new_docs = []
        for x in res:
            new_docs.append({'kmeansNorm' : x['kmeansNorm']})
        for index, doc in enumerate(new_docs, 1):
            doc['_id'] = index
        self.collections['Centroids'].insert_many(new_docs)
        
    def cluster(self, g:str):
        get_movies = [
            {
                '$match' : {
                    '$and' : [
                        {'type' : 'movie'},
                        {'numVotes' : {'$gte' : 10000}},
                        {'startYear' : {'$ne' : None}},
                        {'avgRating' : {'$ne' : None}},
                        {'genres' : g},
                        {'kmeansNorm' : {'$ne' : None}},
                    ]
                }
            }
        ]
        res = list(self.collections['Movies'].aggregate(get_movies))
        centroids = list(self.collections['Centroids'].find({}))
        cluster_counts = {}
        for c in centroids:
            cluster_counts[c['_id']] = {}
            cluster_counts[c['_id']]['count'] = 0
            cluster_counts[c['_id']]['total_start_year'] = 0
            cluster_counts[c['_id']]['total_avg_rating'] = 0
            
        for x in res:
            euclian_distance = 10.0
            _id = 0
            for c in centroids:
                new_euclian_distance = sqrt(pow(x['kmeansNorm'][0] - c['kmeansNorm'][0], 2) + pow(x['kmeansNorm'][1] - c['kmeansNorm'][1], 2))
                if new_euclian_distance < euclian_distance:
                    euclian_distance = new_euclian_distance
                    _id = c['_id']
            x['cluster'] = _id
            self.collections['Movies'].replace_one({'_id':x['_id']}, x)
            cluster_counts[_id]['count'] += 1
            cluster_counts[_id]['total_start_year'] += x['kmeansNorm'][0]
            cluster_counts[_id]['total_avg_rating'] += x['kmeansNorm'][1]
        
        for c in cluster_counts:
            if cluster_counts[c]['count'] != 0:
                for centroid in centroids:
                    if centroid['_id'] == c:
                        centroid['kmeansNorm'] = [
                            (cluster_counts[c]['total_start_year'] / cluster_counts[c]['count']),
                            (cluster_counts[c]['total_avg_rating'] / cluster_counts[c]['count']),
                        ]
            else:
                random_movie = random.choice(res)
                for centroid in centroids:
                    if centroid['_id'] == c:
                        centroid['kmeansNorm'] = random_movie['kmeansNorm']
        self.update_docs(centroids, self.collections['Centroids'])
    
    
    
    def best_clusters(self):
        cluster_data = {}
        genres = ['Action', 'Horror', 'Romance', 'Sci-Fi', 'Thriller']
        for g in genres:
            k = 0
            if g == 'Action':
                k = 35
            elif g == 'Horror':
                k = 30
            elif g == 'Romance':
                k = 40
            elif g == 'Sci-Fi':
                k = 30
            elif g == 'Thriller':
                k = 50
            self.centroids(k, g)
            for i in range(0, 100):
                self.cluster(g)
            centroids = list(self.collections['Centroids'].find({}))
            for c in centroids:
                cluster_data[c['_id']] = []
                cluster_data[c['_id']].append(tuple((c['kmeansNorm'][0], c['kmeansNorm'][1])))
                get_docs = [
                    {
                        '$match' : {
                            '$and' : [
                                {'type' : 'movie'},
                                {'numVotes' : {'$gte' : 10000}},
                                {'startYear' : {'$ne' : None}},
                                {'avgRating' : {'$ne' : None}},
                                {'genres' : g},
                                {'cluster' : c['_id']},
                            ]
                        }
                    }
                ]
                documents = list(self.collections['Movies'].aggregate(get_docs))
                cluster_docs = [tuple((doc['kmeansNorm'][0], doc['kmeansNorm'][1])) for doc in documents][0:25]
                cluster_data[c['_id']].append(cluster_docs)
            
            groups = {}
            for key in cluster_data.keys():
                groups[key] = cluster_data[key][0]
            data = pd.DataFrame(columns = ['x', 'y', 'label'])
            for c in cluster_data.keys():
                for cluster in cluster_data[c][1]:
                    data = data.append(
                        {'x':cluster[0], 'y':cluster[1], 'label':c}, ignore_index=True
                    )
                plt.scatter(x=data.loc[data['label']== c, 'x'],
                            y=data.loc[data['label']== c, 'y'],
                            alpha=1)
                plt.annotate(c,
                            data.loc[data['label']==c, ['x', 'y']].mean(),
                            horizontalalignment='center',
                            verticalalignment='center',
                            size=20, weight='bold',
                            color='black')            
            plt.show()
                        
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    c = Clustering(path)
    runtime = timeit.timeit(c.best_clusters, number=1)
    print('It took {} seconds to do Q4'.format(runtime))
