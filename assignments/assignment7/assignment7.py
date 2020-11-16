import pandas as pd
import numpy as np
from psycopg2 import connect, errors
import timeit
import os

import psycopg2

class Itemset:
    def __init__(self, path) -> None:
        self.path = path
        self.logger = []
        self.conn = connect(
            database="assignment2", user='postgres', password='admin', host='127.0.0.1', port='5432'
        )
        self.cur = self.conn.cursor()
        
    def popular_movie_actors(self):
        qry = """
            CREATE TABLE Popular_Movie_Actors
            AS 
            SELECT ta.*	
            FROM title_actor ta
            JOIN title t
                ON ta.titleid = t.id
            WHERE (
                (t.avgratings > 5) AND
                (t.type = 'movie')
        )
        """
        try:
            self.cur.execute(qry)
        except psycopg2.errors.DuplicateTable:
            print("Table already exists")
        else:
            self.conn.commit()
    
    def item_set_size_one(self):
        qry = """
            CREATE TABLE L1 AS
            select pma1.actorid as actor1, count (*) num_of_movies
            from popular_movie_actors pma1
            group by pma1.actorid
            having count(*) > 5
            order by count (*) desc
        """
        try:
            self.cur.execute(qry)
        except psycopg2.errors.DuplicateTable:
            print("Table already exists")
        else:
            self.conn.commit()
    
    def item_set_size_two(self):
        qry = """
            CREATE TABLE L2 AS
            select pma1.actorid as actor1, pma2.actorid as actor2, m.name, count(*) num_of_movies_together
            from popular_movie_actors pma1
            inner join
            popular_movie_actors pma2
            on
                pma1.titleid = pma2.titleid
                and
                pma1.actorid < pma2.actorid
            join members m
            on
                pma1.actorid = m.id 
                and
                pma2.actorid = m.id
            group by pma1.actorid, pma2.actorid
            having count(*) > 5
            order by count(*) desc
        """
        try:
            self.cur.execute(qry)
        except psycopg2.errors.DuplicateTable:
            print("Table already exists")
        else:
            self.conn.commit()
    
    def item_set_size_three(self):
        qry = """
            CREATE TABLE L3 AS
            select pma1.actorid as actor1, pma2.actorid as actor2, pma3.actorid as actor3, count(*) num_of_movies_together
            from popular_movie_actors pma1
            inner join popular_movie_actors pma2
                on
                pma1.titleid = pma2.titleid
                and
                pma1.actorid < pma2.actorid
            inner join popular_movie_actors pma3
                on
                pma1.titleid = pma3.titleid
                and
                pma1.actorid < pma3.actorid
                and
                pma2.actorid < pma3.actorid
            group by pma1.actorid, pma2.actorid, pma3.actorid
            having count(*) > 5
            order by count(*) desc
        """
        try:
            self.cur.execute(qry)
        except psycopg2.errors.DuplicateTable:
            print("Table already exists")
        else:
            self.conn.commit()
    
    def freq_item_set(self):
        n = 1
        while True:
            qry = self.item_set_size_n_query(n)
            self.cur.execute(qry)
            res = self.cur.fetchall()
            if len(res) > 0:
                print(f"There are {len(res)} items in L{n}")
                self.item_set_size_n(n)
                n += 1
            else:
                break
        print(f"L{n} is an empty latice")
        
        last_non_empty = n-1
        tbl_cols_qry = f"select column_name from information_schema.columns where table_name = 'l{last_non_empty}'"
        self.cur.execute(tbl_cols_qry)
        col_names = self.cur.fetchall()
        col_names = [col for col in col_names if 'actor' in col[0]]
        select = f'select l{last_non_empty}.*, '
        names = ', '.join([f'm{index}.name as actor{index}_name' for index in range(1, len(col_names)+1)])
        joins = ' '.join([f'join member m{index} on l{last_non_empty}.actor{index} = m{index}.id' for index in range(1, len(col_names)+1)])
        qry = select + names + f' from L{last_non_empty} ' + joins
        create = "CREATE TABLE last_non_empty_latice AS " + qry
        try:
            self.cur.execute(create)
        except psycopg2.errors.DuplicateTable:
            print(f"Last Non Empty Latice Table already exists. Skipping...")
        else:
            self.conn.commit()
        
        
    def item_set_size_n_query(self, n):
        qry = ''
        select = ', '.join([f"pma{x}.actorid as actor{x}" for x in range(1, n+1)])
        count = ', count(*) num_of_movies_together'
        qry = 'select ' + select + count + ' from popular_movie_actors pma1'
        if n > 1:
            for x in range(2, n+1):
                qry = qry + f' inner join popular_movie_actors pma{x} on '
                titleids = f'pma1.titleid = pma{x}.titleid and '
                actorids = ' and '.join([f'pma{y}.actorid < pma{x}.actorid' for y in range(1, x)])
                qry = qry + titleids + actorids
        group = 'group by ' + ', '.join([f'pma{x}.actorid' for x in range(1, n+1)])
        having = 'having count(*) > 5'
        order = 'order by count(*) desc'
        qry = ' '.join([qry, group, having, order])
        return qry
    
    def item_set_size_n(self, n):
        qry = self.item_set_size_n_query(n)
        qry = f"CREATE TABLE L{n} as " + qry
        try:
            self.cur.execute(qry)
        except psycopg2.errors.DuplicateTable:
            print(f"Table {n} already exists")
        else:
            self.conn.commit()


        
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    i = Itemset(path)
    i.freq_item_set()
    # runtime = timeit.timeit(i.item_set_size_two, number=1)
    # print(runtime)