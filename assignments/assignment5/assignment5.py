from psycopg2 import errors, connect
import os
import timeit
import winsound


class database:
    def __init__(self, path):
        self.conn = connect(
            database="assignment2", user='postgres', password='admin', host='127.0.0.1', port='5432'
        )
        self.cur = self.conn.cursor()
        self.path = path
        self.logger = []
        self.custom_queries = []
        self.materialized_views = []
        self.non_materialized_views = []
        self.materialized_globals = []
        self.non_materialized_globals = []

    def create_sources(self):
        # id, title, year, runtime > 75 mins
        S1_NON = """
            CREATE VIEW ComedyMovie
            AS 
                SELECT DISTINCT(t.id), t.title, t.startYear
                FROM title t
                JOIN title_genre tg
                    ON t.id = tg.titleid
                JOIN genre g 
                    ON tg.genreid = g.id
                WHERE (
                    t.runtime >= 75 AND
                    t.type = 'movie' AND
                    g.genre = 'Comedy'
                )
        """
        S1_MAT = """
            CREATE MATERIALIZED VIEW ComedyMovie_Mat
            AS 
                SELECT DISTINCT(t.id), t.title, t.startYear
                FROM title t
                JOIN title_genre tg
                    ON t.id = tg.titleid
                JOIN genre g 
                    ON tg.genreid = g.id
                WHERE (
                    t.runtime >= 75 AND
                    t.type = 'movie' AND
                    g.genre = 'Comedy'
                )
        """
        S2_NON = """
            CREATE VIEW NonComedyMovie
            AS 
                SELECT t.id, t.title, t.startYear
                FROM title t
                WHERE NOT EXISTS (
                    SELECT id
                    FROM ComedyMovie
                    WHERE id = t.id
                ) 
                    AND t.type = 'movie'
                    AND t.runtime >= 75
        """
        S2_MAT = """
            CREATE MATERIALIZED VIEW NonComedyMovie_Mat
            AS 
                SELECT t.id, t.title, t.startYear
                FROM title t
                WHERE NOT EXISTS (
                    SELECT id
                    FROM ComedyMovie
                    WHERE t.id = ComedyMovie.id
                ) 
                    AND t.type = 'movie'
                    AND t.runtime >= 75
        """
        S3_NON = """
            CREATE VIEW ComedyActor
            AS
                SELECT DISTINCT(m.id), m.name, m.birthyear, m.deathyear
                FROM member m
                JOIN title_actor ta
                    ON m.id = ta.actorid
                JOIN title t
                    ON ta.titleid = t.id
                JOIN title_genre tg
                    ON t.id = tg.titleid
                JOIN genre g 
                    ON tg.genreid = g.id
                WHERE (
                    t.type = 'movie' AND 
                    g.genre = 'Comedy'
                )
        """
        S3_MAT = """
            CREATE MATERIALIZED VIEW ComedyActor_Mat
            AS
                SELECT DISTINCT(m.id), m.name, m.birthyear, m.deathyear
                FROM member m
                JOIN title_actor ta
                    ON m.id = ta.actorid
                JOIN title t
                    ON ta.titleid = t.id
                JOIN title_genre tg
                    ON t.id = tg.titleid
                JOIN genre g 
                    ON tg.genreid = g.id
                WHERE (
                    t.type = 'movie' AND 
                    g.genre = 'Comedy'
                )
        """
        S4_NON = """
            CREATE VIEW NonComedyActor
            AS
                SELECT DISTINCT(m.id), m.name, m.birthyear, m.deathyear
                FROM member m
                JOIN title_actor ta
                    ON m.id = ta.actorid
                JOIN title t
                    ON ta.titleid = t.id
                WHERE NOT EXISTS (
                    SELECT id
                    FROM ComedyActor
                    WHERE m.id = ComedyActor.id
                ) AND t.type = 'movie'
        """
        S4_MAT = """
            CREATE MATERIALIZED VIEW NonComedyActor_Mat
            AS
                SELECT DISTINCT(m.id), m.name, m.birthyear, m.deathyear
                FROM member m
                JOIN title_actor ta
                    ON m.id = ta.actorid
                JOIN title t
                    ON ta.titleid = t.id
                WHERE NOT EXISTS (
                    SELECT id
                    FROM ComedyActor
                    WHERE m.id = ComedyActor.id
                ) AND t.type = 'movie'
        """
        S5_NON = """
            CREATE VIEW ActedIn
            AS
                SELECT ta.*
                FROM title_actor ta
                JOIN title t
                    ON ta.titleid = t.id
                WHERE t.type = 'movie'
        """
        S5_MAT = """
            CREATE MATERIALIZED VIEW ActedIn_Mat
            AS
                SELECT ta.*
                FROM title_actor ta
                JOIN title t
                    ON ta.titleid = t.id
                WHERE t.type = 'movie'
        """

        self.non_materialized_views.extend([S1_NON, S2_NON, S3_NON, S4_NON, S5_NON])
        
        self.materialized_views.extend([S1_MAT, S2_MAT, S3_MAT, S4_MAT, S5_MAT])

        for index, view in enumerate(self.non_materialized_views):
            try:
                self.cur.execute(view)
                self.conn.commit()
            except errors.DuplicateTable:
                self.conn.rollback()
        print("Finished adding the Non-Materialized Views")
        for index, view in enumerate(self.materialized_views):
            try:
                self.cur.execute(view)
                self.conn.commit()
            except errors.DuplicateTable:
                self.conn.rollback()
        print("Finished adding the Materialized Views")

    def create_global_schemas(self):

        ALL_MOVIES_NON = """
            CREATE VIEW All_Movie
            AS
                SELECT *, 'Comedy' as genre
                FROM ComedyMovie
                UNION
                (
                    SELECT DISTINCT ON (ncm.id) ncm.*, g.genre
                    FROM NonComedyMovie ncm 
                    JOIN title_genre tg
                        ON ncm.id = tg.titleid
                    JOIN genre g
                        ON tg.genreid = g.id
                )
        """
        ALL_MOVIES_MAT = """
            CREATE MATERIALIZED VIEW All_Movie_Mat
            AS
                SELECT *, 'Comedy' as genre
                FROM ComedyMovie_Mat
                UNION
                (
                    SELECT DISTINCT ON (ncm.id) ncm.*, g.genre
                    FROM NonComedyMovie_Mat ncm 
                    JOIN title_genre tg
                        ON ncm.id = tg.titleid
                    JOIN genre g
                        ON tg.genreid = g.id
                )
        """
        ALL_ACTORS_NON = """
            CREATE VIEW All_Actor
            AS
                SELECT *
                FROM ComedyActor
                UNION
                SELECT *
                FROM NonComedyActor
        """
        ALL_ACTORS_MAT = """
            CREATE MATERIALIZED VIEW All_Actor_Mat
            AS
                SELECT *
                FROM ComedyActor_Mat
                UNION
                SELECT *
                FROM NonComedyActor_Mat
        """
        ALL_MOVIE_ACTOR_NON = """
            CREATE VIEW All_Movie_Actor
            AS
                SELECT *
                FROM ActedIn
        """
        ALL_MOVIE_ACTOR_MAT = """
            CREATE MATERIALIZED VIEW All_Movie_Actor_Mat
            AS
                SELECT *
                FROM ActedIn_Mat
        """

        self.non_materialized_globals.extend([ALL_MOVIES_NON, ALL_ACTORS_NON, ALL_MOVIE_ACTOR_NON])
        self.materialized_globals.extend([ALL_MOVIES_MAT, ALL_ACTORS_MAT, ALL_MOVIE_ACTOR_MAT])
        
        for index, view in enumerate(self.non_materialized_globals):
            try:
                self.cur.execute(view)
                self.conn.commit()
            except errors.DuplicateTable:
                self.conn.rollback()
        print("Finished adding the Non-Materialized Global Schemas")
        for index, view in enumerate(self.materialized_globals):
            try:
                self.cur.execute(view)
                self.conn.commit()
            except errors.DuplicateTable:
                self.conn.rollback()
        print("Finished adding the Materialized Global Schemas")
        
    def queries(self):
        query_one = """
            SELECT aa.id, aa.name, aa.birthyear, COUNT(am.id)
            FROM all_actor_mat aa
            JOIN all_movie_actor_mat ama
                ON aa.id = ama.actorid
            JOIN all_movie_mat am
                ON ama.titleid = am.id
            WHERE (
                aa.deathyear is NULL AND
                (am.startyear > 2000 AND am.startyear < 2005)
            )
            GROUP BY aa.id, aa.name, aa.birthyear
            HAVING COUNT (am.id) >= 10
        """
        self.cur.execute(query_one)
        results = self.cur.fetchall()
        print("\nFinished with the first query")
        for i in range(0, 10):
            print(results[i])
        
        query_two = """
            SELECT aa.id, aa.name, aa.birthyear, aa.deathyear
            FROM all_actor_mat aa
            JOIN all_movie_actor_mat ama
                ON aa.id = ama.actorid
            JOIN all_movie_mat am
                ON ama.titleid = am.id
            WHERE (
                aa.name like 'Ja%' AND
                am.genre != 'Comedy'
            )
        """
        self.cur.execute(query_two)
        results = self.cur.fetchall()
        print("\nFinished with the two query")
        for i in range(0, 10):
            print(results[i])
    
    def expanded_queries(self):
        query_one_mat = """
            SELECT aa.id, aa.name, aa.birthyear, COUNT(am.id)
            FROM (SELECT *
                FROM ComedyActor_Mat
                UNION
                SELECT *
                FROM NonComedyActor_Mat) aa
            JOIN (SELECT *
                FROM ActedIn_Mat) ama
                ON aa.id = ama.actorid
            JOIN (SELECT *, 'Comedy' as genre
                FROM ComedyMovie_Mat
                UNION
                (
                    SELECT DISTINCT ON (ncm.id) ncm.*, g.genre
                    FROM NonComedyMovie_Mat ncm 
                    JOIN title_genre tg
                        ON ncm.id = tg.titleid
                    JOIN genre g
                        ON tg.genreid = g.id
                )) am
                ON ama.titleid = am.id
            WHERE (
                aa.deathyear is NULL AND
                (am.startyear > 2000 AND am.startyear < 2005)
            )
            GROUP BY aa.id, aa.name, aa.birthyear
            HAVING COUNT (am.id) >= 10
        """
        query_one_non = """
            SELECT aa.id, aa.name, aa.birthyear, COUNT(am.id)
            FROM (SELECT *
                FROM ComedyActor
                UNION
                SELECT *
                FROM NonComedyActor) aa
            JOIN (SELECT *
                FROM ActedIn) ama
                ON aa.id = ama.actorid
            JOIN (SELECT *, 'Comedy' as genre
                FROM ComedyMovie
                UNION
                (
                    SELECT DISTINCT ON (ncm.id) ncm.*, g.genre
                    FROM NonComedyMovie ncm 
                    JOIN title_genre tg
                        ON ncm.id = tg.titleid
                    JOIN genre g
                        ON tg.genreid = g.id
                )) am
                ON ama.titleid = am.id
            WHERE (
                aa.deathyear is NULL AND
                (am.startyear > 2000 AND am.startyear < 2005)
            )
            GROUP BY aa.id, aa.name, aa.birthyear
            HAVING COUNT (am.id) >= 10
        """
        start_time = timeit.default_timer()
        self.cur.execute(query_one_mat)
        results = self.cur.fetchall()
        print('\nIt took {} seconds to perform query one with the materialized views'.format(timeit.default_timer() - start_time))
        
        start_time = timeit.default_timer()
        self.cur.execute(query_one_non)
        results = self.cur.fetchall()
        print('It took {} seconds to perform query one with the non-materialized views'.format(timeit.default_timer() - start_time))
        
        query_two_mat = """
            SELECT aa.id, aa.name, aa.birthyear, aa.deathyear
            FROM (SELECT *
                FROM ComedyActor_Mat
                UNION
                SELECT *
                FROM NonComedyActor_Mat) aa
            JOIN (SELECT *
                FROM ActedIn_Mat) ama
                ON aa.id = ama.actorid
            JOIN (SELECT *, 'Comedy' as genre
                FROM ComedyMovie_Mat
                UNION
                (
                    SELECT DISTINCT ON (ncm.id) ncm.*, g.genre
                    FROM NonComedyMovie_Mat ncm 
                    JOIN title_genre tg
                        ON ncm.id = tg.titleid
                    JOIN genre g
                        ON tg.genreid = g.id
                )) am
                ON ama.titleid = am.id
            WHERE (
                aa.name like 'Ja%' AND
                am.genre != 'Comedy'
            )
        """
        query_two_non = """
            SELECT aa.id, aa.name, aa.birthyear, aa.deathyear
            FROM (SELECT *
                FROM ComedyActor
                UNION
                SELECT *
                FROM NonComedyActor) aa
            JOIN (SELECT *
                FROM ActedIn) ama
                ON aa.id = ama.actorid
            JOIN (SELECT *, 'Comedy' as genre
                FROM ComedyMovie
                UNION
                (
                    SELECT DISTINCT ON (ncm.id) ncm.*, g.genre
                    FROM NonComedyMovie ncm 
                    JOIN title_genre tg
                        ON ncm.id = tg.titleid
                    JOIN genre g
                        ON tg.genreid = g.id
                )) am
                ON ama.titleid = am.id
            WHERE (
                aa.name like 'Ja%' AND
                am.genre != 'Comedy'
            )
        """
        start_time = timeit.default_timer()
        self.cur.execute(query_two_mat)
        results = self.cur.fetchall()
        print('It took {} seconds to perform query two with the materialized views'.format(timeit.default_timer() - start_time))
        
        start_time = timeit.default_timer()
        self.cur.execute(query_two_non)
        results = self.cur.fetchall()
        print('It took {} seconds to perform query two with the non-materialized views'.format(timeit.default_timer() - start_time))
        
    def optimized_queries(self):
        query_one_mat = """
            SELECT aa.id, aa.name, aa.birthyear, COUNT(am.id)
            FROM (SELECT *
                FROM ComedyActor_Mat
                UNION
                SELECT *
                FROM NonComedyActor_Mat) aa
            JOIN (SELECT *
                FROM ActedIn_Mat) ama
                ON aa.id = ama.actorid
            JOIN (SELECT *
                FROM ComedyMovie_Mat
                UNION
                (
                    SELECT DISTINCT ON (ncm.id) ncm.*
                    FROM NonComedyMovie_Mat ncm 
                )) am
                ON ama.titleid = am.id
            WHERE (
                aa.deathyear is NULL AND
                (am.startyear > 2000 AND am.startyear < 2005)
            )
            GROUP BY aa.id, aa.name, aa.birthyear
            HAVING COUNT (am.id) >= 10
        """
        query_one_non = """
            SELECT aa.id, aa.name, aa.birthyear, COUNT(am.id)
            FROM (SELECT *
                FROM ComedyActor
                UNION
                SELECT *
                FROM NonComedyActor) aa
            JOIN (SELECT *
                FROM ActedIn) ama
                ON aa.id = ama.actorid
            JOIN (SELECT *
                FROM ComedyMovie
                UNION
                (
                    SELECT DISTINCT ON (ncm.id) ncm.*
                    FROM NonComedyMovie ncm 
                )) am
                ON ama.titleid = am.id
            WHERE (
                aa.deathyear is NULL AND
                (am.startyear > 2000 AND am.startyear < 2005)
            )
            GROUP BY aa.id, aa.name, aa.birthyear
            HAVING COUNT (am.id) >= 10
        """
        start_time = timeit.default_timer()
        self.cur.execute(query_one_mat)
        results = self.cur.fetchall()
        print('It took {} seconds to perform the optimized query one with the materialized views'.format(timeit.default_timer() - start_time))
        
        start_time = timeit.default_timer()
        self.cur.execute(query_one_non)
        results = self.cur.fetchall()
        print('It took {} seconds to perform the optimized query one with the non-materialized views'.format(timeit.default_timer() - start_time))
        
        query_two_mat = """
            SELECT aa.id, aa.name, aa.birthyear, aa.deathyear
            FROM (
                SELECT *
                FROM NonComedyActor_Mat) aa
            JOIN (SELECT *
                FROM ActedIn_Mat) ama
                ON aa.id = ama.actorid
            JOIN (
                SELECT DISTINCT ON (ncm.id) ncm.*
                FROM NonComedyMovie_Mat ncm 
            ) am
                ON ama.titleid = am.id
            WHERE (
                aa.name like 'Ja%'
            )
        """
        query_two_non = """
            SELECT aa.id, aa.name, aa.birthyear, aa.deathyear
            FROM (
                SELECT *
                FROM NonComedyActor) aa
            JOIN (SELECT *
                FROM ActedIn) ama
                ON aa.id = ama.actorid
            JOIN (
                SELECT DISTINCT ON (ncm.id) ncm.*
                FROM NonComedyMovie ncm 
            ) am
                ON ama.titleid = am.id
            WHERE (
                aa.name like 'Ja%'
            )
        """
        start_time = timeit.default_timer()
        self.cur.execute(query_two_mat)
        results = self.cur.fetchall()
        print('It took {} seconds to perform the optimized query two with the materialized views'.format(timeit.default_timer() - start_time))
        
        start_time = timeit.default_timer()
        self.cur.execute(query_two_non)
        results = self.cur.fetchall()
        print('It took {} seconds to perform the optimized query two with the non-materialized views'.format(timeit.default_timer() - start_time))
        
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    db = database(path)
    db.create_sources()
    db.create_global_schemas()
    db.queries()
    db.expanded_queries()
    db.optimized_queries()
    if len(db.logger) > 0:
        print(db.logger) 
    winsound.Beep(440, 1000)