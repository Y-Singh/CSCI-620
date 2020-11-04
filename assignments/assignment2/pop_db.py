userfrom psycopg2 import errors, sql, connect
import os
import timeit
import winsound
import json


class database:
    def __init__(self, path):
        self.conn = connect(
            database="assignment2", user='postgres', password='admin', host='127.0.0.1', port='5432'
        )
        self.cur = self.conn.cursor()
        self.path = path
        self.logger = []
        self.custom_queries = []

    def copy_init_info(self):
        t = open(self.path + '\\data\\title.csv', "r", encoding='utf-8')
        g = open(self.path + '\\data\\genre.csv', "r", encoding='utf-8')
        r = open(self.path + '\\data\\role.csv', "r", encoding='utf-8')
        m = open(self.path + '\\data\\member.csv', "r", encoding='utf-8')

        self.create_basic_table(t, table_name="title")
        self.create_basic_table(g, table_name="genre")
        self.create_basic_table(r, table_name="role")
        self.create_basic_table(m, table_name="member")


    def create_basic_table(self, f, table_name):
        query = """
            COPY {table_name} 
            FROM STDIN WITH (NULL 'None', FORMAT CSV, HEADER);
        """.format(table_name = table_name)
        self.cur.copy_expert(query, f)
        self.conn.commit()
    
    def copy_relations(self):
        
        file_path = open(self.path + '\\data\\title_actor.csv', "r", encoding='utf-8')
        self.create_relation(file_path, table_name="title_actor", table_id='actorid')

        file_path = open(self.path + '\\data\\title_director.csv', "r", encoding='utf-8')
        self.create_relation(file_path, table_name="title_director", table_id='directorid')

        file_path = open(self.path + '\\data\\title_genre.csv', "r", encoding='utf-8')
        self.create_relation(file_path, table_name="title_genre", table_id='genreid')

        file_path = open(self.path + '\\data\\title_producer.csv', "r", encoding='utf-8')
        self.create_relation(file_path, table_name="title_producer", table_id='producerid')

        file_path = open(self.path + '\\data\\title_writer.csv', "r", encoding='utf-8')
        self.create_relation(file_path, table_name="title_writer", table_id='writerid')

        file_path = open(self.path + '\\data\\actor_title_role.csv', "r", encoding='utf-8')
        query = """
            CREATE TEMP TABLE tmp_table
            ON COMMIT DROP
            AS
            SELECT *
            FROM actor_title_role
            WITH NO DATA;
            
            COPY tmp_table FROM STDIN WITH (NULL 'None', FORMAT CSV, HEADER);

            INSERT INTO actor_title_role
            SELECT DISTINCT ON (titleid, actorid, roleid) i.*
            FROM tmp_table i
            WHERE EXISTS (
                SELECT title_actor.titleid, title_actor.actorid, role.id
                FROM title_actor, role
                WHERE title_actor.titleid = i.titleid AND title_actor.actorid = i.actorid AND role.id = i.roleid
                FOR SHARE
            )
            ON CONFLICT DO NOTHING;
        """
        self.cur.copy_expert(query, file_path)
        self.conn.commit()

    def create_relation(self, f, table_name:str, table_id:str):
        query = """
            CREATE TEMP TABLE tmp_table
            ON COMMIT DROP
            AS
            SELECT *
            FROM {table_name}
            WITH NO DATA;
            
            COPY tmp_table FROM STDIN WITH (NULL 'None', FORMAT CSV, HEADER);

            INSERT INTO {table_name}
            SELECT DISTINCT ON (titleid, {table_id}) i.*
            FROM tmp_table i
            WHERE EXISTS (
                SELECT member.id, title.id
                FROM member, title
                WHERE member.id = i.{table_id} AND title.id = i.titleid
                FOR SHARE
            )
            ON CONFLICT DO NOTHING;
        """.format(table_name=table_name, table_id=table_id)
        
        self.cur.copy_expert(query, f)
        self.conn.commit()

    def queries(self):
        def get_invalid_tas():
            """
            Queries the number of invalid title_actor relationships with respect to role
            """
            query = """
                SELECT titleid, actorid
                FROM title_actor
                EXCEPT (
                    SELECT titleid, actorid
                    FROM actor_title_role
                )
            """
            self.custom_queries.append(query)
            self.cur.execute(query)
            missing = self.cur.fetchall()
            print("\n(2.1) Example Data :")
            for i in range(0, 5):
                print(missing[i])
            print("\nThere are {} missing entries that are in Title_Actor that are not in Actor_Title_Role".format(len(missing)))

        def get_phi_actors():
            """
            Returns the actors who are alive and name starts with "Phi" and did not participate in any movies in 2014
            """
            query = """
                SELECT *
                FROM member m
                JOIN title_actor ta
                    ON m.id = ta.actorid
                    JOIN title t
                        ON ta.titleid = t.id
                WHERE (
                    (m.name LIKE 'Phi%' AND m.deathYear IS NULL) AND
                    (t.type = 'movie' AND (t.startYear != 2014 or t.endYear != 2014))
                );
            """
            self.custom_queries.append(query)
            self.cur.execute(query)
            actors = self.cur.fetchall()
            print("\n(2.2) Example Data :")
            for i in range(0, 5):
                print(actors[i])
        
        def get_gill_producers():
            """
            Returns the producers who has "Gill" in their name and who had a talk-show in 2017.
            Ordered in the number of who producer more
            """
            query = f"""
                SELECT m.name
                FROM member m
                JOIN title_producer tp
                    ON m.id = tp.producerid
                    JOIN title t
                        ON tp.titleid = t.id
                        JOIN title_genre tg
                            ON t.id = tg.titleid
                            JOIN genre g
                                ON tg.genreid = g.id
                WHERE (
                    (m.name LIKE '%Gill%') AND
                    (t.startYear = 2017 or t.endYear = 2017) AND
                    (g.genre = 'Talk-Show')
                )
                GROUP BY m.name
                ORDER BY count(*) DESC;
            """
            self.custom_queries.append(query)
            self.cur.execute(query)
            producers = self.cur.fetchall()
            print("\n(2.3) Example Data :")
            for i in range(0, 5):
                print(producers[i])   

        def get_longrun_producers():
            """
            Returns the name of producers who are alive and titles with runtimes greater than 120 minutes.
            Is ordered by who has producered the most titles
            """
            query = """
                SELECT m.name, COUNT(m.name) total
                FROM member m
                JOIN title_producer tp
                    ON m.id = tp.producerid
                    JOIN title t
                        ON tp.titleid = t.id
                WHERE (
                    (m.deathYear IS NULL) AND
                    (t.runtime > 120)
                )
                GROUP BY m.name
                ORDER BY COUNT(*) DESC;
            """
            self.custom_queries.append(query)
            self.cur.execute(query)
            producers = self.cur.fetchall()
            print("\n(2.4) Example Data :")
            for i in range(0, 5):
                print(producers[i])   

        def get_actors_jesus():
            query = f"""
                SELECT m.name, r.role
                FROM member m
                JOIN actor_title_role atr
                    ON m.id = atr.actorid
                    JOIN role r
                        ON atr.roleid = r.id
                WHERE (
                    (m.deathYear IS NULL) AND
                    (LOWER(r.role) LIKE 'jesu%christ' OR LOWER(r.role) LIKE 'jesus' OR LOWER(r.role) = 'christ' )
                )
                ORDER BY r.role
            """
            self.custom_queries.append(query)
            self.cur.execute(query)
            actors = self.cur.fetchall()
            print("\n(2.5) Example Data :")
            for i in range(0, 5):
                print(actors[i])

        qry_time = timeit.timeit(get_invalid_tas, number=1)
        print("(2.1) It took {} seconds for this query".format(qry_time))

        qry_time = timeit.timeit(get_phi_actors, number=1)
        print("(2.2) It took {} seconds for this query.".format(qry_time))

        qry_time = timeit.timeit(get_gill_producers, number=1)
        print("(2.3) It took {} seconds for this query".format(qry_time))

        qry_time = timeit.timeit(get_longrun_producers, number=1)
        print("(2.4) It took {} seconds for this query".format(qry_time))

        qry_time = timeit.timeit(get_actors_jesus, number=1)
        print("(2.5) It took {} seconds for this query".format(qry_time))

        self.queries_explain()

    def queries_explain(self):
        for num, query in enumerate(self.custom_queries):
            self.cur.execute(self.cur.mogrify('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + query))
            plan = self.cur.fetchall()[0][0]
            with open("q2_{}.json".format(num+1), 'w') as f:
                json.dump(plan, f, indent=1)
    
    def create_indexes(self):
        qry_1 = """
            CREATE INDEX title_actor_index ON title_actor (titleid, actorid);
            CREATE INDEX actor_title_index ON actor_title_role (titleid, actorid);
        """

        qry_2 = """
            CREATE INDEX title_start_year_index ON title(startYear);
            CREATE INDEX title_end_year_index ON title(endYear);
        """
        qry_3 = """
            CREATE INDEX member_name_index ON member(name);
        """
        qry_4 = """
            CREATE INDEX member_death_year_index ON member(deathYear);
            CREATE INDEX title_runtime_index ON title(runtime);
        """

        self.cur.execute(qry_1)
        self.cur.execute(qry_2)
        self.cur.execute(qry_3)
        self.cur.execute(qry_4)
        self.conn.commit()

if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    db = database(path)

    # runtime = timeit.timeit(db.copy_init_info, number=1)
    # print("It took {} seconds to create the basic tables".format(runtime))

    # runtime = timeit.timeit(db.copy_relations, number=1)
    # print("It took {} seconds to create the basic tables".format(runtime))

    db.queries()

    # db.create_indexes()
    
    if len(db.logger) > 0:
        print(db.logger) 
    winsound.Beep(440, 1000)