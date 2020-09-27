from psycopg2 import errors, sql, connect
import os
import timeit

class database:
    def __init__(self, path):
        self.conn = connect(
            database="assignment2", user='postgres', password='admin', host='127.0.0.1', port='5432'
        )
        self.cur = self.conn.cursor()
        self.path = path
        self.logger = []
    
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
    
    def copy_titles(self):
        file_path = open(self.path + '\\data\\title.csv', "r", encoding='utf-8')
        self.cur.copy_expert("""COPY title FROM STDIN WITH (NULL 'None', FORMAT CSV, HEADER);""", file_path)
        self.conn.commit()
    
    def copy_genres(self):
        file_path = open(self.path + '\\data\\genre.csv', "r", encoding='utf-8')
        self.cur.copy_expert("""COPY genre FROM STDIN WITH (NULL 'None', FORMAT CSV, HEADER);""", file_path)
        self.conn.commit()
    
    def copy_roles(self):
        file_path = open(self.path + '\\data\\role.csv', "r", encoding='utf-8')
        self.cur.copy_expert("""COPY role FROM STDIN WITH (NULL 'None', FORMAT CSV, HEADER);""", file_path)
        self.conn.commit()
    
    def copy_member(self):
        file_path = open(self.path + '\\data\\member.csv', "r", encoding='utf-8')
        self.cur.copy_expert("""COPY member FROM STDIN WITH (NULL 'None', FORMAT CSV, HEADER);""", file_path)
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
            self.cur.execute(query)
            missing = self.cur.fetchall()
            print("\nExample Data :")
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
            self.cur.execute(query)
            actors = self.cur.fetchall()
            print("\nExample Data :")
            for i in range(0, 5):
                print(actors[i])
        
        def get_gill_producers():
            """
            Returns the producers who has "Gill" in their name and who had a talk-show in 2017.
            Ordered in the number of who producer more
            """
            query = """
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
            self.cur.execute(query)
            producers = self.cur.fetchall()
            print("\nExample Data :")
            for i in range(0, 5):
                print(producers[i])   

        def get_longrun_producers():
            
        qry_time = timeit.timeit(get_invalid_tas, number=1)
        print("(2.1) It took {} seconds to query the number of missing entries".format(qry_time))

        qry_time = timeit.timeit(get_phi_actors, number=1)
        print("(2.2) It took {} seconds to query the number of actors whos name starts with Phi...".format(qry_time))

        qry_time = timeit.timeit(get_gill_producers, number=1)
        print("(2.3) It took {} seconds to query the number of producers whos name contains 'Gill'...".format(qry_time))

if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    db = database(path)
    db.queries()
    # print(timeit.timeit(db.copy_relations, number=1))
    if len(db.logger) > 0:
        print(db.logger) 

