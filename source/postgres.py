import psycopg2
import json

class Connection():

    def __init__(self):

        with open('./private/db_config.json') as config_file:    
            db_config = json.load(config_file)

        self.conn = psycopg2.connect(**db_config)

    def query(self, sql):

        curr = self.conn.cursor()
        curr.execute(sql)
        result = curr.fetchall()
        curr.close()
        self.conn.commit()
        return result 
