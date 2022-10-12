import configparser
import logging
import psycopg2
from sql_queries import create_table_queries

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-4s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def create_tables(cur, con):
    '''
    Runs DDL against the target DB to create fact and dim tables
    Parameters:
        cur (psycopg2 cursor object)
        con (psycopg2 connection object)
    '''
    for query in create_table_queries:
        logging.info(f"Executing query {query[:64]}...")
        try:
            cur.execute(query)
            con.commit()
        except Exception as e:
            logging(e)
    logging.info("Done with creating tables")
            

def main():
    config = configparser.ConfigParser()
    config.read('config\config.cfg')
    con = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = con.cursor()
    create_tables(cur, con)
    
    con.close()
    
if __name__ == "__main__":
    main()
    