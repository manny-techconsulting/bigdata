import pandas as pd
from sqlalchemy import create_engine
import os

def getfileName(filename):
    f = os.path.join(os.path.dirname(__file__), filename)
    return f

def connect(filename):
    engine = create_engine('postgresql+psycopg2://consultants:WelcomeItc%402022@ec2-3-9-191-104.eu-west-2.compute.amazonaws.com/testdb')
    
    with engine.connect() as conn:
        rapper_df = pd.read_csv(getfileName(filename))
        rapper_df.to_sql(name='smoking',  con=conn, if_exists='replace')
        print(conn.closed)

if __name__ == '__main__':
    connect(r'data/smoking.csv')