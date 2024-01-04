import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd


print("Landing has started")

user = 'admin'
pwd = urllib.parse.quote("jt+lwENObhw>W9-x", safe='')
host = 'slice-prod-read-replica.ckz702qgrqmu.eu-west-1.rds.amazonaws.com'
port = '3306'
schema = 'prodkaykroo'


def source_db_connection():
    engine = create_engine(
        f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{schema}")
    return engine


engine = source_db_connection()

source_query = text(f"""

    select *
    from genesis_product_item_request

    """)

# with engine.connect() as conn:
#     customer = conn.execute(query).fetchall()
df = pd.read_sql_query(source_query, engine)
print("Total records form source : ", df.shape[0])


users = 'root'
pwds = 'admin123'
host = '192.168.18.38'
port = '3306'
schema = 'test'


def target_db_connection():
    engine = create_engine(
        f"mysql+pymysql://{users}:{pwds}@{host}:{port}/{schema}")
    return engine


t_engine = target_db_connection()


target_query = text(f"""

    truncate table  test.genesis_product_item_request

    """)

with t_engine.connect() as t_conn:
    t_conn.execute(target_query)


df.to_sql("genesis_product_item_request", con=t_engine, if_exists='append',
          index=False, chunksize=1000)
print("Data pushed success")
