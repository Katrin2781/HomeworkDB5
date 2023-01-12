import pandas as pd
import sqlalchemy
import configparser
from DBClass import create_table
import json

params = {}
config = configparser.ConfigParser()
config.read('setting.ini')
params['userdb'] = config['params']['login']
params['pasword'] = config['params']['password']
params['host'] = config['params']['host']
params['port'] = config['params']['port']
params['db_name'] = config['params']['db_name']
params['SQLsystem'] = config['params']['SQLsystem']

DSN = F"{params['SQLsystem']}://{params['userdb']}:{params['pasword']}" \
      F"@{params['host']}:{params['port']}/{params['db_name']}"
engine = sqlalchemy.create_engine(DSN)

create_table(engine)

with open("data.json") as f:
    data = json.load(f)
for row in data:
    table_name = row['model']
    fields = row['fields']
    df = pd.DataFrame(fields)
    df.to_sql(table_name, engine, if_exists='append', index=False)

publ_name = input('Ведите имя писателя или издательства для вывода: ')

sql_df = pd.read_sql(
    f"""select b.title , s."name" ,s2.price ,s2.date_sale  from stock s3
    join book b on b.id = s3.id_book
    join shop s on  s.id  = s3.id_shop
    join sale s2 on s2.id_stock = s3.id
    where b.id_publisher =(select p.id from publisher p where name = '{publ_name}')""",
    con=engine)
print(sql_df)



