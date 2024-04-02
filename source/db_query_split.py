import os
import psycopg2
import psycopg2.extras as extras
import sqlalchemy

engine = 'postgresql://postgres:mnbvcxz1234567@3.111.61.248:5432/de_db'
engine_past = sqlalchemy.create_engine('postgresql://postgres:mnbvcxz1234567@3.111.61.248:5432/de_db')
pgconnection = "dbname=de_db user=postgres host=3.111.61.248 port=5432 password = mnbvcxz1234567"
fd = open(r'source/pg_script.sql', 'r')
query = fd.read()
fd.close()
query_splt = query.split('--')

def Makelist(dt_colnames, df):
    lis = [['Datatypes'], ['Aggregated Value']]
    for col in dt_colnames:
        lis[0].append(str(df[col].dtypes))
        if str(df[col].dtypes) == 'int64' or str(df[col].dtypes) == 'float64':
            lis[1].append(df[col].sum())
        else:
            lis[1].append('NA_string')
    return lis

def execute_values(conn, df, schema, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    insrt_query = "INSERT INTO %s.%s(%s) VALUES %%s" % (schema, table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, insrt_query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()