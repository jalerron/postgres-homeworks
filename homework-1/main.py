"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

# connect to db
pass_ = input('Enter a paswword: ')
conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password=pass_)


def import_data_to_db(file, table):
    with open(file, 'r') as fp:
        csv_reader = csv.reader(fp)
        next_ = next(csv_reader)
        try:
            with conn.cursor() as cur:
                # execute query
                insert_query = f"INSERT INTO {table} VALUES ({','.join(['%s']*len(next_))})"

                # input data to db
                for row in csv_reader:
                    cur.execute(insert_query, row)

        finally:
            conn.commit()
            conn.close()


import_data_to_db('./north_data/customers_data.csv', 'customers')
import_data_to_db('./north_data/employees_data.csv', 'employees')
import_data_to_db('./north_data/orders_data.csv', 'orders')
