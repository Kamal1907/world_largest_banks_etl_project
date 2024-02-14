# importing required library, running on VScode from my local machine

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
import wget

# url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
# filename = 'exchange_rate.csv'
# wget.download(url, filename)


# initializing all the known variables as shared in the project scenario
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
csv_path = 'exchange_rate.csv'
output_csv_path = './Largest_banks_data.csv'
table_attribs = ['Name', 'MC_USD_Billion']
table_name = 'Largest_banks'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as file:
        file.write(timestamp + ':' + message + '\n')

log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html = requests.get(url).text
    data = BeautifulSoup(html, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table = data.find_all('tbody')
    column_data = table[0].find_all('tr')
    for row in column_data[1:]:
        col = row.find_all('td')
        if (len(col)==0):
            continue
        if (col[1].find('a')==None):
            continue
        data_dict = {"Name": col[1].find_all('a')[1].contents[0],
                     "MC_USD_Billion": float(col[2].contents[0].replace('\n', ''))}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df1], ignore_index=True)
    return df
df = extract(url, table_attribs)
# print(df)

log_progress("Data extraction complete. Initiating Transformation process")

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rate = pd.read_csv(csv_path).set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df

df = transform(df, csv_path)
#print(df)

log_progress("Data transformation complete. Initiating Loading process")

# quiz = df['MC_EUR_Billion'][4]
# print(f"Answer to quiz: {quiz}")

def load_to_csv(df, output_path):
     ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
     df.to_csv(output_path)

load_to_csv(df, output_csv_path)

log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists = 'replace', index=False)

sql_connection = sqlite3.connect(table_name)

log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)

log_progress("Data loaded to Database as a table, Executing queries")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    query_output = pd.read_sql(query_statement, sql_connection)
    print(f"Query statement:\n{query_statement}\n")
    print(f"Query output:\n{query_output}")

query1 = 'SELECT * FROM Largest_banks'

query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'

query3 = 'SELECT Name from Largest_banks LIMIT 5'

run_query(query1, sql_connection)
run_query(query2, sql_connection)
run_query(query3, sql_connection)

log_progress("Process Complete")

sql_connection.close()