import sys
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
from time import sleep
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import pymysql
from Send_Email import *
import json
import datetime

def finance_scarpe(log_url):

    try:
        "starting webdriver"

        data_list=[]
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox');
        options.add_argument('--headless');
        options.add_argument("disable-infobars");
        options.add_argument("start-maximized");
        options.add_argument('--disable-extensions');
        options.add_argument('--disable-gpu');
        options.add_argument("--disable-dev-shm-usage");
        options.add_argument('--log-level=3')
        browser = webdriver.Chrome(executable_path=driver_path, chrome_options=options)

        "opening the website"

        browser.get(log_url)
        html_chunk=browser.page_source

        "Starting to parse the data"

        soup = BeautifulSoup(html_chunk)
        table_data=soup.find(class_="W(100%) M(0)")
        sleep(2)
        thead=table_data.find_all('thead')[0].findAllNext('th')
        headers=[ x.text.replace('*','').replace(' ','_') for x in thead]
        print(headers)
        tbody=table_data.find_all('tbody')[0].findAllNext('tr')
        for t in tbody:
            table_dt=t.find_all('td')
            data_row=[ dt.text for dt in table_dt]
            if len(data_row) > 6:
                data_row[6]=data_row[6].replace(',','')
                data_list.append(data_row)

        df = pd.DataFrame(data_list, columns=headers)
        df = df[:-1]

        df['Volume'] = df['Volume'].convert_dtypes()
        df['Date'] = df['Date'].convert_dtypes()

        "Setting the data type for each cloumn"

        print(df.dtypes)
        convert_dict = {
            'Open': float,
            'High':float,
            'Low':float,
            'Close':float,
            'Adj_Close':float,
            'Volume': int
        }
        df['Volume']=df['Volume'].replace(',', '')
        df = df.astype(convert_dict)
        df['Open_avg']=df['Open'].mean()
        df['High_avg']=df['High'].mean()
        df['Low_avg']=df['Low'].mean()
        df['Close_avg']=df['Close'].mean()
        df['Adj_Close_avg']=df['Adj_Close'].mean()
        df['Volume_avg']=df['Volume'].mean()

        "Creating data base connection"
        "pass you data base info"

        connection = pymysql.connect(host=db_endpoint, user=db_username, password=db_password, database=db_name)
        cursor = connection.cursor()
        counter=0

        "Inserting data into database"

        for index, row in df.iterrows():
            print(row)
            format = '%b %d, %Y'  # The format
            date_value = datetime.datetime.strptime(row['Date'], format).date()
            print(date_value)
            exp=f"INSERT INTO task_1 (Date,Low,Open,High,Volume,Open_avg,Close,Low_avg,High_avg,Close_avg,Volume_avg,Adj_Close,Adj_Close_avg) values ({date_value},{row['Low']},{row['Open']},{row['High']},{row['Volume']},{row['Open_avg']},{row['Close']}," \
                f" {row['Low_avg']}, {row['High_avg']},{row['Close_avg']},{row['Volume_avg']},{row['Adj_Close']},{row['Adj_Close_avg']} )"
            print(exp)
            cursor.execute(exp)
            connection.commit()
        connection.commit()
        cursor.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    log_url='https://finance.yahoo.com/quote/COMM/history?p=COMM'
    data_table =finance_scarpe(log_url)