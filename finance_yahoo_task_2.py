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


def click_button():
    print()

def finance_scarpe(log_url):
    try:
        "starting webdriver"

        data_list = []
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox');
        # options.add_argument('--headless');
        options.add_argument("disable-infobars");
        options.add_argument("start-maximized");
        options.add_argument('--disable-extensions');
        options.add_argument('--disable-gpu');
        options.add_argument("--disable-dev-shm-usage");
        options.add_argument('--log-level=3')
        browser = webdriver.Chrome(executable_path=driver_path, chrome_options=options)

        "opening the website"

        browser.get(log_url)

        "Starting to parse the data"

        'getting headrs for data'
        heads=browser.find_elements_by_xpath("//div[@class='D(tbhg)']/div[1]/div")
        headrs=[h.text for h in heads]

        print(headrs)

        availiable_row = browser.find_elements_by_xpath("//div[@data-test='fin-row']")

        btn=True

        while btn:
            count = 0
            for row in availiable_row:
                try:
                    tad=row.find_element_by_css_selector("svg").get_attribute("data-icon")
                    if tad=='caret-right':
                        row.find_element_by_css_selector("button").click()
                        count=1
                except Exception as e:
                    pass
            availiable_row = browser.find_elements_by_xpath("//div[@data-test='fin-row']")
            if count==0:
                btn=False

        data_list=[]

        for rows in availiable_row:

            cc = rows.find_elements_by_css_selector("span")
            data_chunk=[r.text.replace(',','') for r in cc]
            if len(data_chunk)==6:
                data_list.append(data_chunk)
                data_chunk=[]
        print(data_list)



        df = pd.DataFrame(data_list, columns=headrs)
        print(df)

        convert_dict = {

            'TTM': float,
            headrs[2]: float,
            headrs[3]: float,
            headrs[4]: float,
            headrs[5]: float,
        }

        df = df.astype(convert_dict)

        connection = pymysql.connect(host=db_endpoint, user=db_username, password=db_password, database=db_name)
        cursor = connection.cursor()
        counter = 0

        for index, row in df.iterrows():
            exp = f"INSERT INTO task_2 (Breakdown,TTM,date_1,date_2,date_3,date_4) values ('{row['Breakdown']}',{row['TTM']},{row['12/30/2021']},{row['12/30/2020']},{row['12/30/2019']},{row['12/30/2018']} )"
            print(exp)
            cursor.execute(exp)
            connection.commit()
        connection.commit()
        cursor.close()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    log_url = 'https://finance.yahoo.com/quote/COMM/financials?p=COMM'
    data_table = finance_scarpe(log_url)