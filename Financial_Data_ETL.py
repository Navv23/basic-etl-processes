import selenium.webdriver as webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
from selenium.webdriver.chrome.service import Service
import datetime

options = Options()
options.binary_location = r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"
service = Service(r"D:\Selenium\chromedriver-win64\chromedriver.exe")
driver = webdriver.Chrome(options=options, service=service)

#Extract
driver.get("https://portal.tradebrains.in/marketstats/gainers/NIFTY")
driver.maximize_window()
time.sleep(1)

soup = bs(driver.page_source, 'lxml')

table = soup.find('div', class_='''ant-table-content''')

headers = []

for i in table.find_all('th')[:-1]:
    title = i.text
    headers.append(title)
    
df = pd.DataFrame(columns=headers)

for j in table.find_all('tr')[1:]:
    rows = j.find_all('td')[0:4]
    data = [tr.text for tr in rows]
    length = len(df)
    df.loc[length] = data

todays_date = datetime.date.today()
filename = f"Top_Gainers_{todays_date}.csv"
df.to_csv(r"C:\Users\navv\Desktop\\" + filename, index=False)

#Transform
df1 = pd.read_csv(r"C:\Users\navv\Desktop\Top_Gainers_2023-10-31.csv",index_col=False)
df1 = df1.iloc[1:]
df1.head()

df1['CMP'] = df1['CMP'].str.replace('₹','')
df1['Prev Close'] = df1['Prev Close'].str.replace('₹','')
df1['Change'] = df1['Change'].str.replace('₹','')
df1.head()

df1['change_percent'] = df1['Change'].str.split(" ").str[0]
df1.head()
df1['change_amount'] = df1['Change'].str.split("(").str[1]
df1['change_amount'] = df1['change_amount'].str.replace(")",'')

df1 = df1.drop(['Change'], axis=1)

#Load
import psycopg2
from sqlalchemy import create_engine

db_credentials = {
    'host': 'localhost',
    'database': 'Financial_data_ETL',
    'user': 'postgres',
    'password': '1234',
    'port':'5432'
}

conn = psycopg2.connect(**db_credentials)

engine = create_engine(f'postgresql+psycopg2://{db_credentials["user"]}:{db_credentials["password"]}@{db_credentials["host"]}/{db_credentials["database"]}')

todays_date = datetime.date.today()
table_name = f'top_gainers_{todays_date}'

df1.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

conn.close()