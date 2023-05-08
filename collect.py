from bs4 import BeautifulSoup
from pytrends.request import TrendReq
import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time

def main():
    data = scrape()
    names = [row[0] for row in data if len(row) > 0]
    names = list(set(names))
    ref_keyword = 'ai tool'

    # pytrends = TrendReq(hl='en-US', tz=360)
    # pytrends.build_payload(['SEO PowerSuite Professional - Yearly', ref_keyword], timeframe='today 5-y')
    # print(pytrends.interest_over_time())

    df = pytrends_time(names, ref_keyword)

    cnx = snowflake.connector.connect(
        user='jsices', 
        password='Armeta101', 
        account='zs31584.east-us-2.azure',
        role='SF_HACKATHON_ROLE',
        warehouse='SF_HACKATHON_WH',
        database='SF_HACKATHON',
        schema='HACKATHON')

    success, _, _, _ = write_pandas(cnx, df, table_name='AI_TRENDS', auto_create_table=True, overwrite=True)
    cnx.close()

def pytrends_time(kw_list, kw_ref):
    pytrends = TrendReq(hl='en-US', tz=360)
    #pytrends = TrendReq(hl='en-US', tz=360, proxies=['https://34.203.233.13:80',], retries=2, backoff_factor=0.1, requests_args={'verify':False})

    df = pd.DataFrame(columns=['name', 't_date', 'interest_static', 'interest_relative', 'reference_interest'])

    for kw in kw_list:
        pytrends.build_payload([kw], timeframe='today 5-y')
        static = pytrends.interest_over_time()

        if static.empty:
            continue

        pytrends.build_payload([kw, kw_ref], timeframe='today 5-y')
        relative = pytrends.interest_over_time()

        print(f"{kw_list.index(kw)}. {kw}")

        out = pd.merge(relative, static, on=['date'], suffixes=['_multi', '_single'])\
            .filter(items=[kw + '_multi', kw_ref, kw + '_single'])\
            .reset_index()\
            .rename(columns={'date': 't_date', kw + '_multi': 'interest_relative', kw + '_single': 'interest_static', kw_ref: 'reference_interest'})
        
        #out['t_date'] = pd.to_datetime(out['t_date']).strftime("%Y/%m/%d %H:%M:%S")
        out['t_date'] = pd.to_datetime(out['t_date']).dt.tz_localize('UTC')
        out['name'] = kw
        out = out[['name', 't_date', 'interest_static', 'interest_relative', 'reference_interest']]
        df = pd.concat([df, out], ignore_index=True)

        time.sleep(20)
        
    return df

def scrape():
    # Make a request to the website
    url = "https://www.insidr.ai/full-ai-tools-list/"
    response = requests.get(url)

    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table element using BeautifulSoup methods
    table = soup.find("table")

    # Extract the data from the table
    data = []
    for row in table.find_all("tr"):
        row_data = []
        for cell in row.find_all("td"):
            row_data.append(cell.text.strip())
        
        link = row.find('a')
        if link is not None:
            row_data.append(link.get('href'))

        data.append(row_data)
    
    rows = [[row[0], row[2], row[3], row[4]] for row in data if len(row) > 0]
    return data

if __name__ == "__main__":
    main()