import sys
import os
from datetime import datetime
import base64
import pymysql
import pandas as pd
import numpy as np
import json
import requests
from bs4 import BeautifulSoup as BS
from urllib.request import Request, urlopen
import lxml.html
import re
import pickle
import urllib.request

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

default_args = {
    'start_date':datetime(2022, 7, 1),
}


def get_reviews_by_page(prd_id, page_num):
    cookies = {
    'NNB': 'IDMGADL3MHIWC',
    'AD_SHP_BID': '27',
    '_ga_4BKHBFKFK0': 'GS1.1.1642402376.2.1.1642402379.0',
    'ASID': 'dc763f370000017e66d652af00000061',
    'MM_NEW': '1',
    'NFS': '2',
    '_ga': 'GA1.2.2106199447.1642400290',
    '_ga_7VKFYR6RV1': 'GS1.1.1645429964.2.0.1645429964.60',
    'autocomplete': 'use',
    'nx_ssl': '2',
    'ncpa': '"6148|l2e3zkv4|cdf8e41eb25f6af897ae99fe3fa6ed973aebe219|s_124fd409a50b|c1c4719e7921bf013624130d6d96c1956ac7b3b2:95694|l2ucqnlk|51bfe6675daed5b2a745736a747eb9ec6419ddad|95694|0d82818b487a4e234125e83eec99c34021630735"',
    'sus_val': 'S0F1UsdM+HwwniR8Tq3qX19O',
    'spage_uid': '',
    }

    headers = {
        'authority': 'search.shopping.naver.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        # Requests sorts cookies= alphabetically
        # 'cookie': 'NNB=IDMGADL3MHIWC; AD_SHP_BID=27; _ga_4BKHBFKFK0=GS1.1.1642402376.2.1.1642402379.0; ASID=dc763f370000017e66d652af00000061; MM_NEW=1; NFS=2; _ga=GA1.2.2106199447.1642400290; _ga_7VKFYR6RV1=GS1.1.1645429964.2.0.1645429964.60; autocomplete=use; nx_ssl=2; ncpa="6148|l2e3zkv4|cdf8e41eb25f6af897ae99fe3fa6ed973aebe219|s_124fd409a50b|c1c4719e7921bf013624130d6d96c1956ac7b3b2:95694|l2ucqnlk|51bfe6675daed5b2a745736a747eb9ec6419ddad|95694|0d82818b487a4e234125e83eec99c34021630735"; sus_val=S0F1UsdM+HwwniR8Tq3qX19O; spage_uid=',
        'referer': 'https://search.shopping.naver.com/catalog/11180765626?query=%ED%85%90%ED%8A%B8&NaPm=ct%3Dl2pqilo8%7Cci%3D692b32d7f59460de272136a51743c0fdfcd658f0%7Ctr%3Dslsl%7Csn%3D95694%7Chk%3D8608eff24f89eb17897e60155517dc3307ffcd6e',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36',
    }

    params = {
        'nvMid': prd_id,
        'reviewType': 'ALL',
        'sort': 'QUALITY',
        'isNeedAggregation': 'N',
        'isApplyFilter': 'N',
        'page': page_num,
        'pageSize': '20',
    }

    response = requests.get('https://search.shopping.naver.com/api/review', params=params, cookies=cookies, headers=headers)
    reviewlist = json.loads(response.text)
    
    list_reviews = []
    
    for i in range(0, 20):
        
        list_review = []
        
        # prd_id
        if 'matchNvMid' in reviewlist['reviews'][i]:
            list_review.append(reviewlist['reviews'][i]['matchNvMid'])
        else:
            list_review.append("NaN")
        
        # prd_url
        if 'pageUrl' in reviewlist['reviews'][i]:
            list_review.append(reviewlist['reviews'][i]['pageUrl'])
        else:
            list_review.append("NaN")
            
        # user_id
        if 'userId' in reviewlist['reviews'][i]:
            list_review.append(reviewlist['reviews'][i]['userId'])
        else:
            list_review.append("NaN")
        
        # content
        if 'content' in reviewlist['reviews'][i]:
            list_review.append(reviewlist['reviews'][i]['content'])
        else:
            list_review.append("NaN")
        
        # register_date
        if 'registerDate' in reviewlist['reviews'][i]:
            list_review.append(reviewlist['reviews'][i]['registerDate'])
        else:
            list_review.append("NaN")
        
        # quality_score
        if 'qualityScore' in reviewlist['reviews'][i]:
            list_review.append(reviewlist['reviews'][i]['qualityScore'])
        else:
            list_review.append("NaN")
            
        # star_score
        if 'starScore' in reviewlist['reviews'][i]:
            list_review.append(reviewlist['reviews'][i]['starScore'])
        else:
            list_review.append("NaN")
        
        # image_url
        if 'images' in reviewlist['reviews'][i]:
            list_review.append(reviewlist['reviews'][i]['images'])
        else:
            list_review.append("NaN")
        
        list_reviews.append(list_review)
    
    cols = ['prd_id', 'prd_url', 'user_id', 'review', 'uploaded_date', 'quality_score', 'star_score', 'image_url']   
    return pd.DataFrame(list_reviews, columns = cols)


def save_review(**kwargs):
    prd_id = kwargs['dag_run'].conf.get('prd_id')
    page_end_num = kwargs['dag_run'].conf.get('page_end_num')

    list_df = []
    for i in range(1, page_end_num+1):
        list_df.append(get_reviews_by_page(prd_id, i))
    df = pd.concat(list_df)
    df['image_url'] = df['image_url'].astype(str)
    df.to_pickle("/home/ec2-user/airflow/df_storage/df_{}.pkl".format(prd_id))
    #return df


def insert_to_db(**kwargs):
    prd_id = kwargs['dag_run'].conf.get('prd_id')
    df = pd.read_pickle("/home/ec2-user/airflow/df_storage/df_{}.pkl".format(prd_id))

    con = pymysql.connect(host='xx.ap-northeast-2.rds.amazonaws.com', user='xx', password='xx' 
                          ,port=3306, database = 'crawling_dag', use_unicode=True, charset='utf8')
    cur = con.cursor()
    sql = """INSERT INTO tent_review_dag(prd_id, prd_url, user_id, review, uploaded_date, quality_score, star_score, image_url) VALUES
         (%s, %s, %s, %s, %s, %s, %s, %s) """
    for row in df.iterrows():
        try:
            cur.execute(sql, row[1].tolist())
        except Exception as e:
            print(e)
            break
    con.commit()


with DAG(
    dag_id='crawling_and_inserting',
    #schedule_interval='@daily',
    default_args=default_args,
    description='tent product review crawling and inserting into DB',
    tags=['crawling_inserting_2022-07-17'],
    render_template_as_native_obj=True,
    catchup=False
) as dag:

    task_crawling = PythonOperator(
        task_id='crawling_tent_reviews',
        python_callable=save_review,
        #op_kwargs={'prd_id':'', 'page_end_num':''},
        provide_context=True,
        dag=dag
    )

    task_inserting_to_db = PythonOperator(
        task_id='inserting_data_to_mysql',
        python_callable=insert_to_db,
        provide_context=True,
        dag=dag
    )


task_crawling >> task_inserting_to_db
