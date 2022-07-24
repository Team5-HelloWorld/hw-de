import os
import time
#import threading
#import subprecess import run

os.system("nohup airflow webserver &")
time.sleep(30)
os.system("cd airflow | nohup airflow scheduler &")
time.sleep(30)

os.system("""airflow dags trigger crawling_and_inserting --conf '{"prd_id":"19751131585", "page_end_num":9}'""")
time.sleep(180)
os.system("""airflow dags trigger crawling_and_inserting --conf '{"prd_id":"22895277426", "page_end_num":9}'""")
time.sleep(180)
os.system("""airflow dags trigger crawling_and_inserting --conf '{"prd_id":"22412166138", "page_end_num":6}'""")
time.sleep(180)
os.system("""airflow dags trigger crawling_and_inserting --conf '{"prd_id":"27413049522", "page_end_num":10}'""")
time.sleep(180)
os.system("""airflow dags trigger crawling_and_inserting --conf '{"prd_id":"27011310522", "page_end_num":7}'""")
time.sleep(180)
os.system("""airflow dags trigger crawling_and_inserting --conf '{"prd_id":"21033853139", "page_end_num":5}'""")
time.sleep(300)

os.system("ps -ef | grep task webserver | kill -9 pid")
os.system("ps -ef | grep task airflow scheduler | kill -9 pid")
