from selenium import webdriver
import pandas as pd
import requests
import boto3
from IPython.display import display
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options

url = "https://hfr.health.gov.ng/facilities/hospitals-search?_token=4Wll44OzOr1kZWrvOzm7FGC1y3zCYbgGs99vHRSf&state_id=124&ward_id=0&facility_level_id=0&ownership_id=0&operational_status_id=1&registration_status_id=2&license_status_id=1&geo_codes=0&service_type=0&service_category_id=0&entries_per_page=20&page=1"

options = Options()
options.headless = True
driver = webdriver.Chrome(options=options)
driver.get(url)


df = pd.read_html(driver.page_source)[0]
df.to_csv("Health_data.csv") #
display(df)
health_table = BeautifulSoup(driver.page_source, "html.parser")
driver.quit()

s3 = boto3.resource('s3',
       aws_access_key_id = 'AKIATLEGUPNKMJLK7I7R',
       aws_secret_access_key= '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',
       region_name = 'us-east-1')


object = s3.Object('s3-bucket-data-dump', 'Health_data.csv')
object.upload_file('Health_data.csv')
