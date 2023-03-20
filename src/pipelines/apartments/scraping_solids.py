import requests
import re
import datetime
from bs4 import BeautifulSoup
from datetime import datetime as dt
import json
import pickle

import gzip
import shutil

# from io import BytesIO

# from realestate.common.helper_functions import json_zip_writer


from dagster import (
    Any,
    Int,
    # solid,
    Field,
    String,
    # OutputDefinition,
    # composite_solid,
    FileHandle,
    LocalFileHandle,
    ExpectationResult,
    # EventMetadataEntry,
    Output,
)
# from .types_realestate import PropertyDataFrame, SearchCoordinate, JsonType


from dagster_aws.s3.solids import S3Coordinate

# from realestate.common.solids_spark_delta import upload_to_s3
# from realestate.common.solids_filehandle import json_to_gzip
# from dagster.utils.temp_file import get_temp_file_name

def apartments_region_search(area: String, max_n=100):
    # only USA
    # city, state
    usa = area.split(',')
    city = usa[0].strip().lower()
    state = usa[1].strip().lower()

    base_url = 'https://www.apartments.com/'
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
               }
    page = requests.get(base_url + city + '-' + state, headers=headers)

    bs = BeautifulSoup(page.content, 'html.parser')
    listings_find = bs.find_all('div', class_='content-wrapper')
    listings = {}
    for each in listings_find:
        listings[each.find('a').get('aria-label').split(',')[0]] = each

    with open('./test_scrape.json', 'w') as f:
        json.dump(listings, f)
    


apartments_region_search('PleaSaNton, cA')