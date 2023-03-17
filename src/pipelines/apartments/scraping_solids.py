import requests
import re
import datetime
from bs4 import BeautifulSoup
from datetime import datetime as dt
import json

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

def apartments_region_search(area: String, max_n=1000):
    # only USA
    # city, state
    usa = area.split(',')
    city = usa[0].strip().lower()
    state = usa[1].strip().lower()

    bs = BeautifulSoup()
    

apartments_region_search('Pleasanton, CA')