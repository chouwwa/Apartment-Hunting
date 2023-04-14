# import requests
from requests_html import HTMLSession, HTMLResponse
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
    usa = area.split(",")
    city = usa[0].strip().lower()
    state = usa[1].strip().lower()

    base_url = "https://www.apartments.com/"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    }

    # Start session and get HTMLResponse
    page = get_url(base_url + city + "-" + state, headers=headers)
    # page = get_url(
    #     "https://webscraper.io/test-sites/e-commerce/allinone/phones", headers=headers
    # )

    # Get list of items with css
    # listings = page.html.xpath("/html/body/div[1]/div[3]/div/div[2]/div/div/div/div[1]")
    # listings = page.html.xpath(
    #     '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div'
    # )

    # print(listings)

    # save_html(page, "test_scrape.pkl")

    return page


def get_url(url, headers) -> HTMLResponse:
    session = HTMLSession()
    return session.response_hook(session.get(url, headers=headers))


def xpath_find(page, path):
    return page.html.xpath(path)


def save_html(page, fn):
    with open("./{fn}".format(fn=fn), "wb") as f:
        pickle.dump(page, f)


def load_html(fn):
    with open("./{fn}".format(fn=fn), "rb") as f:
        return pickle.load(f)


# print(
#     page.html.xpath(
#         '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div[1]/a'
#     )
# )


def get_listings(page):
    listings_name = page.html.xpath(
        '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/a[1]/@aria-label'
    )
    listings_price = page.html.xpath(
        '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/div[1]/a/p[1]'
    )
    listings_amenities = [
        x.innerText
        for x in page.html.xpath(
            '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/a/p'
        )
    ]
    # listings_address = page.html.xpath("")
    listings_link = page.html.xpath(
        '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/a[1]/@href'
    )
    listings_beds = page.html.xpath(
        '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/div[1]/a[1]/p[2]'
    )
    listings = []
    for i, v in enumerate(listings_name):
        listings.append(
            {
                "name": v.split(",")[0],
                "price": listings_price[i],
                "beds": listings_beds[i],
                "amenities": listings_amenities[i]
                if i < len(listings_amenities)
                else None,
                "link": listings_link[i],
            }
        )

    return listings


# apartments_region_search("PleaSaNton, cA")

session = HTMLSession()
page = session.response_hook(load_html("test_scrape.pkl"))

listings = get_listings(page)

for i in listings[0]:
    print(type(listings[0][i]))
# with open("./listings.json", "w+") as f:
#     json.dump(get_listings(page), f)
