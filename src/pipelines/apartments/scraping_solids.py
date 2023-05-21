# import requests
from requests_html import HTMLSession, HTMLResponse
import re
import datetime
from bs4 import BeautifulSoup
from datetime import datetime as dt

import pyspark
import pyspark.sql.functions
import pyarrow as pa
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


from dagster_aws.s3 import S3Coordinate

# from realestate.common.solids_spark_delta import upload_to_s3
# from realestate.common.solids_filehandle import json_to_gzip
# from dagster.utils.temp_file import get_temp_file_name


def apartments_region_search(area: String, max_n=100):
    """Searches Apartments.com in the area provided with a max of max_n results

    Args:
        area (String): City, State (only US)
        max_n (int, optional): Maximum results scraped. Defaults to 100.

    Returns:
        HTMLResponse: HTML response from requests-html with max_n results
    """
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
def get_listing_names(page):
    listings_name = page.html.xpath(
        '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/a[1]/@aria-label'
    )
    return listings_name


def get_listing_prices(page):
    listings_price = [
        x.text
        for x in page.html.xpath(
            '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/div[1]/a/p[1]'
        )
    ]
    return listings_price


def get_listing_amenities(page):
    listings_amenities = [
        [y.text for y in x.find("span")]
        for x in page.html.xpath(
            '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/a/p'
        )
    ]
    return listings_amenities


def get_listing_links(page):
    listings_link = page.html.xpath(
        '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/a[1]/@href'
    )
    return listings_link


def get_listing_beds(page):
    listings_beds = [
        x.text
        for x in page.html.xpath(
            '//*[@id="placardContainer"]/ul/li/article/section/div/div[2]/div/div[1]/a[1]/p[2]'
        )
    ]
    return listings_beds


def get_listings_dict(page):
    listings_name = get_listing_names(page)
    listings_price = get_listing_prices(page)
    listings_beds = get_listing_beds(page)
    listings_amenities = get_listing_amenities(page)
    listings_links = get_listing_links(page)

    listings = []
    for i, v in enumerate(listings_name):
        listings.append(
            {
                "name": v.split(",")[0],
                "price": listings_price[i],
                "beds": listings_beds[i],
                "amenities": listings_amenities[i]
                if i < len(listings_amenities)
                else [],
                "link": listings_links[i],
            }
        )

    return listings


def get_listings_pyarrow(page) -> pa.Table:
    listings = [
        get_listing_names(page),
        get_listing_prices(page),
        get_listing_beds(page),
        get_listing_amenities(page),
        get_listing_links(page),
    ]

    same_len = len(listings[0])
    for each in listings:
        if len(each) < same_len:
            each += [None] * (same_len - len(each))
    # print(len(listings[-2]))
    return pa.table(listings, names=("names", "prices", "beds", "amenities", "links"))


def get_listings(page):
    listings = [
        get_listing_names(page),
        get_listing_prices(page),
        get_listing_beds(page),
        get_listing_amenities(page),
        get_listing_links(page),
    ]

    num_listings = len(listings[0])
    with_amenities = len(listings[3])
    return [
        (
            listings[0][i].split(",")[0],
            listings[1][i],
            listings[2][i],
            listings[3][i] if i < with_amenities else [],
            listings[4][i],
        )
        for i in range(num_listings)
    ]


# apartments_region_search("PleaSaNton, cA")

session = HTMLSession()
page = session.response_hook(load_html("test_scrape.pkl"))

listings = get_listings(page)

# for each in listings:
#     print(each)
#     print(type(each))

# with open("./listings.json", "w+") as f:
#     json.dump(listings, f)
#
# print(get_listings_pyarrow(page))
