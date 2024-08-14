import requests
from bs4 import BeautifulSoup
import re
import time
import random
import pandas as pd

from .types_realestate import PropertyDataFrame, SearchCoordinate


SearchCoordinate.description
from dagster import (
    Int, 
    op,
    Field,
    String,
    LocalFileHandle,
    Out,
    Output,
    OpExecutionContext
)

@op(
    description="""This will scrape domain for listing ids and then use the dev API to retrieve values""",
    config_schema={
        "domain_main_url": Field(
            String,
            default_value="https://api.domain.com.au/v1/",
            is_required=False,
            description=(
                """Main URL to start the search with (propertyType unspecific).
            No API will be hit with this request. This is basic scraping."""
            ),
        ),
        "domain_search_url": Field(
            String,
            default_value="https://www.domain.com.au/",
            is_required=False,
            description=(
                """Main URL to start the search with (propertyType specific).
            No API will be hit with this request. This is basic scraping."""
            ),
        ),
    },
    required_resource_keys={"fs_io_manager"},
    out=Out(io_manager_key="fs_io_manager"),

)
def list_props(context: OpExecutionContext, searchCriteria: SearchCoordinate) -> PropertyDataFrame:
        headers = { 
	        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", 
	            "Referer": "https://www.domain.com.au/"
        }
        listPropertyIds = []
        
        base_url = (
            context.op_config["domain_search_url"]
            + searchCriteria["rentOrBuy"]
            + "/"
            + searchCriteria["city"]
            + "/"
        )
        context.log.info(f"Search url: {url}")
        
        listing_ids = []
        for i in range(1,6):
            url = f"{base_url}?page={i}"
            response = requests.get(url, headers=headers)
            if response.ok:
                soup = BeautifulSoup(response.content, 'html')
                elements_with_class = soup.find_all(class_='css-1qp9106')

                for element in elements_with_class:
                    data_testid_value = element.get('data-testid')
                    if data_testid_value:
                        listing_id = re.sub(r'\D', '', data_testid_value)
                        listing_ids.append(listing_id)
            else:
                context.log.info(response)

        headers = {
            'X-API-Key': 'key_07e6c61e2af44856103c7a58aa5339aa'
            }

        base_url = context.op_config["domain_main_url"]
        context.log.info("Authenticating...")
        authenticate = requests.get(base_url + "me", headers=headers)
        context.log.info(f"Status Code: {authenticate.status_code}")
        url = base_url + f"listings/{listing_ids[i]}"
        





            

        
        




@op(
    description="""This will scrape immoscout without hitting the API (light scrape)""",
    config_schema={
        "immo24_main_url_en": Field(
            String,
            default_value="https://www.immoscout24.ch/en/",
            is_required=False,
            description=(
                """Main URL to start the search with (propertyType unspecific).
            No API will be hit with this request. This is basic scraping."""
            ),
        ),
        "immo24_search_url_en": Field(
            String,
            default_value="https://www.immoscout24.ch/en/real-estate/",
            is_required=False,
            description=(
                """Main URL to start the search with (propertyType specific).
            No API will be hit with this request. This is basic scraping."""
            ),
        ),
    },
    required_resource_keys={"fs_io_manager"},
    out=Out(io_manager_key="fs_io_manager"),

)
def list_props_immo24(context, searchCriteria: SearchCoordinate) -> PropertyDataFrame:
    # propertyType : str, rentOrBuys : str, radius : int, cities : str)
    listPropertyIds = []

    # find max page
    url = (
        context.op_config["immo24_search_url_en"]
        + searchCriteria["rentOrBuy"]
        + "/city-"
        + searchCriteria["city"]
        + "?r="
        + str(searchCriteria["radius"])
        + "&map=1"  # only property with prices (otherwise mapping id to price later on does not map)
        + ""
    )
    context.log.info("Search url: {}".format(url))
    html = requests.get(url)
    soup = BeautifulSoup(html.text, "html.parser")
    buttons = soup.findAll("a")
    p = []
    for item in buttons:
        if len(item.text) <= 3 & len(item.text) != 0:
            p.append(item.text)
    if p:
        lastPage = int(p.pop())
    else:
        lastPage = 1
    context.log.info("Count of Pages found: {}".format(lastPage))

    ids = []
    propertyPrice = []
    for i in range(1, lastPage + 1):
        url = (
            context.op_config["immo24_main_url_en"]
            + searchCriteria["propertyType"]
            + "/"
            + searchCriteria["rentOrBuy"]
            + "/city-"
            + searchCriteria["city"]
            + "?pn="  # pn= page site
            + str(i)
            + "&r="
            + str(searchCriteria["radius"])
            + "&se=16"  # se=16 means most recent first
            + "&map=1"  # only property with prices (otherwise mapping id to price later on does not map)
        )
        context.log.debug("Range search url: {}".format(url))

        html = requests.get(url)
        soup = BeautifulSoup(html.text, "html.parser")
        links = soup.findAll("a", href=True)
        hrefs = [item["href"] for item in links]
        hrefs_filtered = [href for href in hrefs if href.startswith("/" + searchCriteria['rentOrBuy'] + "/")]
        ids += [re.findall("\d+", item)[0] for item in hrefs_filtered]

        # get normalized price without REST-call
        h3 = soup.findAll("span")
        for h in h3:
            text = h.getText()
            if "CHF" in text or "EUR" in text:
                start = text.find("CHF") + 4
                end = text.find(".â\x80\x94")

                price = text[start:end]

                # remove all characters except numbers --> equals price
                price = re.sub("\D", "", price)
                propertyPrice.append(price)

    # merge all ids and prices to dict
    dictPropPrice = {}
    for j in range(len(ids)):
        dictPropPrice[ids[j]] = propertyPrice[j]

    propDict = {}
    # loop through dict
    for id in dictPropPrice:
        last_normalized_price = dictPropPrice[id]

        listPropertyIds.append(
            {
                "id": id,  # name = parition
                "fingerprint": str(id) + "-" + str(last_normalized_price),
                "is_prefix": False,
                "rentOrBuy": searchCriteria["rentOrBuy"],
                "city": searchCriteria["city"],
                "propertyType": searchCriteria["propertyType"],
                "radius": searchCriteria["radius"],
                "last_normalized_price": str(last_normalized_price),
            }
        )

    return listPropertyIds





headers = { 
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", 
	"Referer": "https://www.domain.com.au/"
}

base_url = "https://www.domain.com.au/rent/"
errCount = 0
listing_ids = []

for i in range(1,6):
    if i > 1:
        url = f"{base_url}?page={i}"
    else:
        url = base_url
    
    response = requests.get(url, headers=headers)
    if response.ok:
        soup = BeautifulSoup(response.content, 'html')
        elements_with_class = soup.find_all(class_='css-1qp9106')
        
        for element in elements_with_class:
            data_testid_value = element.get('data-testid')
            if data_testid_value:
                listing_id = re.sub(r'\D', '', data_testid_value)
                listing_ids.append(listing_id)
    else:
        print(response)
        errCount += 1

headers = {
    'X-API-Key': 'key_07e6c61e2af44856103c7a58aa5339aa'
    }

base_url = 'https://api.domain.com.au/v1/me'

authenticate = requests.get(base_url, headers=headers)

print(authenticate.status_code)

base_url = 'https://api.domain.com.au/v1/listings/'
url = f"{base_url}{listing_ids[0]}"
response = requests.get(url, headers=headers)

if response.ok:
    listing = response.json()
else:
    raise RuntimeError

titles = list(listing.keys())

df = pd.DataFrame(columns = titles)
