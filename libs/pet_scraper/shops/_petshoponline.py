import math
import asyncio
import requests
import pandas as pd
from ..etl import PetProductsETL
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from loguru import logger


class PetShopOnlineETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "PetShopOnline"
        self.BASE_URL = "https://pet-shop-online.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '.product-block-list'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 1
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 3

    def extract(self, category):
        url = self.BASE_URL+category
        soup = asyncio.run(self.scrape(url, '.product-list--collection'))
        n_product = int(soup.find(
            'p', class_="collection__products-count").get_text().replace(' products', '').replace(' product', ''))
        pagination_length = math.ceil(n_product / 24)
        urls = []

        for i in range(1, pagination_length + 1):
            soup_pagination = asyncio.run(self.scrape(
                f"{url}?page={i}", '.product-list--collection'))
            for prod_list in soup_pagination.find('div', class_="product-list--collection").find_all('div', class_="product-item--vertical"):
                urls.append(self.BASE_URL + prod_list.find('a').get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="product-meta__title").get_text()
            product_description = None

            if soup.find('div', class_="product-block-list__item--description"):
                product_description = soup.find('div', class_="product-block-list__item--description").find(
                    'div', class_="text--pull").get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            headers = {
                "User-Agent": UserAgent().random,
                'Accept': 'application/json'
            }

            product_info = requests.get(url, headers=headers)

            for variant_info in product_info.json()['product']["variants"]:
                variants.append(variant_info.get('title'))
                image_urls.append(
                    soup.find('meta', attrs={'property': "og:image"}).get('content'))

                if (variant_info.get('compare_at_price') != ""):
                    price = float(variant_info.get('compare_at_price'))
                    discount_price = float(variant_info.get('price'))
                    discount_percentage = round(
                        (price - discount_price) / price, 2)

                    prices.append(price)
                    discounted_prices.append(discount_price)
                    discount_percentages.append(discount_percentage)

                else:
                    prices.append(variant_info.get('price'))
                    discounted_prices.append(None)
                    discount_percentages.append(None)

            df = pd.DataFrame({
                "variant": variants,
                "price": prices,
                "discounted_price": discounted_prices,
                "discount_percentage": discount_percentages,
                "image_urls": image_urls
            })
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
