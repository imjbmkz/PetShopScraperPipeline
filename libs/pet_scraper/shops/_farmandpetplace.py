import math
import asyncio
import requests
import pandas as pd

from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger


class FarmAndPetPlaceETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "FarmAndPetPlace"
        self.BASE_URL = "https://www.farmandpetplace.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '.content-page'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 3
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 3

    def extract(self, category):
        soup = asyncio.run(self.scrape(
            category, 'body.product-cats', wait_until="load"))

        if not soup or isinstance(soup, bool):
            print(f"[ERROR] Failed to scrape category page: {category}")
            return pd.DataFrame(columns=["shop", "url"])

        result_count = soup.find('p', class_="woocommerce-result-count")
        if result_count:
            words = result_count.get_text().split()
            n_product = next((int(w) for w in words if w.isdigit()), 0)
        else:
            n_product = 0

        n_pagination = math.ceil(n_product / 24)
        urls = []

        if n_pagination == 1:
            shop_area = soup.find('div', class_="shop-filters-area")
            if shop_area:
                urls.extend([
                    self.BASE_URL + a_tag.get('href')
                    for product in shop_area.find_all('div', class_="product")
                    if (a_tag := product.find('a')) and a_tag.get('href')
                ])
        else:
            for i in range(1, n_pagination + 1):
                base = category.split("page-")[0]
                new_url = f"{base}page-{i}.html"

                soup_pagination = asyncio.run(
                    self.scrape(new_url, 'div.shop-filters-area')
                )

                if not soup_pagination or isinstance(soup_pagination, bool):
                    logger.warning(
                        f"[WARN] Skipped pagination page: {new_url}")
                    continue

                shop_area = soup_pagination.find(
                    'div', class_="shop-filters-area")
                if shop_area:
                    urls.extend([
                        self.BASE_URL + a_tag.get('href')
                        for product in shop_area.find_all('div', class_="product")
                        if (a_tag := product.find('a')) and a_tag.get('href')
                    ])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', attrs={'itemprop': 'name'}).get_text()
            product_description = None

            if soup.find('div', class_="short-description"):
                product_description = soup.find(
                    'div', class_="short-description").get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_id = soup.find(
                'div', class_="ruk_rating_snippet").get('data-sku')

            rating_wrapper = requests.get(
                f"https://api.feefo.com/api/10/reviews/summary/product?since_period=ALL&parent_product_sku={product_id}&merchant_identifier=farm-pet-place&origin=www.farmandpetplace.co.uk")
            rating = float(rating_wrapper.json()['rating']['rating'])
            product_rating = f'{rating}/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            if soup.find('select', id="attribute"):
                variants.append(soup.find('select', id="attribute").find_all(
                    'option')[0].get('value'))
                image_urls.append(
                    soup.find('img', class_="attachment-shop_single").get('src'))
                if soup.find('div', class_="price").find('span', class_="rrp"):
                    price = float(soup.find('div', class_="price").find(
                        'span', class_="rrp").find('strong').get_text().replace('£', ''))
                    discounted_price = float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', ''))
                    discount_percentage = "{:.2f}".format(
                        (price - discounted_price) / price)

                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percentage)

                else:
                    prices.append(float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', '')))
                    discounted_prices.append(None)
                    discount_percentages.append(None)

            else:
                variants.append(None)
                image_urls.append(
                    soup.find('img', class_="attachment-shop_single").get('src'))
                if soup.find('div', class_="price").find('span', class_="rrp"):
                    price = float(soup.find('div', class_="price").find(
                        'span', class_="rrp").find('strong').get_text().replace('£', ''))
                    discounted_price = float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', ''))
                    discount_percentage = "{:.2f}".format(
                        (price - discounted_price) / price)

                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percentage)

                else:
                    prices.append(float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', '')))
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
