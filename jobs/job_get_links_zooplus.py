import sys
sys.path.append("/home/josh/airflow/libs")

import os
import sys
import argparse
import datetime as dt
from loguru import logger
from pet_scraper.connection import Connection
from pet_scraper.factory import SHOPS, run_etl

SHOP_NAME = "Zooplus"

if __name__=="__main__":
    start_time = dt.datetime.now()
    logger.remove()
    logger.add("logs/std_out.log", rotation="10 MB", level="INFO")
    logger.add("logs/std_err.log", rotation="10 MB", level="ERROR")
    logger.add(sys.stdout, level="INFO")
    logger.add(sys.stderr, level="ERROR")

    logger.info(f"{SHOP_NAME} Scraper has started")
    client = run_etl(SHOP_NAME)

    client.get_links_by_category()
    end_time = dt.datetime.now()
    duration = end_time - start_time
    logger.info(f"{SHOP_NAME} Links Scraper (shop={SHOP_NAME}) has ended. Elapsed: {duration}")
