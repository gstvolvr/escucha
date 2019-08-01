from collections import defaultdict
from datetime import timedelta
from search import Search
import configparser
import datetime
import logging
import os
import pandas as pd
import requests
import time

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config.ini")
    keywords_request = requests.get(config["escucha"]["keywords_url"])
    keywords = keywords_request.text.split("\n")
    today = datetime.date.today().strftime("%Y-%m-%d")

    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(filename=f"logs/{today.replace('-', '_')}.log",level=logging.INFO)
    data_dir = 'data' # local
    os.makedirs(os.path.join(data_dir, today), exist_ok=True)

    search = Search(config["escucha"]["client_path"], config["escucha"]["secret_path"], f"{data_dir}/{today}/artists_to_playlists.npz", f"{data_dir}/{today}/artist_indices.npz")

    start = time.time()
    search.search_playlists_on_keywords(keywords, True)
    delta = timedelta(seconds=time.time() - start)

    logging.info(f"total time: {delta}")
