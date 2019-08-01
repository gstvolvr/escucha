from datetime import timedelta
import argparse
import client_credentials_flow
import collections
import constants
import datetime
import hashlib
import itertools
import logging
import numpy as np
import os
import pandas as pd
import requests
import scipy as sp
import scipy.sparse
import time

class Search:

    def __init__(self, client_path, secret_path, graph_path, artists_path):
        self.graph = None
        self.client_path = client_path
        self.secret_path = secret_path
        self.graph_path =  graph_path
        self.artists_path = artists_path

        # convert spotify id to int for sparse indexing
        self.artist_indices = collections.defaultdict(lambda:-1) # avoid collision with 0 index
        self.artist_index = 0
        self.playlist_index = 0

        # keep track of processed user ids and playlists in memory — but don't store them
        self.checked_ids = set([])
        self.checked_playlists = set([])

        # initialize new authentication for each instance
        self.session = None
        self._authenticate()
        self.header = None
        self.naptime = 0.0005

        self.num_users = 0
        self.start_time = time.time()

    def _authenticate(self, renew=True):
        """get latest client credential flow key - it expires every 60 minutes"""

        if renew:
            token = client_credentials_flow.create_token(self.client_path, self.secret_path)
        self.session = requests.Session()
        self.headers = { "Authorization": f"Bearer {token}" }

    def _init_graph(self, load_existing):
        """initialize sparse matrix of size num aritsts x num playlists"""

        if load_existing:
            logging.info("loading existing graph")
            # read coordinate format and conver to lists of lists
            self.graph = sp.sparse.load_npz(self.graph_path).tolil()
            dd = collections.defaultdict(lambda:-1)
            self.artist_indices = pd.read_feather(self.artists_path).set_index("index").to_dict(orient="dict", into=dd)['artist_index']
        else:
            # educated guess on the size of the matrix
            logging.info("initializing graph")
            n_artists = int(2e6)
            n_playlists = int(1e7)
            self.graph = sp.sparse.lil_matrix((n_artists, n_playlists), dtype=np.int16)

    def _dump_graph(self):
        """write graph and artists indices to local file"""
        df = pd.DataFrame.from_dict(self.artist_indices, orient='index', columns=["artist_index"]).reset_index()
        df.to_feather(self.artists_path)
        # convert coordinate format first since scipy doens't support Save for lil_matrix
        sp.sparse.save_npz(self.graph_path, self.graph.tocoo())
        time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%m')
        logging.info(f"dumping graph and artist indices at {time_now}")

    def _check_request(self, href):
        """check if a request is valid, otherwise update the token"""

        try:
            request = self.session.get(href, headers=self.headers)
        except:
            logging.error("failed on: ", href)
            return {}

        if request.status_code != requests.codes.ok:
            if request.status_code in constants.status_code_descriptions:
                logging.warn(f"{request.status_code} -> {constants.status_code_descriptions[request.status_code]}")
            if request.status_code == 429:
                self.naptime *= 1.05 # TODO: revisit effects on long running jobs
                secs = int(request.headers["Retry-After"])
                logging.warn(f"sleeping for {secs*3} seconds")
                time.sleep(secs)
            elif request.status_code == 401:
                self._authenticate()
            else:
                logging.warn(f"{request.status_code} -> unknown")

            try:
                request = self.session.get(href, headers=self.headers).json()
                return request
            except:
                logging.error(f"failed on: {href}", href)
                return {}

        time.sleep(self.naptime)
        return request.json()

    def search_playlists_on_keywords(self, keywords, load_existing=False):
        """find playlists containing the {keyword} using Spotify Playlist API"""

        self._init_graph(load_existing)
        count = 0

        for keyword in keywords:
            k_start = time.time()
            logging.info(f"\nkeyword numer {count} = {keyword.strip()}")
            self._search_playlists_on_keyword(keyword)
            k_delta = timedelta(seconds=time.time() - k_start)
            logging.info(f"keyword numer {count} = {keyword} took, {k_delta}")
            self._dump_graph()
            count += 1

    def _search_playlists_on_keyword(self, keyword):
        """"find playlists containing the {keyword} using Spotify Playlist API — then search for it's user's public playlists"""

        href = f"https://api.spotify.com/v1/search?q={keyword}&type=playlist&market=us&limit=50"
        request = self._check_request(href)
        loop = True

        while "playlists" in request and loop:
            for playlist in request["playlists"]["items"]:
                user = playlist["owner"]["id"]
                u_start = time.time()
                self._search_playlists_on_user(user)
                u_delta = timedelta(seconds=time.time() - u_start)
                playlist_rate = self.playlist_index / ((time.time() - self.start_time) / 60.)

                logging.debug(f"\n\nplaylist name from search: {playlist['name']}")
                logging.debug(f"user number {self.num_users} from search: {user}")
                logging.info(f"user number {self.num_users} took: {u_delta}")
                logging.info(f"total playlists processed: {self.playlist_index}")
                logging.info(f"processing at a rate of {playlist_rate: 0.2f} / minute")

            if "next" in request["playlists"] and request["playlists"]["next"]:
                request = self._check_request(request["playlists"]["next"])

            else:
                loop = False

    def _search_playlists_on_user(self, user):
        """find all playlists made by {user}

        avoid storing or logging any user ids
        """

        user = user.replace("#", "%23").replace("?", "%3F").replace("%", "%25")
        href = f"https://api.spotify.com/v1/users/{user}/playlists?market=us&limit=50"

        request = self._check_request(href)

        if user in self.checked_ids:
            logging.warn("already checked this user")
            return

        self.checked_ids.update([user])
        self.num_users += 1

        while "items" in request:
            public_playlists = request["items"]
            self._update_graph(public_playlists)

            if request["next"]: # can be None
                request = self._check_request(request["next"])
            else:
                return

    def _update_graph(self, playlists):
        """
        add artist -> playlist relationships in graph
        """

        for playlist in playlists:
            pid = playlist["id"]

            if pid in self.checked_playlists:
                logging.debug(f"\t\taleady processed: {playlist['name']}")
                logging.warn("playlist has already been processed")
                continue
            else:
                logging.debug(f"playlist number {self.playlist_index}: {playlist['name']}")
                p_index = self.playlist_index
                self.playlist_index += 1
                self.checked_playlists.update([pid])

            href = playlist["tracks"]["href"]
            tracks = self._check_request(href)

            if "items" not in tracks:
                continue

            prev_aid = -1
            playlist_artists = set([])
            for item in tracks["items"]:
                if item["track"] and "artists" in item["track"]:
                    for artist in item["track"]["artists"]:
                        aid = artist["id"]

                        # get lil matrix index for current artist
                        a_index = self.artist_indices[aid]

                        # avoid dictionary look ups for same back to back artist
                        if prev_aid == aid or aid in playlist_artists:
                            continue

                        playlist_artists.update([aid])

                        if a_index == -1:
                            a_index = self.artist_index
                            self.artist_indices[aid] = self.artist_index
                            self.artist_index += 1

                        self.graph[a_index, p_index] += 1
                        prev_aid = aid
