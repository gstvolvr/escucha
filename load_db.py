from google.cloud import firestore
import numpy as np
import pandas as pd
import scipy as sp
import scipy.sparse
import constants
import time

def load_recommendations(artists, graph, index_id_map, ignore_top=0.05):
    """
    load recommendations for the top artists
    {artist-id}
       {artist info}
       {recommendation-ignore-top-5%}
           {similar artist x}
             {similar artist info}
           ....
           {similar artist x+100}
             {similar artist info}
       {recommendation-ignore-top-10%}
           {similar artist r}
             {similar artist info}
           ....
           {similar artist r+100}
             {similar artist info}
    """

    db = firestore.Client()
    # id|node_name|playlist_degree|followers|genres|image_url|popularity|spotify_url|connections|index
    artists.sort_values(by="connections", ascending=False, inplace=True)
    artists["image_url"].fillna(constants.DEFAULT_IMG_URL, inplace=True)
    artists["genres"].fillna(constants.DEFAULT_STR, inplace=True)

    collection_name = "spotify-ids"

    num_artists = artists.shape[0]
    num_recs = 100 # max subcollections in firebase

    ignore_artists_rank_by_index = set(artists.iloc[:int(ignore_top*num_artists)]["index"].values)
    top_artists = artists.iloc[:int(ignore_top*num_artists)]

    num_artists_to_load = 750

    for aid, artist_row in artists.iloc[:num_artists_to_load, :].iloc[:num_artists_to_load].iterrows():

        # sort by connections, and remove the first which is almost always itself
        ## if by chance there's another artists that shows up in every playlist with this artist, it could be ranked 1st — I'll take my chances
        i = artist_row["index"]
        similar_artists = np.argsort(graph[i, :].toarray()[0])[-2::-1]

        # ignore recommendations from the bottom percentile or artists with less than 50 playlists in common
        min_score = min(np.quantile(similar_artists, 0.01), 50)

        # aritsts -> id|node_name|playlist_degree|followers|genres|image_url|popularity|spotify_url
        artist_info = {
            u"name": artist_row["node_name"],
            u"playlist_degree": float(artist_row["playlist_degree"]),
            u"spotify_url": artist_row["spotify_url"],
            u"image_url": artist_row["image_url"],
            u"followers": float(artist_row["followers"]),
            u"popularity": float(artist_row["popularity"]),
            u"genres": artist_row["genres"],
            u"connections":  artist_row["connections"]
        }

        artist_ref = db.collection(collection_name).document(aid)
        artist_ref.set(artist_info)
        rec_ref = artist_ref.collection(f"recommendations-{ignore_top*100}")

        rec_count = 0
        for rank, j in enumerate(similar_artists):

            # once we've reached the max number of recommendations, move on to the next artist
            if rec_count == num_recs:
                break
            if j in ignore_artists_rank_by_index:
                continue

            rec_count += 1
            rec_score = graph[i, j] # number of connections betwee i and j

            # we don't care about recommendations that have less than `min_score` playlists in common
            if rec_score < min_score:
                break

            rec_aid = index_id_map.loc[j, "id"]

            aritsts -> id|node_name|playlist_degree|followers|genres|image_url|popularity|spotify_url
            rec_info = {
                u"id": rec_aid,
                u"name": artists.loc[rec_aid, "node_name"],
                u"playlist_degree": float(artists.loc[rec_aid, "playlist_degree"]),
                u"spotify_url": artists.loc[rec_aid, "spotify_url"],
                u"image_url": artists.loc[rec_aid, "image_url"],
                u"followers": float(artists.loc[rec_aid, "followers"]),
                u"popularity": float(artists.loc[rec_aid, "popularity"]),
                u"genres": artists.loc[rec_aid, "genres"],
                u"score": float(rec_score)
            }
            rec_ref.document(str(rank)).set(rec_info)

def artists_graph(inpath, outpath=None):
    """load artist by playlist matrix

    dot product with it's transpose provides an artist by artist matrix containing the number playlists in common between any two artists.
    """
    m = sp.sparse.load_npz(inpath)
    graph = m.dot(m.T)

    if outpath:
      sp.sparse.save_npz(outpath, graph)

    return graph

if __name__ == '__main__':

    # this is a pre computed file of artist information
    ## this will be replaced in future realeases with API queries to the Spotify Artist API from JavaScript
    rich_artists = pd.read_csv("data/2019-07-30/ranked_rich_artists.bsv", sep="|", index_col="id")

    # output from query.py
    index_id_map = pd.read_feather("data/2019-07-30/artist_indices.feather")
    index_id_map.columns = ["id", "index"]
    index_id_map.set_index("index", inplace=True)

    # output from query.py
    graph = artists_graph("data/2019-07-30/artists_to_playlists.npz")
    load_recommendations(rich_artists, graph, index_id_map, 0.0001)
    load_recommendations(rich_artists, graph, index_id_map, 0.0005)
    load_recommendations(rich_artists, graph, index_id_map, 0.0025)
    load_recommendations(rich_artists, graph, index_id_map, 0.01)
    load_recommendations(rich_artists, graph, index_id_map, 0.02)
