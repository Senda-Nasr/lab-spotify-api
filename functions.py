
 # to download html code
import requests
from bs4 import BeautifulSoup # to navigate through the html code
import pandas as pd
import numpy as np
import re

def scrape_hot100():
    hot_songs_url = "https://www.billboard.com/charts/hot-100/"
    hot_songs_response = requests.get(hot_songs_url)
    hot_songs_soup = BeautifulSoup(hot_songs_response.text, 'html.parser')
    
    if hot_songs_response.status_code == 200:
        
        hot_songs_titles = [elem.get_text().replace("\n", "").replace("\t", "") for elem in hot_songs_soup.select("div  ul  li  ul  li  h3")]
        hot_songs_artists = [elem.parent.find_all("span")[0].get_text().replace("\n", "").replace("\t","") for elem in hot_songs_soup.select("div ul li ul li h3")]
        
        hot_songs_df = pd.DataFrame({'artist': hot_songs_artists, 'title': hot_songs_titles})
        hot_songs_df.to_csv("hot_100.csv", index=False)
        return hot_songs_df

import pandas as pd

def remove_hot_songs(df1, df2)-> pd.DataFrame:
    
    #converting all values of both dataframes to lower case
    df1 = df1.applymap(lambda x: x.lower() if isinstance(x, str) else x)    
    df2= df2.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    
    # performing a left merge to obtain the rows in the not hot songs that are present in the hot songs
    left_merge_df = df1.merge(df2, indicator=True, how='left')
    
    # keeping only the rows that are only present in the not hot dataframe
    result_df= left_merge_df.query('_merge == "left_only"').drop('_merge', axis=1)
    
    return result_df

import sys
from config import *
import pandas as pd
import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials
import numpy as np
import time

#Initialize SpotiPy with user credentials 
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=Client_ID,
                                                           client_secret=Client_Secret))

sp._session.timeout = 10



def search_song(title:str, artist:str=None, lim: int = 5):
    
    """
    function to get the song id, based on the title and artist:
    """
    
    if artist:
        query = f"track:{title} artist:{artist}"
    else:
        query = f"track:{title}"
    try:
        results = sp.search(q=query)
        tracks = results['tracks']['items']
        if not tracks:
            print("Song", title, "from", artist, "not found!")
            return pd.DataFrame()
        
        # Extract relevant information from each track
        records = []
        for track in tracks[:lim]:
            record = {
                'title': track['name'],
                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                'id': track['id']
            }
            records.append(record)
            
        # Create DataFrame from the list of records
        df = pd.DataFrame(records)
        return df
    
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()
    
    

def get_audio_features_for_chunks(sp, list_of_song_ids, chunk_size=50, sleep_time=20):
    
    """
    function to retrieve the audio features for the different chunks we have created before
    """
    
    # Split the list_of_song_ids into chunks of size chunk_size
    song_id_chunks = [list_of_song_ids[i:i + chunk_size] for i in range(0, len(list_of_song_ids), chunk_size)]

    # Create an empty DataFrame to store the audio features
    df_audio_features = pd.DataFrame()

    # Iterate through each chunk
    for chunk in song_id_chunks:
        time.sleep(sleep_time) 
        my_dict = sp.audio_features(chunk)

        # Check if my_dict is not None and contains elements before creating a DataFrame
        if my_dict and isinstance(my_dict, list) and len(my_dict) > 0:
            # Create a new dictionary with a more structured format
            my_dict_new = {key: [item[key] for item in my_dict] for key in my_dict[0]}

            # Create a DataFrame from the audio features and append it to df_audio_features
            df_chunk = pd.DataFrame(my_dict_new)
            df_audio_features = pd.concat([df_audio_features, df_chunk], ignore_index=True)

    return df_audio_features



def add_audio_features(df, audio_features_df):
    """
    Merge a given DataFrame with the audio features DataFrame based on a specified column and remove all duplicates.

    Parameters:
    - df: Original DataFrame
    - audio_features_df: DataFrame containing audio features
    - merge_column: Column to merge on (default is 'id')

    Returns:
    - Merged and de-duplicated DataFrame
    """
   
    # Merge DataFrames
    merged_df = pd.merge(df, audio_features_df, on="id", how='inner')


    # Remove all duplicates from the merged DataFrame
    merged_df = merged_df.drop_duplicates()

    return merged_df
