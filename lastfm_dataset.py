import pandas as pd
import numpy as np

source_path = "/media/bogdan/2084D71E84D6F4EE/MyData/School/Year1Block1/IDS/Ass4/lastfm-dataset-1K/" \
              "userid-timestamp-artid-artname-traid-traname.tsv"


df_raw = pd.read_csv(source_path, sep='\t', nrows=100000)
df_raw.columns = ['user_id', 'timestamp', 'artist_id', 'artist_name', 'track_id', 'track_name']
# for col in df_raw:
#     print("------------")
#     print (df_raw[col].isnull().sum())

# Generate new artificial ID for each song
df_raw['artist_track'] = df_raw['artist_name'] + '|||' + df_raw['track_name']

# Drop redundant info
df = df_raw.drop(['timestamp', 'artist_id', 'track_id','artist_name', 'track_name'], axis=1)
df['play_count'] = np.nan
print(df)
print('--------')
grouped = df.groupby(['user_id', 'artist_track'])
print(grouped.groups)
