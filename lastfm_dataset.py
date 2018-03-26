import pandas as pd
import random

source_path = "/media/bogdan/2084D71E84D6F4EE/MyData/School/Year1Block1/IDS/Ass4/lastfm-dataset-1K/" \
              "userid-timestamp-artid-artname-traid-traname.tsv"


# Count the lines
num_lines = sum(1 for l in open(source_path))

# Sample size - in this case ~1%
size = int(num_lines / 100)

# The row indices to skip. This will take a while. If you use the whole dataset, you can skip this part.
# skip_idx = random.sample(range(0, num_lines), num_lines - size)


# Dataframe containing the entire dataset. Skip some lines with error in them
df_raw = pd.read_csv(source_path, sep='\t', error_bad_lines=False)

# Dataframe containing a sample of the dataset
# df_raw = pd.read_csv(source_path, sep='\t', skiprows=skip_idx)

df_raw.columns = ['user_id', 'timestamp', 'artist_id', 'artist_name', 'track_id', 'track_name']

# Generate new artificial ID for each song
df_raw['artist_track'] = df_raw['artist_name'] + '|||' + df_raw['track_name']

# Drop redundant info
df = df_raw.drop(['timestamp', 'artist_id', 'track_id','artist_name', 'track_name'], axis=1)
# df['play_count'] = np.nan
print(df)
print('--------')
grouped = df.groupby(['user_id', 'artist_track'])

series = grouped.size()
df_final = pd.DataFrame({
                         'user':series.index.get_level_values('user_id'),
                         'song':series.index.get_level_values('artist_track'),
                         'count':series.values})
df_final = df_final[['user', 'song', 'count']]
print (df_final)

df_final.to_csv('./lastfm.csv', index=False)

