import pandas as pd


# Set these values first!!
path_to_pickles = "/Users/mattkirby/repos/RC-v1.0--Data-Gathering/season_pickles/"
pitcher_id = 425844.0
export_path = "pitcher_" + str(pitcher_id) + ".csv"


# Create DataFrames from pickle files for each season
filenames = ['pitches_' + str(i) + '.pkl' for i in range(2010,2019)]
seasons = ['df_' + str(i) for i in range(10,19)]
season_dataframes = {}

for i in list(zip(filenames, seasons)):
    path = path_to_pickles + i[0]
    season_dataframes[i[1]] = pd.read_pickle(path, compression='zip')


# Concatenate all seasons into a single DataFrame
all_pitches = pd.concat(season_dataframes.values())


# Create DataFrame from just a single pitchers pitches
sample_pitcher = all_pitches[all_pitches['pitcher'] == pitcher_id]


# Create a column representing the next pitch that will be thrown
sample_pitcher['next_pitch'] = sample_pitcher['pitch_name'].shift(1)


# Compute the pitchers next pitch probabilities for prior batter matchups
next_pitch_probs = sample_pitcher.groupby('batter').size().div(len(sample_pitcher))
batter_priors = pd.DataFrame(sample_pitcher.groupby(['batter', 'next_pitch']).size().div(len(sample_pitcher)).div(next_pitch_probs, axis=0, level='batter'))


# create a DataFrame from these prior probabilities
pitch_batter_matchup = {}
for batter_id in set(sample_pitcher['batter'].tolist()):
    pitch_batter_matchup[int(batter_id)] = dict(zip(batter_priors.T[batter_id].columns.tolist(),
                                                    batter_priors.T[batter_id].values[0]))
df = pd.DataFrame(pitch_batter_matchup).T.fillna(0)


# Merge prior probabilities into pitcher DataFrame
sample_pitcher['batter'] = sample_pitcher['batter'].astype(int)
df['batter'] = df.index

sample_pitcher = sample_pitcher.merge(df,
                                      how = 'inner',
                                      on = 'batter',
                                     )


# Export the pitcher DataFrame
sample_pitcher.to_csv(export_path)
