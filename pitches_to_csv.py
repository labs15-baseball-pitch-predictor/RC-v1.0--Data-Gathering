import pandas as pd

filenames = ['pitches_' + str(i) + '.pkl' for i in range(2010,2019)]

seasons = ['df_' + str(i) for i in range(10,19)]

season_dataframes = {}

for i in list(zip(filenames, seasons)):
        path = "/Users/mattkirby/repos/RC-v1.0--Data-Gathering/season_pickles/" + i[0]
        df = pd.read_pickle(path, compression='zip')
        df = df.drop(columns = ['des'])
        season_dataframes[i[1]] = df


i = 2010
for df in season_dataframes.values():
    path = "/Users/mattkirby/repos/RC-v1.0--Data-Gathering/season_csvs/pitches_" + str(i) + ".csv"
    df.to_csv(path)
    print(path[-16:], '...Done')
    i += 1
