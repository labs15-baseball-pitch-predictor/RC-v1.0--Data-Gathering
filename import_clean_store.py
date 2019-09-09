import pandas as pd
from pybaseball import statcast
import random
import mr_clean


seasons = {#'2010':{'start_date': '2010-04-04', 'end_date': '2010-11-01'},
           #'2011':{'start_date': '2011-03-31', 'end_date': '2011-10-28'},
           #'2012':{'start_date': '2012-03-28', 'end_date': '2012-10-28'},
           #'2013':{'start_date': '2013-03-31', 'end_date': '2013-10-30'},
           #'2014':{'start_date': '2014-03-22', 'end_date': '2014-10-29'},
           #'2015':{'start_date': '2015-04-05', 'end_date': '2015-11-01'},
           '2016':{'start_date': '2016-04-03', 'end_date': '2016-11-02'},
           '2017':{'start_date': '2017-04-02', 'end_date': '2017-11-01'},
           '2018':{'start_date': '2018-03-29', 'end_date': '2018-10-28'},
           '2019':{'start_date': '2019-03-20', 'end_date': '2019-09-07'}
           }


def pull_statcast_data(start_date, end_date, year):
    """
    Date Format: YYYY-MM-DD
    """
    df = statcast(start_dt = start_date, end_dt = end_date)
    return df


def clean(df):
    """
    Drop features, sort pitches cronologically, fills pitch type unknown with
    random sample from pitchers season pitch probabilities,
    creates pitch_category feature from pitch type map
    """
    # Drop depriciated and extraneous features
    df = df.drop(columns = ['spin_dir','spin_rate_deprecated',
                            'break_angle_deprecated','break_length_deprecated',
                            'game_type','tfs_deprecated',
                            'tfs_zulu_deprecated', 'umpire'])

    # Sort pitches chronologically
    df = df.sort_values(by = ['game_date', 'game_pk',
                              'at_bat_number','pitch_number'])

    # Get all the unique pitcher names in the df
    pitcher_list = df.player_name.unique().tolist()
    # Initialize empty dictionary to store each pitcher and their pitches and
    # Percentages for each pitch
    pitcher_dict = {}
    # Iterate over each pitcher:
    for pitcher in pitcher_list:
        # Assign the normalized value_counts to a variable
        pitch_percentages = df[df.player_name == pitcher].pitch_type.value_counts(normalize=True)
        # Convert that Series object to a dict and assign it as the value to
        # The pitcher dictionary
        # (Pitcher name as key)
        pitcher_dict[pitcher] = pitch_percentages.to_dict()

    # Grab the rows where pitch_type is null:
    nulls = df[df.pitch_type.isna()]

    # Iterate over each null row
    for index, row in nulls.iterrows():
      population = list(pitcher_dict[row.player_name].keys())
      weights = list(pitcher_dict[row.player_name].values())
      try:
        pitch = random.choices(population, weights, k=1)[0]
      except IndexError:
        pitch = 'FF'
      df.at[index, 'pitch_type'] = pitch

    # Create map for pitch type into categories:
    pitch_type_map = {'FA':'fastball', 'FF':'fastball', 'FT':'fastball',
        'FC':'fastball','FS':'fastball', 'SI':'fastball', 'SF':'fastball',
        'SL':'breaking','CB':'breaking', 'CU':'breaking', 'SC':'breaking',
        'KC':'breaking','CH':'offspeed', 'KN':'offspeed', 'EP':'offspeed',
        'FO':'pitchout', 'PO':'pitchout'}

    # Create pitch cateogory feature
    df['pitch_cat'] = df['pitch_type']
    df['pitch_cat'] = df['pitch_cat'].replace(pitch_type_map)
    df['if_fielding_alignment'] = df['if_fielding_alignment'].astype(object)
    df['of_fielding_alignment'] = df['of_fielding_alignment'].astype(object)
    return df


def compress_and_export(df, year, f_path = "season_pickles/"):
    """
    Pickle DataFrame
    """
    df.to_pickle(path=(f_path + year + ".pkl"
    ),compression='zip')


def main():
    def pull_clean_and_pickle(start_date, end_date, year):
        """
        Queries statcast, calls cleaning function, pickles season dataframes,
        and writes to seasons directory
        """
        df = pull_statcast_data(start_date, end_date, year)
        df = mr_clean.clean(df)
        compress_and_export(df, year)
    for year in seasons.keys():
      start_date = seasons[year]['start_date']
      end_date = seasons[year]['end_date']
      pull_clean_and_pickle(start_date, end_date, year)


if __name__ == "__main__":
    main()
