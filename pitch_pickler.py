import pandas as pd
from pybaseball import statcast


seasons = {'2010':{'start_date': '2010-04-04', 'end_date': '2010-11-01'},
           '2011':{'start_date': '2011-03-31', 'end_date': '2011-10-28'},
           '2012':{'start_date': '2012-03-28', 'end_date': '2012-10-28'},
           '2013':{'start_date': '2013-03-31', 'end_date': '2013-10-30'},
           '2014':{'start_date': '2014-03-22', 'end_date': '2014-10-29'},
           '2015':{'start_date': '2015-04-05', 'end_date': '2015-11-01'},
           '2016':{'start_date': '2016-04-03', 'end_date': '2016-11-02'},
           '2017':{'start_date': '2017-04-02', 'end_date': '2017-11-01'},
           '2018':{'start_date': '2018-03-29', 'end_date': '2018-10-28'},
           '2019':{'start_date': '2019-3-20', 'end_date': '2019-10-30'}
           }


def pull_clean_and_pickle(start_date, end_date, year):

    # Pull in DataFrame from statcast
    df = statcast(start_dt = start_date, end_dt = end_date)

    # Drop depriciated and extraneous features
    df = df.drop(columns = ['spin_dir','spin_rate_deprecated',
         'break_angle_deprecated','break_length_deprecated', 'game_type',
         'tfs_deprecated', 'tfs_zulu_deprecated', 'umpire'])

    # Sort pitches chronologically
    df = df.sort_values(by = ['game_date', 'at_bat_number', 'pitch_number'])

    # Pickle DataFrame
    df.to_pickle(path=(
    "/Users/mattkirby/repos/RC-v1.0--Data-Gathering/pitches_" + year + ".pkl"
    ),compression='zip')


for year in seasons.keys():
  start_date = seasons[year]['start_date']
  end_date = seasons[year]['end_date']
  pull_clean_and_pickle(start_date, end_date, year)
