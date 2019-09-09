import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import category_encoders
from category_encoders.one_hot import OneHotEncoder
from column_names import col_names
import psycopg2

class preprocess:


    def __init__(pitcher_id):
        self.pitcher_id = pitcher_id


    def import_pitcher(self, pitcher_id = self.pitcher_id):
        con = psycopg2.connect(
                dbname= 'dev',
                host='examplecluster.cdbpwaymevt5.us-east-2.redshift.\
                      amazonaws.com',
                port= '5439',
                user= 'awsuser',
                password= '#######')
        cur = con.cursor()
        query = "SELECT * FROM pitches WHERE pitcher = " + Str(pitcher_id)
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns = col_names)
        cur.close()
        con.close()
        self.df = df


    def initial_column_drop(self):
        drop_columns = [
            'Unnamed: 0', 'release_pos_x', 'release_pos_z', 'des',
            #might want to keep bb_type and filder_2 is the catcher
            'hit_location', 'bb_type', 'hc_x', 'hc_y', 'fielder_2',
            'hit_distance_sc', 'launch_speed', 'launch_angle',
            'effective_speed', 'release_spin_rate', 'release_extension',
            'release_pos_y', 'estimated_ba_using_speedangle',
            'estimated_woba_using_speedangle', 'woba_value','woba_denom',
            'babip_value', 'iso_value', 'launch_speed_angle',
            'if_fielding_alignment', 'of_fielding_alignment', 'sv_id', 'pfx_x',
            'pfx_z', 'plate_x', 'plate_z', 'vx0', 'vy0', 'vz0', 'ax', 'ay',
            'az', 'sz_top', 'sz_bot', 'post_away_score', 'post_home_score',
            'post_bat_score', 'post_fld_score', 'pitch_name'
            ]
        self.df = self.df.drop(columns = drop_columns)


    def data_wrangle(self):

        """
        ## Events

        **Feature Description**: Event of the resulting Plate Appearance.

        **Issue**: Feature is categorical, and polluted with null values

        **Solution**: Impute null values with placeholder, then one-hot encode
                      the feature while dropping the resulting placeholder
                      column
        """
        self.df['events'] = self.df['events'].fillna(value = 0)

        """
        ## Release Speed

        **Feature Description**: Pitch velocities from 2008-16 are via
                                 Pitch F/X, and adjusted to roughly out-of-hand
                                 release point. All velocities from 2017 and
                                 beyond are Statcast, which are reported
                                 out-of-hand.

        **Issue**: Continuous numerical feature with 109 null values

        **Solution**: Impute null values median release speed
        """
        speed_median = self.df['release_speed'].median()
        self.df['release_speed'] = self.df['release_speed'].fillna(value = speed_median)

        """
        ## Zone

        **Feature Description**: Zone location of the ball when it crosses the
                                 plate from the catcher's perspective.

        **Issue**: Discrete numerical feature with 112 null values

        **Solution**: TEMPORARY - fill with center zone, then one-hot encode
        """
        self.df['zone'] = self.df['zone'].fillna(value = 5)

        """
        ## On 1B ID

        **Feature Description**: ID number of batter on fist base

        **Issue**: Categorical feature, polluted with 4863 null values

        **Solution**: TEMPORARY - Drop feature
        """
        self.df = self.df.drop(columns = ['on_1b_id'])


    def create_target_feature(self):
        """
        ## Create Next Pitch Feature
        """
        self.df['next_pitch'] = self.df['pitch_type'].shift(-1)
        index_pos = self.df.shape[0] - 1
        self.df.at[index_pos, 'next_pitch'] = 'FF'


    def one_hot_encode(self):
        one_hot_cols = [
            'events', 'zone', 'pitch_type', 'type', 'home_team', 'away_team',
            'pitch_count', 'L1_pitch_type',	'L1_pitch_result', 'L1_pitch_zone',
            '_count', 'count_cat',	'pitch_cat', 'balls', 'strikes','inning',
            'outs_when_up', 'batting_order_slot'
            ]
        # Instantiate Encoder
        one_hot_encoder = OneHotEncoder(cols=one_hot_cols,
                                        return_df=True,
                                        use_cat_names=True)
        # Encode features
        encoded = one_hot_encoder.fit_transform(self.df[one_hot_cols],
                                                self.df['next_pitch'])

        # Join encoded features into df and drop old columns
        self.df = self.df.join(encoded).drop(columns = one_hot_cols + ['events_0'])


    def binary_encode(self):
        """
        ## Binary Encode
        """
        self.df['inning_topbot'] = self.df['inning_topbot'].replace({'Top':0,
                                                                     'Bot':1})
        self.df['stand'] = self.df['stand'].replace({'L':0, 'R':1})
        self.df['p_throws'] = self.df['p_throws'].replace({'L':0, 'R':1})


    def secondary_column_drop(self):
        """
        ## Drop Unneeded Columns
        """
        drop_cols = [
            'game_date', 'index', 'pitcher', 'batter', 'game_year', 'game_pk',
            'player_name', 'description'
            ]

        self.df = self.df.drop(columns = drop_cols)


    def run(self):
        import_pitcher()
        initial_column_drop()
        data_wrangle()
        create_target_feature()
        one_hot_encode()
        binary_encode()
        secondary_column_drop()
        return self.df
