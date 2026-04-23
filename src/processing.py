#Compute and clean missing values

import pandas as pd

def compute_team_totals(df,outputPath=None):
    df['total_available_games'] = df.groupby(['season', 'team_display_name'])['game_id'].transform('nunique')
    games = df.groupby(['game_id','team_display_name'])[['minutes','field_goals_made','field_goals_attempted','three_point_field_goals_made','three_point_field_goals_attempted','free_throws_made','free_throws_attempted','offensive_rebounds','defensive_rebounds','rebounds','assists','steals','blocks','turnovers','fouls','points']].sum()
    games['team_possessions'] = round(games['field_goals_attempted'] + .44 * games['free_throws_attempted'] - games['offensive_rebounds'] + games['turnovers'],2)
    games['team_metric'] = round((games['field_goals_attempted'] + .44 * games['free_throws_attempted'] + games['turnovers']),3)
    games['total_available_games'] = df.groupby(['game_id', 'team_display_name'])['total_available_games'].first()
    games = games.rename(columns={'rebounds':'team_rebounds','minutes':'team_minutes'}).reset_index()
    # games.to_parquet(f'{outputPath}/home_totals.parquet') # store for later
    return games #return the df to store as a variable for further processing

#per game stats
def compute_shot_pct(df,madeShotCol,attemptShotCol,decimals=3):
    return (df[madeShotCol]/df[attemptShotCol].replace(0,pd.NA)).round(decimals)

def compute_true_shooting_pct(df,pointsCol,fgAttemptCol,ftAttemptCol,decimals=3):
    return (df[pointsCol]/(2*(df[fgAttemptCol]+.44*df[ftAttemptCol]))).round(decimals)

def compute_attempt_rate(df,shotAttemptedCol,fieldGoalAttemptCol,decimals=3):
    return (df[shotAttemptedCol]/df[fieldGoalAttemptCol].replace(0,pd.NA)).round(decimals)

def compute_rating_efficiency(df,pointsCol,teamPossessionsCol,decimals=3):
    return ((df[pointsCol]/df[teamPossessionsCol].replace(0,pd.NA))*100).round(decimals)

def compute_ratios(df,numeratorCol,denominatorCol,decimals=3):
    return (df[numeratorCol]/df[denominatorCol].replace(0,pd.NA)).round(decimals)

def compute_turnover_pct(df,decimals=3):
    return ((df['turnovers']/df['metric'])*100).round(decimals)

#def compute_usage_rate(df,decimals=3):
    # return((df['metric']*(df['team_minutes']/5))/(df['minutes']*df['team_metric'])*100).round(decimals)

#per season/career stats (Averages) - save THIS as a new df
def career_stats(df,outputPath):
    #first get per season averages and totals. from there get the career averages and totals. save this to a df. totals can be saved to existing columns. need to bring in the averages col map. (MAKE THIS A GLOBAL VARIABLE??)
    pass

#find players with missing positions and correct multiple position entries
def get_missing_positions(df1,df2,df3=None):
    df1['athlete_position_abbreviation'] = df1['athlete_position_abbreviation'].replace('NA', pd.NA)
    positions1 = (df2[['athlete_display_name', 'athlete_position_abbreviation']].dropna().drop_duplicates(subset='athlete_display_name').set_index('athlete_display_name')['athlete_position_abbreviation'])
    df1['athlete_position_abbreviation'] = df1['athlete_position_abbreviation'].fillna(df1['athlete_display_name'].map(positions1))
    if df3 is not None:
        positions2 = (df3[['athlete_display_name', 'athlete_position_abbreviation']].dropna().drop_duplicates(subset='athlete_display_name').set_index('athlete_display_name')['athlete_position_abbreviation'])
        df1['athlete_position_abbreviation'] = df1['athlete_position_abbreviation'].fillna(df1['athlete_display_name'].map(positions2))
        positionRank = {'G': 1, 'C': 2, 'F': 3}
        positionList = df1[['athlete_display_name', 'athlete_position_abbreviation']].dropna(subset=['athlete_position_abbreviation']).drop_duplicates().assign(priority=df1['athlete_position_abbreviation'].map(positionRank)).sort_values('priority').drop_duplicates(subset='athlete_display_name').set_index('athlete_display_name')['athlete_position_abbreviation']
        df1['athlete_position_abbreviation'] = df1['athlete_display_name'].map(positionList)
    df1['athlete_position_abbreviation'] = df1['athlete_position_abbreviation'].replace('G-F', 'G')
    df1['athlete_position_abbreviation'] = df1['athlete_position_abbreviation'].replace('F-C', 'C')
    return df1

