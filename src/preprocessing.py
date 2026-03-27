import pandas as pd
import re
import requests
import bs4 as bs

#CSV DATA PRE-PROCESSING FUNCTIONS
#create one CSV from multiple files in a directory
def concat_files(filePath,fileRange): #take original path and range of dates
    #normalize column names
    columnMap = {"min": "minutes",
                 "fg": "field_goals_attempted",
                 "fg3": "three_point_field_goals_attempted",
                 "ft": "free_throws_attempted",
                 "oreb": "offensive_rebounds",
                 "dreb": "defensive_rebounds",
                 "reb": "rebounds",
                 "ast": "assists",
                 "stl": "steals",
                 "blk": "blocks",
                 "to": "turnovers",
                 "pf": "fouls",
                 "pts": "points"
                 }
    output = pd.read_csv(filePath).rename(columns=columnMap)
    #iterate through the range of dates and concat the resulting files to the output dataSet
    for i in fileRange:
        new = re.sub(r'\_\d{4}',f'_{i}',filePath)
        new = pd.read_csv(new).rename(columns=columnMap)
        output = pd.concat([output, new], join='outer', ignore_index=True) #ignore index so the duplicates don't overwrite
    return output

#ESPN API 'SDK' FUNCTIONS
def ESPN_scrape(API,playerID):
    #needs to return a dictionary or dataSet that can then be referenced later by other files
    playerDict = {}
    for i in playerID:
        player = str(i)

        #NEED A WAY TO CHECK FOR MISSING KEYS SO THERE'S NO ERROR, PUT THE KEY ANYWAY THEN ADD NAN OR NONE
        attributes = ["athlete_id", "weight", "height", "DOB", "position", "positionAbbr", "status", "collegeAthlete",
                      "statisticslog"]

        profAthPg = requests.get(API+player).json()  # professional athlete page
        name = profAthPg['fullName']
        collegeAthPg = requests.get(profAthPg['collegeAthlete']['$ref']).json()
        seasonsPg = requests.get(profAthPg['seasons']['$ref']).json()

        playerDict.update({name: {"athlete_id": int(player), "weight": profAthPg['weight'],
                                                   "height": profAthPg['height'],
                                                   "DOB": profAthPg['dateOfBirth'],
                                                   "position": profAthPg['position']['name'],
                                                   "positionAbbr": profAthPg['position']['abbreviation'],
                                                   "status": profAthPg['status']['type'],
                                                   "totalWNBASeasons": seasonsPg['count'],
                                                   "ncaa_athlete_id": collegeAthPg['id'],
                                                   "totalNCAASeasons": collegeAthPg['experience']['years'],
                                                   "NCAAStats":{}}})

        collegeStatsPg = requests.get(collegeAthPg['statisticslog']['$ref']).json()

        scrapKeys = {"displayName","shortDisplayName","description","abbreviation","displayValue","summary"}
        for entry in collegeStatsPg['entries']: #iterates through entries dict (amount should == totalNCAASeasons)
            for data in entry['statistics']: #iterates through list of entry objects
                if data['type'] == 'team': #check if data is team or total stats
                    stats = requests.get(data['statistics']['$ref']).json()
                    season = re.search(r'\d{4}',str(stats))
                    playerDict[name]["NCAAStats"].update({season.group():stats['splits']['categories']}) #THIS OUTPUT NEEDS TO BE NORMALIZED

    return playerDict

#BASKETBALL/SPORTS REFERENCE WEB SCRAPE DATA PRE-PROCESSING FUNCTIONS
#wnba box scores from 1997-2002
def br_wnbabox_scrape(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    webPage = requests.get(url, headers=headers)

    soup = bs(webPage.content, 'html.parser')  # parse the webpage using beautiful soup
    table = soup.find('table', {'class': 'suppress_glossary sortable stats_table'})  # find the table element using html object class
    if not table:
        raise Exception("Could not find table.")

    rows = table.find_all('tr')
    tableGrid = {}

    headerRowsCount = 0
    foundDataRow = False

    for iR, row in enumerate(rows):
        if row.find('td'):  # if we find a td tag
            foundDataRow = True
        else:
            headerRowsCount += 1

        cells = row.find_all(['th', 'td'])
        iC = 0

        for data in cells:  # look at the data in the cells
            while (iR, iC) in tableGrid:  # skip already logged data
                iC += 1

            try:
                rowSpan = int(data.get('rowspan', 1))
            except ValueError:
                rowSpan = 1

            try:
                colSpan = int(data.get('colspan', 1))
            except ValueError:
                colSpan = 1

            cellText = data.text.strip()  # i need to not strip here and i need to pull out the link nested in box score
            cellText = re.sub(r'\[.*?\]', '', cellText)

            for r in range(rowSpan):  # iterate through rowspan (look at number of rows in table)
                for c in range(colSpan):
                    tableGrid[(iR + r, iC + c)] = cellText  # key: tuple coordinates of the cell, value: cell text

            iC += colSpan
    pass

#NCAA player annual stat averages from 1981-2008
def srcbb_ncaaplayer_scrape(url,player:list):
    #use regex to create url using the player name list
    pass

#SINGLE TABLE WEB SCRAPE FUNCTIONS
def draft_scrape(url):
    pass

def allstar_scrape(url):
    pass

def awards_scrape(url):
    pass

#DATA CLEANING FUNCTIONS
#clean up dataSet to account for nan, mixed types, missing values
def normalize(dataSet):
    #add the collegeAthPg headers here for each data set
    headers = {"int": ['game_id', 'season', 'season_type', 'athlete_id', 'team_id', 'field_goals_made',
                       'field_goals_attempted', 'three_point_field_goals_made', 'three_point_field_goals_attempted',
                       'free_throws_made', 'free_throws_attempted', 'offensive_rebounds', 'defensive_rebounds',
                       'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'fouls', 'plus_minus', 'points',
                       'athlete_jersey', 'team_score', 'opponent_team_id', 'opponent_team_score'],
               "float": ['minutes'],
               "dt": ['game_date_time'],
               "date": ['game_date'],
               "bool": ['starter','ejected','did_not_play','active','team_winner'],
               "str": ['athlete_display_name', 'team_name', 'team_location', 'team_short_display_name',
                       'athlete_short_name', 'athlete_position_name', 'athlete_position_abbreviation',
                       'team_display_name', 'team_uid', 'team_slug', 'team_logo', 'team_abbreviation', 'team_color',
                       'team_alternate_color', 'home_away', 'opponent_team_name', 'opponent_team_location',
                       'opponent_team_display_name', 'opponent_team_abbreviation', 'opponent_team_logo',
                       'opponent_team_color', 'opponent_team_alternate_color', 'athlete_headshot_href', 'reason']}

    #get a list of values, apply conversion to all values of each corresponding column in the list
    for i in headers:
        inc = [val for val in headers[i] if val in dataSet.columns] #list of headers to include, avoid keyerror
        if i == "int":
            dataSet[inc] = dataSet[inc].apply(pd.to_numeric, errors='coerce').astype('Int64')  # int normalization
        if i == "float":
            dataSet[inc] = dataSet[inc].apply(pd.to_numeric, errors='coerce')  # float normalization
        if i == "dt":
            dataSet[inc] = dataSet[inc].apply(pd.to_datetime, errors='coerce')  # datetime normalization
        if i == "date":
            dataSet[inc] = dataSet[inc].apply(pd.to_datetime, format='%Y%m%d', errors='coerce') # date normalization
        if i == "str":
            dataSet[inc] = dataSet[inc].astype('string').replace(r'\s+', " ", regex=True) # string normalization (do i need to account for trailing and leading spaces?)
        if i == "bool":
            dataSet[inc] = dataSet[inc].astype('boolean') # bool normalization to accept no value
    return dataSet

#remove certain key, value pairs from json inputs
def removeKV(data,keys): #dictionary of values to look at, list of keys to remove
    # for k in data:
    #     for
    pass
    #if isinstance(data:dict):


#DATABASE FUNCTIONS
