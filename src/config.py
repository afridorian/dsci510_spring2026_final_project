from pathlib import Path
from dotenv import load_dotenv

# project configuration from .env (secret part)
envPath = Path('src/.env').resolve()
load_dotenv(dotenv_path=envPath)  # loads into os.environ

# project configuration
baseDIR = Path(__file__).resolve().parent.parent
dataDIR = baseDIR/'data'
resultsDIR = baseDIR/'results'

#raw output directories
fileLocationWNBA = dataDIR / 'WNBA_box_stats'
fileLocationNCAA = dataDIR / 'NCAA_box_stats'
fileLocationPlayers = dataDIR /'player_bios.parquet'
fileLocationSchedule = fileLocationWNBA / 'WNBA_schedules.json'
fileLocationAthleteIDs = fileLocationNCAA /'ESPN_athlete_ids.parquet'
#fileLocationAthleteNames = fileLocationNCAA / 'SR_athlete_names.parquet'

#cleaned output directory
fileLocationCleaned = dataDIR / 'cleaned'

#load data sources configuration
gitHubLink = 'https://api.github.com/repos/sportsdataverse/sportsdataverse-data/releases/tags/espn_wnba_player_boxscores'
brScheduleURL = 'https://www.basketball-reference.com/wnba/years/'
brGameURL = 'https://www.basketball-reference.com'
srcbbURL = 'https://www.sports-reference.com/cbb/players/'
espnAPIURL = 'http://sports.core.api.espn.com/v2/sports/basketball/leagues/wnba/athletes/'
draftURL = 'https://www.wnba.com/wnbadraft/all-time'
allStarsURL = 'https://www.acrossthetimeline.com/wnba/all-stars.html#start=1997-01-01'
awardsURL = 'https://www.acrossthetimeline.com/wnba/awards.html#awards=mvp%2Casmvp%2Cccmvp%2Cfmvp%2Cpotw%2Cpotm%2Crotm%2Ccommonth%2Callw1%2Callw2%2Calld1%2Calld2%2Callrook%2Cdpoty%2Croty%2Cnoty%2Cswoty%2Cmip%2Csport%2Cdawn%2Ccomseason%2Calldec%2Calldech%2Ctop15%2Ctop20%2Cw25%2Cappoty%2Capdoty%2Cap6poty%2Capcpoty%2Caproty%2Capallrook%2Capmip%2Capcoty%2Cap1st%2Cap2nd%2Cappotw&start=1997-01-01'

#scrape and data cleaning variables, change here to adjust scope of data retrieval
yearsWNBAScheduleScrape = range(1997, 2004)
wehoopConcatFiles = ['player_box_2004.parquet','player_box_2005.parquet','player_box_2006.parquet','player_box_2007.parquet','player_box_2008.parquet','player_box_2009.parquet','player_box_2010.parquet','player_box_2011.parquet','player_box_2012.parquet','player_box_2013.parquet','player_box_2014.parquet','player_box_2015.parquet','player_box_2016.parquet','player_box_2017.parquet','player_box_2018.parquet','player_box_2019.parquet','player_box_2020.parquet','player_box_2021.parquet','player_box_2022.parquet','player_box_2023.parquet','player_box_2024.parquet','player_box_2025.parquet']
brConcatFiles = ['player_box_1997.parquet','player_box_1998.parquet','player_box_1999.parquet','player_box_2000.parquet','player_box_2001.parquet','player_box_2002.parquet','player_box_2003.parquet']
concatWNBAFiles = ['wehoop.parquet','bbr.parquet']
concatNCAAFiles = ['combined_NCAA_player_box.parquet']

#parsing variables - outputs to be used as inputs in other functions
#playerNameIDUnique = '/Users/kmonroygill/Library/CloudStorage/GoogleDrive-monroygi@usc.edu/My Drive/Spring 2026/DSCI 510/dsci510_spring2026_final_project/data/cached/unique_player_id_name.parquet'