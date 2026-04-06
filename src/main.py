import config
import load_parquet as lp
import load_BBReference as bbr
import load_ESPN as es
import load_allstar_awards as award

# STEP 1. DOWNLOAD AND CACHE WNBA DATA
# lp.download_git(config.gitHubLink,config.fileLocationWNBA)
# bbr.br_wnbaschedule_scrape(config.brScheduleURL, config.yearsWNBAScheduleScrape,config.fileLocationSchedule)
# bbr.br_wnbabox_scrape(config.brGameURL, config.fileLocationSchedule, config.fileLocationWNBA) #process will take 2hours 10mins for full dataset due to rate limiting
# wehoopConcat = lp.concat_files(config.wehoopConcatFiles, config.fileLocationWNBA, f'{config.fileLocationWNBA}/wehoop.parquet') #concat wehoop data
# brConcat = lp.concat_files(config.brConcatFiles, config.fileLocationWNBA,  f'{config.fileLocationWNBA}/bbr.parquet') #concat basketball reference data
# lp.normalize(wehoopConcat) #normalize wehoop data
# lp.normalize(brConcat) #normalize bbr data
# WNBAData = lp.concat_files(config.concatWNBAFiles, config.fileLocationWNBA,f'{config.fileLocationCleaned}/combined_WNBA_player_box.parquet') #combine both wnba data sources, store as variable (filepath) to work with or access again via data cache

# STEP 2. DOWNLOAD AND CACHE NCAA DATA
#es.get_ESPN_athlete_id(f'{config.fileLocationCleaned}/combined_WNBA_player_box.parquet',config.fileLocationAthleteIDs) #generate list of player names and espn player IDs from WNBA data
# es.ESPN_scrape(config.espnAPIURL,config.fileLocationAthleteIDs,config.fileLocationPlayers,config.fileLocationNCAA) #process will take 8 minutes for full dataset
# lp.normalize(config.fileLocationPlayers) #normalize player bios
# missingAthletes = bbr.get_SR_athlete_names(config.fileLocationPlayers, f'{config.fileLocationCleaned}/combined_WNBA_player_box.parquet') #get list of players with no NCAA stats
# bbr.srcbb_ncaaplayer_scrape(config.srcbbURL, missingAthletes, config.fileLocationNCAA) #process will take 1 hour 10 mins for full dataset



#if __name__ == "__main__":