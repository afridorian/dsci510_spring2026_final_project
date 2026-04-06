# <u> WNBA Predictive Player Viability </u>
## Project Overview
The goal of this project is to build a predictive model that determines professional WNBA viability of current NCAA players 
using logistic regression and randon forest modeling. The professional and collegiate game stats of WNBA players, both 
current and historic, were compiled and used to train the model. The model was localized by player position, with viability 
categorized into four levels: not viable, bench player, starter, and all star. The mean score for each category
was determined by a composite performance score derived from professional stats, collegiate stats, awards, years in the league, 
and All-Star selection.
## Data Sources
1. WNBA player box statistics 
   1. sportsdataverse wehoop-wnba-stats-data repo aggregation of espn.com box score game data - 116,701 records (2002-2025)
      i. 2004 is the first year with complete datasets
   2. basketball-reference.com box score game data - 29,983 records (1997-2003)
2. NCAA player box statistics
   1. espn.com season averages and totals - 1,500 records (2009-Present)
      i. 2009/10 season is the first year with complete datasets
   2. sports-reference.com season averages and totals - 2,037 records (1981-Present)
      i. 1981/82 season is the first year with complete datasets
      ii. was used to supplement missing statistics from espn.com
3. Draft Order - wnba.com - (1997-2024)
4. All-Star Roster - acrossthetimeline.com - 498 records (1999-2025)
5. WNBA player awards - acrossthetimeline.com - 2141 records (1997-2025)
6. NCAA player awards
## Results
## Installation
## Running Analysis


From `src/` directory run:

`python main.py `

Results will appear in `results/` folder. All obtained will be stored in `data/`