# <u> WNBA Predictive Player Viability </u>
## Project Overview
The goal of this project is to build a predictive model that determines professional WNBA viability of current NCAA players 
using logistic regression and randon forest modeling. The professional and collegiate game stats of WNBA players, both 
current and historic, were compiled and used to train the model. The model was localized by player position, with viability 
categorized into four levels: not viable, bench player, starter, and all star. The mean score for each category
was determined by a composite performance score derived from professional stats, collegiate stats, awards, years in the league, 
and All-Star selection.
## Data Sources
1. WNBA player box statistics (2002-2022)
   1. 107000 records
   2. espn.com box score scrape from wehoop repo
2. NCAA player box statistics
   1. Current WNBA players NCAA performance
      1.
   2. Current NCAA players performance
      1.
3. Draft Order (1997-2024)
4. All-Star Roster (1999-2025)
5. WNBA player awards
6. NCAA player awards
### Constraints
The model is missing information from data not included in hard line stats: 
## Data Preprocessing
### Inclusions
### Exclusions 
1. Non-domestic professional players. Domestic is defined by having played a minimum of 3 years of NCAA basketball.
### Composite Variable Engineering
## Feature Engineering
Composite Score - the analysis used to determine the composite score <br>
How the composite differs for each position at each viability level
## Target Variable
## Modeling Approach 
### Logistic Regression
### Decision Tree
### Random Forest
## Model Evaluation
## Results Interpretation
## Conclusion
Keana Monroy-Gill <br>
DSCI 510 - Spring 2026
