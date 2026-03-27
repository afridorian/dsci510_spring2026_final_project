#load all the string constants paths data sources
from pathlib import Path
from dotenv import load_dotenv

# project configuration from .env (secret part)
envPath = '/Users/Afridorian/Library/CloudStorage/GoogleDrive-monroygi@usc.edu/My Drive/Spring 2026/DSCI 510/dsci510_spring2026_final_project/src/.env'
load_dotenv(dotenv_path=envPath)  # loads into os.environ

# project configuration
dataDIR = "'/Users/Afridorian/Library/CloudStorage/GoogleDrive-monroygi@usc.edu/My Drive/Spring 2026/DSCI 510/dsci510_spring2026_final_project/data'"
resultsDIR = "'/Users/Afridorian/Library/CloudStorage/GoogleDrive-monroygi@usc.edu/My Drive/Spring 2026/DSCI 510/dsci510_spring2026_final_project/results'"

# data sources configuration
