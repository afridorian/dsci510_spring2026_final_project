import pandas as pd

def concat_files(directory): #create one CSV from multiple files in a directory
    pass
    path = directory


def read_csv(file)->dict: #read box score files

    data = pd.read_csv(file) #read CSV data

    #boxDict = {} #stat dictionary
    #return boxDict
    return data.head()