# ========================================
# File Name: ISDvariablesort.py
# Author: Toby Peele and Rachel Kennedy
# Date: 05/19/2022
# ========================================

import pandas as pd

def ISDvariablesort(df, variables):
    """
    Sort dataframe based off input start time and end time and input variables

    Parameters:
        df (dataframe): the dataframe to be organized and modified
        variables (list): user input variables
            Options: Air Temperature, Dew Point Temperature, Sea Level Pressure, Wind Direction, Wind Speed Rate,
            Sky Condition Code, Precip

    Returns:
        df (dataframe): the modified dataframe
    """

    # Try to find the variable 'Precip' in the list of variables. If it exists then the following 
    # code will run. Otherwise a value error is thrown and the code block is skipped. 
    
    try:
        idx = variables.index('Precip')
        variables.insert(idx, 'Other Precip Depth')
        variables.insert(idx, 'Twelve Hour Precip Depth')
        variables.insert(idx, 'Six Hour Precip Depth')
        variables.insert(idx, 'Three Hour Precip Depth')
        variables.insert(idx, 'One Hour Precip Depth')
        variables.remove('Precip')
    except:
        pass

    labels = df.columns.tolist() # convert column headers to list/index
    df2 = df[list(set(labels) & set(variables))] # sorts out variables that are not present in database
    df2 = df2[variables]
    df = pd.concat([df[['Datetime', 'ICAO']], df2], axis=1) # returns database with datetime and selected variables

    return df