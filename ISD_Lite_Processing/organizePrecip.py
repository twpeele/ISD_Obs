# ========================================
# File Name: organizePrecip.py
# Author: Toby Peele
# Date: 05/19/2022
# ========================================

import math
from numpy import NaN


def organizePrecip(df):

    """
    Organize precip according to 3hr, 6hr, and 12hr values. Attach these 
    to the end of the passed dataframe. 

    Parameters:
        df (dataframe) - the dataframe to be organized and modified.
    Returns:
        df (dataframe) - the modified dataframe. 
    """

    precip3hr = list()
    precip12hr = list()
    precipOther = list()

    cnt = 0
    for i in range(len(df)):

        # Determine what kind of varible is the first value in dataframe. 

        if i == 0 and not math.isnan(df.loc[i, 'Six Hour Precip Depth']):
            j = 1
            while math.isnan(df.loc[j, 'Six Hour Precip Depth']) and j < len(df):
                j += 1
                if j == len(df):
                    break
            if j == 3:
                precip3hr.append(df.loc[i, 'Six Hour Precip Depth'])
                precip12hr.append(NaN)
                precipOther.append(NaN)
                df.loc[i, 'Six Hour Precip Depth'] = NaN
            elif j == 6:
                precip3hr.append(NaN)
                precip12hr.append(NaN)
                precipOther.append(NaN)
            elif j == 12:
                precip12hr.append(df.loc[i, 'Six Hour Precip Depth'])
                precip3hr.append(NaN)
                precipOther.append(NaN)
                df.loc[i, 'Six Hour Precip Depth'] = NaN
            else:
                precip3hr.append(NaN)
                precip12hr.append(NaN)
                precipOther.append(df.loc[i, 'Six Hour Precip Depth'])
                df.loc[i, 'Six Hour Precip Depth'] = NaN
        
        # Determine what kind of variable the value is based on the value of the counter. 

        elif not math.isnan(df.loc[i, 'Six Hour Precip Depth']) and (cnt <=11) and i > 0:
            if cnt == 2:
                precip3hr.append(df.loc[i, 'Six Hour Precip Depth'])
                precip12hr.append(NaN)
                precipOther.append(NaN)
                df.loc[i, 'Six Hour Precip Depth'] = NaN
            elif cnt == 5:
                precip3hr.append(NaN)
                precip12hr.append(NaN)
                precipOther.append(NaN)
            elif cnt == 11:
                precip12hr.append(df.loc[i, 'Six Hour Precip Depth'])
                precip3hr.append(NaN)
                precipOther.append(NaN)
                df.loc[i, 'Six Hour Precip Depth'] = NaN
            else:
                precip3hr.append(NaN)
                precip12hr.append(NaN)
                precipOther.append(df.loc[i, 'Six Hour Precip Depth'])
                df.loc[i, 'Six Hour Precip Depth'] = NaN
            cnt = 0

        # Determine what kind of variable something is if the counter is higher than 11.
        # In this case we use the count forward method to make the determination.

        elif not math.isnan(df.loc[i, 'Six Hour Precip Depth']) and (cnt > 11) and i > 0:
            j = i + 1
            while math.isnan(df.loc[j, 'Six Hour Precip Depth']) and j < len(df):
                j += 1
                if j == len(df):
                    break
            if j == i + 3:
                precip3hr.append(df.loc[i, 'Six Hour Precip Depth'])
                precip12hr.append(NaN)
                precipOther.append(NaN)
                df.loc[i, 'Six Hour Precip Depth'] = NaN
            elif j == i + 6:
                precip3hr.append(NaN)
                precip12hr.append(NaN)
                precipOther.append(NaN)
            elif j == i + 12:
                precip3hr.append(NaN)
                precip12hr.append(df.loc[i, 'Six Hour Precip Depth'])
                precipOther.append(NaN)
                df.loc[i, 'Six Hour Precip Depth'] = NaN
            else:
                precip3hr.append(NaN)
                precip12hr.append(NaN)
                precipOther.append(df.loc[i, 'Six Hour Precip Depth'])
                df.loc[i, 'Six Hour Precip Depth'] = NaN
            cnt = 0
        
        # We didn't find a value so append nans to our lists and increment the counter. 

        else:
            precip3hr.append(NaN)
            precip12hr.append(NaN)
            precipOther.append(NaN)
            cnt += 1
    
    df.insert(8, 'Three Hour Precip Depth', precip3hr, True)
    df['Twelve Hour Precip Depth'] = precip12hr
    df['Other Precip Depth'] = precipOther
    return df

        