# ========================================
# File Name: processISDLite.py
# Author: Toby Peele and Rachel Kennedy
# Date: 05/19/2022
# ========================================

import os
import gzip 
import shutil
import urllib.request
import pandas as pd

from .parseISD import parseISD
from .ISDfilename import ISDfilename
from .organizePrecip import organizePrecip
from .ISDvariablesort import ISDvariablesort
from glob import glob
from datetime import datetime

DATE_FMT = '%Y%m%d_%H'

def processISDLite(savedir, stations, variables, starttime, endtime):
    """
    Processes information contained in ISD Lite weather observations and outputs them into a 
    dataframe. The function takes a save directory, a list of stations, a list of variables, 
    a start time, and an end time. 

    Parameters:
        savedir (String) - the directory to save the downloaded ISD files. 
        stations (list) - a list of stations in ICAO format that you are interested in.
        variables (list) - the variables you are interested in.
        starttime (String) - a start time in format yyyymmdd_HH.
        endtime (String) - an end time in format yyyymmdd_HH.
    Returns:
        finaldf (Dataframe) - the output dataframe
    """

    start = datetime.strptime(starttime, DATE_FMT)
    end = datetime.strptime(endtime, DATE_FMT)
    
    isdListFile = './isd-history.txt'
    headings = ['USAF', 'WBAN', 'STATION NAME', 'CTRY', 'ST', 'CALL', 'LAT', 'LON', 'ELEV', 'BEGIN', 'END']
    widths = [7, 6, 30, 5, 3, 5, 8, 9, 8, 9, 9]

    df = pd.read_fwf(isdListFile, names=headings, header=None, widths=widths,
                     converters={headings[0]: str, headings[1]: str})

    # Get a list of filenames from stations, start, and end dates. Download and
    # unpack these files into a passed save directory (savedir). 

    print('Downloading necessary data files.')

    filenames = ISDfilename(df, stations, start, end)

    if not os.path.exists(savedir):
        os.makedirs(savedir)

    for file in filenames:
        tmp = file.replace('.gz', '.isd')
        if not os.path.exists(savedir + tmp):
            download(file, savedir)
            unpack(file, savedir)
    
    print('Complete!')

    print('Processing each station. Please wait.')

    dflist = list()

    for station in stations:
        dataseg = df.loc[df['CALL'] == station].USAF
        dataseg2 = df.loc[df['CALL'] == station].WBAN
        if dataseg.values.any():
            searchStr = dataseg.values[0] + '-' + dataseg2.values[0]
            filelist = glob(savedir + searchStr + '*.isd')
            filelist.sort()
            if len(filelist) > 0:
                stationdf = parseISD(filelist[0])
            else:
                continue
            cnt = 1
            while cnt < len(filelist):
                newdf = parseISD(filelist[cnt])
                stationdf = pd.concat([stationdf, newdf], axis=0)
                cnt += 1

            stationdf = stationdf.loc[(stationdf['Datetime'] >= start) & (stationdf['Datetime'] <= end)]
            stationdf.reset_index(inplace=True, drop=True)
            stationdf = organizePrecip(stationdf)
            stationdf.insert(1, 'ICAO', station)
            dflist.append(stationdf)
    
    print('Generating final dataframe...')

    finaldf = ISDvariablesort(dflist[0], variables)
    cnt = 1
    while cnt < len(dflist):
        temp = ISDvariablesort(dflist[cnt], variables)
        finaldf = pd.concat([finaldf, temp], axis=0)
        cnt += 1
    
    return finaldf

def download(filename, savedir):
    baseurl = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/'
    idx = filename.rfind('-') + 1
    year = filename[idx:(idx+4)]
    url = baseurl + year + '/'
    print('Downloading %s' %filename)
    try:
        urllib.request.urlretrieve(url + filename, savedir + filename)
    except Exception as e:
        print(e)

def extractGzip(inputf, outputf):
    try:
        with gzip.open(inputf, 'rb') as f_in, open(outputf, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            if not f_out._checkClosed():
                f_out.close()
    except Exception as e:
        print(e)

def unpack(filename, savedir):
    if os.path.exists(savedir + filename):
        newfilename = filename.replace('.gz', '.isd')
        extractGzip(savedir + filename, savedir + newfilename)
        os.remove(savedir + filename)
        