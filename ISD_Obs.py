# ==========================
# File Name: ISD_Obs.py
# Author: Toby Peele
# Date: 07/05/2023
# ==========================

import os

from datetime import datetime, timedelta
from ISD_Lite_Processing.processISDLite import processISDLite
from glob import glob
from mysql import connector
from __config import Configuration

DATE_FMT = '%Y%m%d_%H'

def ISD_Obs(starttime, endtime, savedir, stationList, variableList, backfill=True, overwrite_table=False):
    """
    Fetches ISD Lite Data and uploads it to the Hindsight Database. 

    Parameters:
        starttime (String) - the start time, date and hour, as a string.
        endtime (String) - the end time, date and hour, as a string.
        savedir (String) - the directory you want to save the downloaded and processed data to. 
        stationList (String) - the name of the file containing the list of stations to work from. 
        variableList (String) - the name of the file containing the list of variables you're interested in. 
        backfill (Boolean) - Default: True, whether or not you want this function to operate in backfill mode.
        overwrite_table (Boolean) - Default: False, whether or not you want this function to overwrite data in existing table.
    """

    print('Starting process.')

    # Create station and variable list from external files

    stations = list()
    with open(stationList, 'r') as f:
        for line in f:
            stations.append(line.strip('\n'))
    
    variables = list()
    with open(variableList, 'r') as f:
        for line in f:
            variables.append(line.strip('\n'))

    # Call processISDLite to generate our dataframe

    df = processISDLite(savedir, stations, variables, starttime, endtime)

    # Convert celcius temperatures to kelvin and kts to m/s.

    if 'Air Temperature' in variables:
        df['Air Temperature'] = df['Air Temperature'] + 273.15
    
    if 'Dew Point Temperature' in variables:
        df['Dew Point Temperature'] = df['Dew Point Temperature'] + 273.15

    if 'Wind Speed Rate' in variables:
        df['Wind Speed Rate'] = df['Wind Speed Rate'] * 0.514444

    # Convert our user's input dates into datetime to use for comparison in dataframe

    start = datetime.strptime(starttime, DATE_FMT)
    end = datetime.strptime(endtime, DATE_FMT)

    print('Starting daily separation and .csv generation process.')

    current = datetime.strptime(start.strftime('%Y%m%d'), '%Y%m%d')

    while current < end:

        # create a stop point for each day

        stop = current + timedelta(days=1)

        # do dataframe search for right date 

        tempdf = df.loc[(df['Datetime'] >= current) & (df['Datetime'] < stop)]

        tempdf.reset_index(inplace=True, drop=True)

        # generate .csv for each day 

        filename = savedir + datetime.strftime(current, '%Y%m%d') + '.csv'
        yr = str(current.year)

        checklist = tempdf.columns.values.tolist()

        with open(filename, 'a') as f:
            for i in tempdf.index:
                tms = datetime.strftime(tempdf['Datetime'][i], '%Y-%m-%d %H:%M:%S')
                if 'Air Temperature' in checklist:
                    f.write(tempdf['ICAO'][i] + ',' + 'temperature,' + yr + ',' + tms + ',0,' + str(tempdf['Air Temperature'][i]) + ', ,1hr_isd,0' + '\n')
                if 'Dew Point Temperature' in checklist:
                    f.write(tempdf['ICAO'][i] + ',' + 'dewpoint,' + yr + ',' + tms + ',0,' + str(tempdf['Dew Point Temperature'][i]) + ', ,1hr_isd,0' + '\n')
                if 'Sea Level Pressure' in checklist:
                    f.write(tempdf['ICAO'][i] + ',' + 'seaLevelPress,' + yr + ',' + tms + ',0,' + str(tempdf['Sea Level Pressure'][i]) + ', ,1hr_isd,0' + '\n')
                if 'Wind Direction' in checklist:
                    f.write(tempdf['ICAO'][i] + ',' + 'windDir,' + yr + ',' + tms + ',0,' + str(tempdf['Wind Direction'][i]) + ', ,1hr_isd,0' + '\n')
                if 'Wind Speed Rate' in checklist:
                    f.write(tempdf['ICAO'][i] + ',' + 'windSpeed,' + yr + ',' + tms + ',0,' + str(tempdf['Wind Speed Rate'][i]) + ', ,1hr_isd,0' + '\n')
                if 'One Hour Precip Depth' in checklist:
                    f.write(tempdf['ICAO'][i] + ',' + 'precip1Hour,' + yr + ',' + tms + ',0,' + str(tempdf['One Hour Precip Depth'][i]) + ', ,1hr_isd,0' + '\n')
                if 'Three Hour Precip Depth' in checklist:
                    f.write(tempdf['ICAO'][i] + ',' + 'precip3Hour,' + yr + ',' + tms + ',0,' + str(tempdf['Three Hour Precip Depth'][i]) + ', ,1hr_isd,0' + '\n')
                if 'Six Hour Precip Depth' in checklist:
                    f.write(tempdf['ICAO'][i] + ',' + 'precip6Hour,' + yr + ',' + tms + ',0,' + str(tempdf['Six Hour Precip Depth'][i]) + ', ,1hr_isd,0' + '\n')

        print('Created ' + filename)
        
        # assign the next day to current. 

        current = stop
    
    print('Separation process complete!')

    uploadISD(savedir, backfill, overwrite_table)

def uploadISD(savedir, is_backfilling, overwrite_table):
    """
    Actually uploads processed ISD Lite Data into Hindsight Database. 

    Parameters:
        savedir (String) - the directory containing the data to be uploaded.
        is_backfilling (Boolean) - whether or not you want to use the backfill staging table in the database.
        overwrite_table (Boolean) - whether or not you want to overwrite or add data to an existing table. 
    """

    config = Configuration()
    RESET_STAGING_BACKFILL_ID = 'ResetAsosStagingBackfillId'
    RESET_STAGING_ID = 'ResetAsosStagingId'
    if is_backfilling:
        staging_table = 'asos_staging_backfill'
    else:
        staging_table = 'asos_staging'

    filelist = glob(savedir + '**/*.csv', recursive=True)
    filelist.sort()

    print('Starting Upload Process')
    print('Connecting to database')

    conn = connector.MySQLConnection(user=      config.db['user'],
                                     password=  config.db['password'],
                                     host=      config.db['host'],
                                     database=  'hindsight_asos',
                                     allow_local_infile = True)
    
    print('Connected!')

    for file in filelist:
        try:
            cursor = conn.cursor()

            # Check to see if table exists. 

            filedate = parseDateFromFilename(file)
            myTableName = 'asos_' + filedate
            tableExists = False

            fetch_asos_table_names = (
                "SHOW TABLES IN hindsight_asos;"
            )

            cursor.execute(fetch_asos_table_names)
            response = cursor.fetchall()
            for r in response:
                if r[0] == myTableName:
                    tableExists = True
                    break
            
            # if we're not overwriting existing table data and the table exists, move on to the next table. 

            if not overwrite_table and tableExists:
                cursor.close()
                continue
            
            conn.start_transaction()

            print('Clearing staging table.')
            clear_staging_table = (
                "DELETE FROM hindsight_asos." + staging_table + " WHERE `id` > 0;"
            )
            cursor.execute(clear_staging_table)
            cursor.callproc('hindsight_asos.%s' % RESET_STAGING_BACKFILL_ID if is_backfilling else RESET_STAGING_ID)
            print('Clear complete!')

            load_file_to_staging = (
                "LOAD DATA LOCAL INFILE %s "
                "REPLACE INTO TABLE hindsight_asos." + staging_table + " "
                "FIELDS TERMINATED BY ',' "
                "LINES TERMINATED BY '\n' "
                "(asos_id, var_name, year, time, altitude, numeric_val, qualitative_val, source, report_type)"
            )

            print('Loading %s into staging table...' % file)
            cursor.execute(load_file_to_staging, (file,))

            if not tableExists:
                print('Creating new table.')
                cursor.callproc('hindsight_asos.CreateAsosArchiveTable', (myTableName,))

            copy_file_to_daily = (
                "INSERT INTO hindsight_asos." + myTableName + " "
                "(asos_id, var_id, year, time, altitude, numeric_val, qualitative_val, source, report_type) "
                    "SELECT "
                    "staging.asos_id, "
                    "av.id, "
                    "staging.year, "
                    "staging.time, "
                    "staging.altitude, "
                    "staging.numeric_val, "
                    "staging.qualitative_val, "
                    "staging.source, " 
                    "staging.report_type "
                "FROM hindsight_asos." + staging_table + " staging "
                    "INNER JOIN hindsight_asos.asos_variables av ON staging.var_name = av.id "
                "ON DUPLICATE KEY UPDATE "
                    "numeric_val = VALUES(numeric_val), "
                    "qualitative_val = VALUES(qualitative_val), "
                    "source = VALUES(source), "
                    "report_type = VALUES(report_type)"
            )

            print('Copying data from staging to %s.' % myTableName)
            cursor.execute(copy_file_to_daily)

            # clean up
            os.remove(file)

            # Finish the transaction
            conn.commit()
            cursor.close()
            print('Complete!')

        except connector.Error as e:
            print('There was an ERROR during the feed, rolling back: {}'.format(e))
            conn.rollback()
            
    print('Data upload complete!')
    conn.close()

def parseDateFromFilename(filename):
    """
    Parses date from file name. 

    Parameters: 
        filename (String) - The file name to be parsed. 
    """

    idx = len(filename)-12
    myDate = filename[idx:(idx+8)]
    return myDate
