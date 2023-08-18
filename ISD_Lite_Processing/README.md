# ISD Lite Processing
This tool takes user defined stations, dates, and variables and fetches the corresponding ISD Lite data. After some processing the tool outputs a dataframe with the desired data. 
## How To Use
The primary function in this library is `processISDLite()`. You should not call the other functions in this library directly. The following is the doc string for the function.

`processISDLite(savedir, stations, variables, starttime, endtime)`

    Processes information contained in ISD Lite weather observations and outputs them into a dataframe. The function takes a save directory, a list of stations, a list of variables, a start time, and an end time. 

    Parameters:
        savedir (String) - the directory to save the downloaded ISD files. 
        stations (list) - a list of stations in ICAO format that you are interested in.
        variables (list) - the variables you are interested in.
        starttime (String) - a start time in format yyyymmdd_HH.
        endtime (String) - an end time in format yyyymmdd_HH.
    Returns:
        finaldf (Dataframe) - the output dataframe

### Inputs

**savedir** - You must select a directory to put the downloaded ISD files into. This is a string that ends with a trailing backslash (Windows) or forward slash (Linux). An example: `/home/user/ISDdir/`

**stations** - A list of stations you are interested in. These stations must be in ICAO format. Example: `KABE` or `KRDU`. 

**variables** - A list of variables you are interested in. These must be from the following list: `Air Temperature, Dew Point Temperature, Sea Level Pressure, Wind Direction, Wind Speed Rate, Sky Condition Code, and Precip`. All other choices will cause a program failure. Your variables must come from the list above. 

**starttime** - The date and time in hours that you want to start with. Must be in format `yyyymmdd_HH`. Example: 20210124_12

**endtime** - The date and time in hours that you want to end with. Must be in format `yyyymmdd_HH`. Example: 20220314_00

### Returns

**finaldf** - A dataframe with all the stations and dates you requested. Note that the stations are all in the same dataframe together. You will need to separate them using a subset based on station to get an individual station. Also note that variables will be listed in the same order that you asked for. 

## Final Notes

This software is being presented as is with no warranty. The software is not particularly intellegent meaning that if you give it bad input data it will either crash or give unexpected results. 

## Resources

### Where to find data
ISD_Lite NOAA Database:
https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/

### Example Code

This code generates two plots. One for temperature at KRDU and the other for one hour precip at the same station. The date range is between 12/01/2009 and 01/03/2022. To utilize this example be sure to change the save directory to something on your machine. 


    # processISDLite_test.py

    import matplotlib.pyplot as plt
    from processISDLite import processISDLite

    stations = ['KABE', 'KRDU', 'KJFK']
    variables = ['Air Temperature', 'Precip']
    starttime = '20091201_00'
    endtime = '20220103_00'
    savedir = 'C:\\Users\\toby2\\Documents\\ISDData\\'

    df = processISDLite(savedir, stations, variables, starttime, endtime)

    df_RDU = df[df['ICAO'] == 'KRDU']

    df_RDU.plot(kind='scatter', x='Datetime', y='Air Temperature')
    plt.title('KRDU Temperature')
    plt.show()

    df_RDU.plot(kind='scatter', x='Datetime', y='One Hour Precip Depth')
    plt.title('KRDU Precip')
    plt.show()

The plots should look something like this:

![Temperature Example](example_plots/rdu_temp.png)

![Precip Example](example_plots/rdu_precip.png)
