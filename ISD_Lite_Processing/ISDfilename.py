
def ISDfilename(df, station,starttime,endtime):

    """
    Author: Rachel Kennedy and Toby Peele - April 2022

    Create a file call name for ISD file based off of list of input stations and list of input years. ISDfilename will
    match input airport stations to ISDhistory text and find the USAF and WBAN numbers associated with input airport.
    Final filename is a combination of USAF ID-WBAN ID-year

    Inputs:
        df (Dataframe) - the lookup table used to look up stations
        station - input in airport ICAO format (ICAO format example: "KRDU")
            format for station input: station = ["KRDU", "KDCA"]
            datatype - list

        starttime/endtime - desired start/end of dataset
            datatype - datetime

    Output:
        Returns a list of filenames for each input airport station and year
            datatype - list
        e.g. inputting ISDfilename(df, ["KRDU"],"20200101_00","20211231_23")
        will return a list of two filenames for KRDU, one for 2020 and one for 2021
    """
    stations = []
    wban = []
    filename = []

    for callsign in station:
        dataseg = df.loc[df['CALL'] == callsign].USAF
        dataseg2 = df.loc[df['CALL'] == callsign].WBAN
        if dataseg.values.any():
            stations.append((dataseg.values[0]))
            wban.append((dataseg2.values[0]))

    start_year = starttime.year
    end_year = endtime.year

    years = list(range(start_year, end_year + 1, 1))

    a = len(stations)

    for i in range(0, a):
        for j in years:
            file = str(stations[i]) + '-' + str(wban[i]) + '-' + str(j) + '.gz'
            filename.append(file)

  #  print(filename)

    return filename