import pandas as pd

def parseISD(filename):
    
    """
    Process a corresponding ISD data file and format into a Pandas dataframe.\
     Values of -9999 are interpreted as missing values.\
     Each datapoint is scaled accordingly.
     
    Parameters:
        filename (String) - The filename of the file to be parsed.
    Returns:
        (DataFrame) - A DataFrame containing all of the parsed and adjusted data.
    """
    
    headings = ['Year', 'Month', 'Day', 'Hour', 'Air Temperature', 'Dew Point Temperature',
                                 'Sea Level Pressure', 'Wind Direction', 'Wind Speed Rate', 'Sky Condition Code',
                                 'One Hour Precip Depth', 'Six Hour Precip Depth']
    
    # We need to specify the widths as the default colspec 'infer' does not correctly pick up rarely seen missing data values. 

    widths = [5, 3, 3, 3, 6, 6, 6, 6, 6, 6, 6, 6]

    df = pd.read_fwf(filename, names=headings, header=None, widths=widths, parse_dates = {'Datetime' : ['Year', 'Month', 'Day', 'Hour']}, na_values=[-9999])

    df['Air Temperature'] = df['Air Temperature']/10
    df['Dew Point Temperature'] = df['Dew Point Temperature']/10
    df['Sea Level Pressure'] = df['Sea Level Pressure']/10
    df['Wind Speed Rate'] = df['Wind Speed Rate']/10
    df['One Hour Precip Depth'] = df['One Hour Precip Depth']/10
    df['Six Hour Precip Depth'] = df['Six Hour Precip Depth']/10

    return df
