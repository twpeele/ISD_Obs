# ========================================
# File Name: Update_ISDtxt.py
# Author: Logan McLaurin
# Date: 08/04/2022
# ========================================

def Update_ISDtxt():
    
    """
    Updates the 'isd-history.txt' file to the current version as published to
    the NCEI. This file contains the station information necessary for
    downloading and publishing ISD-Lite data. This file will contain changes
    over time which can impact active stations and current data availability.
    It is recommended to run this function every couple of months especially
    upon the start of a new year.

    Parameters: None
        
    Returns:
        
        Updated 'isd-history.txt' file to the repository
        
    """
    from ftplib import FTP
    
    ## Hostname for the ftp connection. This does not require a username or
    #       password.
    HOSTNAME = 'ftp.ncdc.noaa.gov'
    
    ## Log on to the server
    ftp_server_noaa = FTP(HOSTNAME)
    ftp_server_noaa.login()
    
    ftp_server_noaa.cwd('pub/data/noaa/') # Directory of the 'isd-history.txt'
    
    filename = 'isd-history.txt'
    
    ## Write as a binary file to the local machine. This file will be downloaded
    #       to the current working directory which will replace the outdated
    #       'isd-history.txt'
    with open(filename, 'wb') as file:
        ftp_server_noaa.retrbinary(f'RETR {filename}', file.write)
    
    ## Logout
    ftp_server_noaa.quit()