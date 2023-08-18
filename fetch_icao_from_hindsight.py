from mysql import connector
from __config import Configuration

def main():
    config = Configuration()
    conn = connector.connect(user=      config.db['user'],
                                     password=  config.db['password'],
                                     host=      config.db['host'],
                                     database=  'hindsight_asos',
                                     allow_local_infile = True)
    
    cursor = conn.cursor()

    select_query = "SELECT icao_id FROM hindsight_asos.asos;"

    cursor.execute(select_query)
    response = cursor.fetchall()

    cursor.close()
    conn.close()

    with open('stations.txt', 'w') as f:
        for item in response:
            f.write(item[0] + '\n')

if __name__ == "__main__":
    main()