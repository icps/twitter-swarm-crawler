import utils

import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def get_dataframe(date):

    filename = "tweet_data/tweets_{}.csv".format(date)
    data     = pd.read_csv(filename)    
    df       = data[data["Swarm"].str.contains('swarm')] ## Drop row that does not have swarmapp information
    df.reset_index(drop = True, inplace = True)

    n1 = len(data); n2 = len(df)
    print("Retrieved {} tweets, {} contain Foursquare info (dropped {})".format(n1, n2, n1 - n2))

    return df


def get_page(url):
    
    session = requests.Session()
    retry   = Retry(connect = 3, backoff_factor = 0.5)
    adapter = HTTPAdapter(max_retries = retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    page = session.get(url)
    
    return page

def get_swarm_link(series, index):
    #return df["Swarm"][index].split("'")[1]
    return [x for x in series[index].split("'") if "swarmapp.com" in x][0]


def get_categories(page):
    
    id_user   = [x for x in page if "id" in x][0].split(":")[-1].split('"')[1]
    longitude = [x for x in page if '"lng"' in x][0].split(":")[-1]
    latitude  = [x for x in page if '"lat"' in x][0].split(":")[-1].split("}")[0]
    country   = [x for x in page if '"cc"' in x][0].split(":")[-1].split('"')[1]
    gender    = [x for x in page if "gender" in x][0].split(":")[-1].split("}")[0].split('"')[1]
    loc_type  = [x for x in page if "name" in x][1].split(':"')[-1].split('"')[0]
    id_place  = [x for x in page if '"id"' in x][1].split(":")[-1].split('"')[1]

    check_ins = {"user": id_user, "gender": gender, "place": id_place, "latitude": latitude, 
                 "longitude": longitude, "category": loc_type, "country": country}
    
    return check_ins


def main():

    dates = utils.time_interval("2020-08-01", "2020-09-02")

    for date1, date2 in dates:

        df_checkin = pd.DataFrame(columns = ["date", "user", "gender", "place", "latitude", "longitude", 
                                             "category", "country"])

        print("Collecting from {} to {}".format(date1, date2))

        date = "{}_{}".format(date1, date2)
        df   = get_dataframe(date)

        for index in range(0, len(df)):

            try:
                swarm_link        = get_swarm_link(df["Swarm"], index)
                page              = get_page(swarm_link)
                soup              = BeautifulSoup(page.text, 'html.parser')

                script_page       = soup.find_all("script")[-1].prettify()

            
                list_info         = script_page.split('"user":{')[1].split(",")

                check_ins         = get_categories(list_info)
                check_ins["date"] = df["Date"][index]
                df_checkin        = df_checkin.append(check_ins, ignore_index = True)

            except:
                print("-----------------------")
                print(df["Tweet"][index])
                print(swarm_link)
                print("-----------------------")
                pass


        filename = "data/{}.csv".format(date)
        df_checkin.to_csv(filename, index = False)
        
        
if __name__ == "__main__":
    main()