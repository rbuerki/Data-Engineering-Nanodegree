# url = "https://data.stadt-zuerich.ch/api/3/action/datastore_search?resource_id=b9308f85-9066-4f5b-8eab-344c790a6982&limit=200"

# url_csv = "https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/b9308f85-9066-4f5b-8eab-344c790a6982/download/2020_verkehrszaehlungen_werte_fussgaenger_velo.csv"

# import pandas as pd
# import requests
# from pprint import pprint

# r = requests.get(url)  # Returns a 'response' object

# # Check main elements of returned JSON file
# print(r.json().keys())
# pprint(r.json()["result"])

# pprint(pd.DataFrame(r.json()["result"]["records"]))

import glob
import os
import pandas as pd

path = r"data/raw/verkehrszaehlungen/"         
all_files = glob.glob(os.path.join(path, "*.csv"))

df_from_each_file = (pd.read_csv(f) for f in all_files)
concatenated_df = pd.concat(df_from_each_file, ignore_index=True)
print(concatenated_df.shape)
