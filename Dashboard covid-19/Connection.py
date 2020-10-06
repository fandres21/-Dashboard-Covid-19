import pandas as pd
import numpy as np
import pymongo as pm

class Connection:
    
    def __init__(self, client, port, db, collection):
        ''' Constructor, make the connection to the db's specified collection'''
        
        try:
            self.client = pm.MongoClient(client, port)
            new_collection = "self.client." + db + "." + collection
            self.collection = eval(new_collection)
        except:
            raise Exception("Can't connect to the database, wrong parameters?")

    def save_data(cls, url, date):
        ''' Read the github csv and saves it on our MongoDB db '''
        
        cols = ["Country_Region", "Province_State", "Lat", "Long_",
                "Confirmed", "Deaths", "Recovered"]
        df = pd.read_csv(url, usecols=cols)

        try:
            for index, row in df.iterrows():
                doc = {"Country": str(row['Country_Region']),
                       "Province": str(row['Province_State']),
                       "Lat": str(row['Lat']),
                       "Lon": str(row['Long_']),
                       "Confirmed": int(row['Confirmed']),
                       "Deaths": int(row['Deaths']),
                       "Recovered": int(row['Recovered']),
                       "Date": date
                    }
                cls.collection.insert_one(doc)
        except:
            raise Exception("Cannot load the data")
            return
        print("Load completed, date: " + str(date))

    def close(cls):
        ''' Close mongodb connection (client)  '''

        cls.client.close()
        
        
