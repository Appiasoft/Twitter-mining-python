# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 15:47:42 2020
@author: gabrielj, evertm
"""

import os, json
import objectpath
import firebase_admin

from bs4 import BeautifulSoup
from collections import Counter
from firebase_admin import credentials
from firebase_admin import firestore

config = {
  "type": "service_account",
  "project_id": "twitter-d4727",
  "private_key_id": "ed095bde2024462964f24eeb663e81e27a31184f",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1Ct6QfQSWvZ09\nJxSm086CDxfQQLS3WP8+arvYGzBPA3gjN4sF0po/AtuSMsWhNDs11XWhaORZxDt6\nA77619R8pFGb7cD3zIbswiK+UOLUCVXIliJscLv8zac1t3eYXKup299mmNpzGxDm\nLXip1iE4SR39DhzVXiEAw/ak+mOPP44ryCfKbedErQEbl7GqI+ImC6CbbjqMWXmB\nlRynRGljuBLOdl2MESdtI1R6fS8pHcLk1gc/YDuI4Y1Au/qXTQ1skXBfNm7E5c0v\neAz10xx/ej8PVwaaCuao9b64OIhYnDamQDiBmYtkvs44aF+iiK/QwGGUCLFwDLiW\n9cdp9CfLAgMBAAECggEAJJjN5OdgxhgOcQ41XoPyetgSOEbcQTP8ZOGul0DnwxEw\nd+4CbGrmQ1aQjD03Nh8Y2QkNflN11a+VVlTrhkma6pdM5/hnya/Olo2n+FDhiXtZ\nm/K9gq5HUhNAedtckbAqA2wu/M5pWlteTylh+vMORBKDvWPc8ETW5xC2CyOSfKJE\nwROYN34kLB4w80eHd4EzsEKOkLrbOkBlw1ourvBaLj7kZAySQ9UnZAbzn9EPM6Da\nyhIhkCSOEz4xkgGMgI9J71ZYj6ijJu3ja4bKJzYk0kc0Z44Hmiz1Lv8V515/8Q0R\nrRcEHrpMQ/fgCp0yRme+3SWnY8yTZYghedPqly16AQKBgQDxNSnmYIuZ3YPyq86v\n5APrGdkIBXkoOeCBSjtAYphhgvSBGwFfnA+2NA4wjPnS30HFBWIlvrPBDWHBhKii\n4B834iSvlUWgjxxvgBW1VpabJPsHRslnYul8BUTo5tF4BgcJaheUWqI/AmYE2v1U\noqm1RbGI55ryNijqlM7DdF6M/wKBgQDAJSSRlflD7eqosD1cEqI4391JEDXEnp1z\n0LRpE9cjuy2RBpuEbCbx+R1XHuQnBOlRI9c2LHV6pBLfNYXEgXaY995g08+rSAIO\nC1J24z/7oCOSZZGoVQhirc+ojZt7ncVbGzfEgUbao9ANVg0c3L18TcDYfaqEIdw/\nv5MkhJQJNQKBgFOEePGP1EZ+cMWBv5sfdEvfM0qXuo+3GokpzsJIYULCOS7Kzvrv\n6nDILXGaHpZpPzic2JMBKPRbUdHTwSBEQgrzfohdQzKI2uuwidO0G4m0kEbAt9PY\nZ8fNe0K5SSYp0B3uYiO4Dh2LEw/zU2wOtnxRmVsx9nCPWDOBOuZqUKK5AoGBAIcJ\n06CJ4WxvSgsSCZ2q7t2/33pY7SDpRMk/dXuf2QSgqQ6SsTuo8tn+dTNkX35Ywk0n\nPjoxlsMBis3ahM+tGRe6pEwysHG1ItSR+nvZjH8gdb+OqIbAdCs98oKIOyzuneRA\nxbnSDWTd2ft1bbPKI1W4I39DGlLQpUUcM6Jnls69AoGAL1ft5z/ukrkbz/A5sG96\nFCYVHvxUXE9PVM7Hv8Ub92Gxp97WWZTccNIt3GyGD+kUaq+8RtN7xN6tODH59qSk\nC7kan7laq55b8rjUm7w0HU8CR9McvPD1VKRp8/UeD+N6j/6zl4p+OmNMV2kzmFX9\nmpx+HT+6HDNwxeMsRlyHsyA=\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-narmq@twitter-d4727.iam.gserviceaccount.com",
  "client_id": "101706656862194422717",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-narmq%40twitter-d4727.iam.gserviceaccount.com"
}



#crear cuenta de servicio
#-->https://console.cloud.google.com/iam-admin/serviceaccounts?hl=es-419&_ga=2.131111180.2037458349.1587329480-153142272.1587329480
#if not firebase_admin._apps:
cred = credentials.Certificate(config) 
default_app = firebase_admin.initialize_app(cred)
db = firestore.client()


#documentacion transacciones y escrituras en lotes
#-->https://firebase.google.com/docs/firestore/manage-data/transactions?hl=es_419
def batched_set_firestore(collection, data):
    slicing=0
    increment = 400
    while slicing< len(data):
        batch = db.batch()
        for i in data[slicing:slicing+increment]:
            batch_ref = db.collection(collection).document()
            batch.set(batch_ref, i)
        print(slicing,slicing+increment)
        slicing+=increment
        batch.commit()

def add_firebase(collection, data):
    # Add a new doc in collection
    for d in data:
       db.collection(collection).document().set(d)


# Definimos la ruta de los archivos JSON
path_to_json = '../dataset2'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

#------------VARIABLES-------------
location = []
location_followers = {}
allHashtags = []
farcHashtags = []
list_location = []
list_all_hashtags = []
list_device = []
device = []
name_followers = {}
user_tw_dict = {}
user_friend_dict = {}
user_old_dict = {}
tweets = []
collections_name = []

def KPIs():
    # Recorremos cada uno de los JSON leidos para realizar el procesamiento de la informacion
    for index, js in enumerate(json_files):
        with open(os.path.join(path_to_json, js), encoding="utf8") as json_file:
            print(json_file.name)
            collection = (os.path.basename(json_file.name)).split(".")[0]
            collections_name.append({"person": collection})

            #Convertimos cada uno de los JSON en String
            json_text = json.load(json_file)
            #Leemos nuestros objetos del Arreglo de datos
            jsonnn_tree = objectpath.Tree(json_text)

            #-----------------------------DB CIUDADES---------------------------------------

            #KPI 1 : Top de ubicaciones en donde se Twittea con mayor frecuencia
            locationtw = tuple(jsonnn_tree.execute('$..location'))

            for i in range(1, len(locationtw)):
                if(locationtw[i] != ''):
                    location.append(locationtw[i])
            #------------------------------------------------------------------------
    
            # KPI 2 : Top de ciudades cuyos usuarios tienen mayor # de Seguidores
            followers_count = tuple(jsonnn_tree.execute('$..followers_count'))
            for i in range(1, len(locationtw)):
                if(locationtw[i] != ''):
                    for j in range(1, len(followers_count)):
                        location_followers[locationtw[i]] = followers_count[j]

            # ------------------------------------------------------------------------
    
            # KPI 3 : Dos listas, una con todos los Hashtags utilizados y otra con los Hashtags que hacen referencia a FARC
            hashtags = tuple(jsonnn_tree.execute('$..hashtags[text]'))
            for i in range(1, len(hashtags)):
                if(hashtags[i] != ''):
                    if('farc' in hashtags[i] or 'Farc' in hashtags[i] or 'FARC' in hashtags[i]):
                        farcHashtags.append(hashtags[i])
                    else:
                        allHashtags.append(hashtags[i])
                        
            #-------------------------------DB PERSONAS-------------------------------------
            
            # KPI 1 : Por tipo de dispositivo
            source = tuple(jsonnn_tree.execute('$..source'))

            for i in range(1, len(source)):
                if (source[i] != ''):
                    string = source[i]
                    soup = BeautifulSoup(string)
                    device.append(soup.a.string)
            # ------------------------------------------------------------------------
    
            # KPI 2 : Top de usuarios con mas Seguidores
            name = tuple(jsonnn_tree.execute('$..screen_name'))

            for i in range(1, len(name)):
                if (name[i] != ''):
                    for j in range(1, len(followers_count)):
                        name_followers[name[i]] = followers_count[j]
            # ------------------------------------------------------------------------      
            text_tweet = tuple(jsonnn_tree.execute('$.text'))
            created_at_tweet = tuple(jsonnn_tree.execute('$.created_at'))
            for i, tweet in enumerate(text_tweet):
                tweets.append({"text":tweet,"created_at":created_at_tweet[i]})
            # ------------------------------------------------------------------------ 

        #print(len(tweets))
        #insert tweets in batch
        batched_set_firestore(collection, tweets)
        
        print(collections_name)
    print("\n")
    #insert persons names into documents in firebase
    add_firebase("persons", collections_name)

    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_location = (word for word in location if word[:1].isupper())
    c = Counter(words_to_count_location)
    # Guardamos nuestro HashMap en un arreglo de objetos
    for i in c.most_common(10):
        list_location.append({"location":i[0],"total":i[1]})
    print("list_location: "+str(list_location)+"\n")
    #insert locations into documents in firebase
    add_firebase("location", list_location)
    
    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_all_hashtags = (word for word in allHashtags if word[:1].isupper())
    words_to_count_farc_hashtags = (word for word in farcHashtags if word[:1].isupper())
    c1 = Counter(words_to_count_all_hashtags)
    c2 = Counter(words_to_count_farc_hashtags)
    # Guardamos nuestro HashMap en un arreglo de objetos
    list_all_hashtags = c1.most_common(10)
    for i in c1.most_common(10):
        list_all_hashtags.append({"hasgtags":i[0],"total":i[1]})
    list_farc_hashtag = [{"hashtag":c2[0],"total":c2[1]}]
    #insert hashtags into documents in firebase
    add_firebase("hashtags", list_all_hashtags)
    #insert farc_hashtag into documents in firebase
    add_firebase("farc_hashtag", list_farc_hashtag)
    print("list_all_hashtags: "+str(list_all_hashtags)+"\n")
    print("list_farc_hashtag: "+str(list_farc_hashtag)+"\n")
    
    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_Device = (word for word in device if word[:1].isupper())
    c = Counter(words_to_count_Device)
    # Guardamos nuestro HashMap en un arreglo de objetos
    for i in c.most_common(10):
        list_device.append({"device":i[0],"total":i[1]})
    #insert device into documents in firebase
    add_firebase("device", list_device)
    print("list_device: "+str(list_device)+"\n")
            

if __name__ == '__main__':
    KPIs()
