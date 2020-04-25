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

config = {}


#crear cuenta de servicio
#-->https://console.cloud.google.com/iam-admin/serviceaccounts?hl=es-419&_ga=2.131111180.2037458349.1587329480-153142272.1587329480
if not firebase_admin._apps:
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

        #insert tweets in batch
        #batched_set_firestore(collection, tweets)

    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_location = (word for word in location if word[:1].isupper())
    c = Counter(words_to_count_location)
    # Guardamos nuestro HashMap en un arreglo de objetos
    for i in c.most_common(10):
        list_location.append({"location":i[0],"total":i[1]})
    print("list_location: "+str(list_location)+"\n")
    
    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_all_hashtags = (word for word in allHashtags if word[:1].isupper())
    words_to_count_farc_hashtags = (word for word in farcHashtags if word[:1].isupper())
    c1 = Counter(words_to_count_all_hashtags)
    c2 = Counter(words_to_count_farc_hashtags)
    # Guardamos nuestro HashMap en un arreglo de objetos
    for i in c1.most_common(10):
        list_all_hashtags.append({"hashtags":i[0],"total":i[1]})
    list_farc_hashtag = [{"hashtag":c2[0],"total":c2[1]}]
    print("list_all_hashtags: "+str(list_all_hashtags)+"\n")
    print("list_farc_hashtag: "+str(list_farc_hashtag)+"\n")
    
    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_Device = (word for word in device if word[:1].isupper())
    c = Counter(words_to_count_Device)
    # Guardamos nuestro HashMap en un arreglo de objetos
    for i in c.most_common(10):
        list_device.append({"device":i[0],"total":i[1]})
    print("list_device: "+str(list_device)+"\n")
            
    #insert persons names into documents in firebase
    add_firebase("persons", collections_name)
    
    #insert locations into documents in firebase
    add_firebase("location", list_location)
    
    #insert hashtags into documents in firebase
    add_firebase("hashtags", list_all_hashtags)
    
    #insert farc_hashtag into documents in firebase
    add_firebase("farc_hashtag", list_farc_hashtag)
    
    #insert device into documents in firebase
    add_firebase("device", list_device)

if __name__ == '__main__':
    KPIs()
