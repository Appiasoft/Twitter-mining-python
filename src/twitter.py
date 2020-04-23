# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 15:47:42 2020
@author: gabrielj, evertm
"""

import os, json
import objectpath
import firebase_admin

from bs4 import BeautifulSoup
from firebase_admin import credentials
from firebase_admin import firestore


#crear cuenta de servicio
#-->https://console.cloud.google.com/iam-admin/serviceaccounts?hl=es-419&_ga=2.131111180.2037458349.1587329480-153142272.1587329480
if not firebase_admin._apps:
    cred = credentials.Certificate('twitter-etl-b9cde-7f75ffe13222.json') 
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

#------------CIUDADES-------------
#KP1
location = []
#KP2
location_followers = {}
#KP3
listprHashtags = {}

#------------PERSONAS------------
#KP1
device = []
#KP2
name_followers = {}
#KP3
user_tw_dict = {}
#KP4
user_friend_dict = {}
#KP5
user_old_dict = {}
#------------TWITTS------------
tweets = []

#------------COLLECTION------------
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


            """
            -----------------------------DB CIUDADES---------------------------------------
            """
            """
            #KPI 1 : Top 5 de ciudades en donde se Twittea con mayor frecuencia
            locationtw = tuple(jsonnn_tree.execute('$..location'))

            for i in range(1, len(locationtw)):
                if(locationtw[i] != ''):
                    location.append(locationtw[i])
            #------------------------------------------------------------------------
            # KPI 2 : Top 5 de ciudades cuyos usuarios tienen mayor # de Seguidores
            followers_count = tuple(jsonnn_tree.execute('$..followers_count'))
            for i in range(1, len(locationtw)):
                if(locationtw[i] != ''):
                    for j in range(1, len(followers_count)):
                        location_followers[locationtw[i]] = followers_count[j]
            # ------------------------------------------------------------------------
            # KPI 3 : Top 5 de ciudades que en el Hashtach incluyen la plabra "Farc"
            hashtags = tuple(jsonnn_tree.execute('$..hashtags'))
            listprHashtags = list(hashtags)
            """
            # ------------------------------------------------------------------------

            """
            -------------------------------DB PERSONAS-------------------------------------
            """
            """
            # KPI 1 : Por tipo de dispositivo
            source = tuple(jsonnn_tree.execute('$..source'))

            for i in range(1, len(source)):
                if (source[i] != ''):
                    string = source[i]
                    soup = BeautifulSoup(string)
                    device.append(soup.a.string)
            # ------------------------------------------------------------------------
            # KPI 2 : Top 10 de usuarios con mas Seguidores
            name = tuple(jsonnn_tree.execute('$..screen_name'))

            for i in range(1, len(name)):
                if (name[i] != ''):
                    for j in range(1, len(followers_count)):
                        name_followers[name[i]] = followers_count[j]
            # ------------------------------------------------------------------------
            # KPI 3 : Top 10 de usuarios mayor  numero de Twitts
            id_user = tuple(jsonnn_tree.execute('$..id'))
            user_tw_dict[name] = id_user.__len__()
            # ------------------------------------------------------------------------
            # KPI 4 : Top 10 de usuarios con mas amigos
            friend_Count = tuple(jsonnn_tree.execute('$..friends_count'))

            for i in range(1,len(name)):
                if(name[i] != ''):
                    for j in range(1,len(friend_Count)):
                        user_friend_dict[name[i]] = friend_Count[j]
            # ------------------------------------------------------------------------
            # KPI 5 : Top 10 usuarios mas antiguos
            created_ad = tuple(jsonnn_tree.execute('$..created_at'))

            for i in range(1,len(name)):
                if(name[i] != ''):
                    for j in range(1,len(created_ad)):
                        user_old_dict[name[i]] = created_ad[j]
            """
            # ------------------------------------------------------------------------          
            text_tweet = tuple(jsonnn_tree.execute('$.text'))
            created_at_tweet = tuple(jsonnn_tree.execute('$.created_at'))
            for i, tweet in enumerate(text_tweet):
                tweets.append({"text":tweet,"created_at":created_at_tweet[i]})
            # ------------------------------------------------------------------------ 
        print(len(tweets))
        #insert tweets in batch
        batched_set_firestore(collection, tweets)
        
        print(collections_name)
    #insert persons names into documents
    add_firebase("persons", collections_name)

if __name__ == '__main__':
    KPIs()
