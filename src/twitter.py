# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 15:47:42 2020
@author: gabrielj, evertm
"""

import os
import json
import objectpath
import firebase_admin

from bs4 import BeautifulSoup
from collections import Counter
from firebase_admin import credentials
from firebase_admin import firestore

config = {
    "type": "service_account",
    "project_id": "twitteranalytics-f965d",
    "private_key_id": "88076ee66e17c16a31f3e8e54a01babdd1e6a115",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC59+qRJys9/gbr\n2xqvisZXy7cOf2BvhS8SEc86lwOsWb6Uxd3d1p/hxUAL38v0L0gKT6gTQWWFwSRC\ntSsd2D9LWvz+6nsdN++N4CwmZb9NRdYVOVWtvEmOqejVE1o+TmH9bU7sSFBQ8h3K\nZUBzh6aVBkcbg1OQhNCuUmSsh6O0/cz7YIeiZSfVfgRFCd76XKyFDCIByMKNe7JE\n5BOjFj4tvEkpL9E9q0PIL+59XOUhh4Eg+kG/ITHccoxqSQj8ryUCdRSwihdd4bL8\nT8jnQUGuunE4qDHOOvoHWpHM0EXoK0SkDEWtaWXEVHa1FoWA2/NcKCd+2934LJ2K\nlxUIrPo5AgMBAAECggEAEkgiRyK9SDAw01Q2y2BZH0hni9qkdad3eSScZVDIi6jx\nk1k1oRlNnmRFvOUYp73+LFqoVebtPrjorPt3waIAlufa9zRrO0lvGK4bqQACTmu5\nyrGtj+bH/xO0+pLBzF49mSxQaz9bUJnR/pOgQmXoz72jlTt+e4voTTrMVX8AGPVa\nDGMtKki5MzrCW5U1WrQFagpQXGwQdQqdY1YpLaqNgXZiTcVLZsX+xX1kjGPh9ynU\nDDpzh0SK5yELj4VDQJKsKV9Vw1KYdPTfYjz2zjFoOh2CuhDeDfqyo008rf9yt3wL\navbdEd6CEGbPV85X1FGx6u+0OgmIJb38QtNrIaYSAQKBgQDnv6CgI4SOE4izfa/G\nMnkf5mdeYUqoS3gnmPgX1cpjVfUDScbQUEKQAC9Vyr5Uwj80a1oKoxrmVS5SYqvH\nWqpyeTsX2wG6+h17SjyMtuh+oOF2HGWMDCYBuOGcCGBh7eSN2IPt9Bw1npIsNBWU\n4mWPxMdizWTd7N6YDEe6RdOxPwKBgQDNbd+GUzMcdiUzEdv3g0cltA7yZ9cFchqL\nY4QRsIntcDTl62ZO28Oblv07EGCJXR5RV83/8Ruc3a3ePuSQPEXjAtX+2QPdyPVs\nLmfbHBmRAcCGCocy8WqPsoEDhQPIXUrWTfwn6BmI/Eszq+dP4KCGeuqMyHD6xmYZ\nzjd9GlL+hwKBgQDTQa5NDNOjKE9vP93iENS1rbUBJjOGYvWvMxTFtY//KbZmPvTL\npL5owqJj1KPYZBJ12H6GP9UfrvrBA02QNyg1nimuP2i+Z45Ee5HivEIIXOPqZUx0\ndAaZf026jTA/VTsJyxvI6MRmZJzfSsN5qz/l5P3VJWMTov/vHxKrzUeCxQKBgQCX\ngJmiCggezG8g2+H7B6FYCBQh6D0m8gDrncgP97+xEdihXFtGe0Mmo1M81RYtGDS8\nzuHJCTtRFBgfWQjd6uWrxzKlUc1NKuD9GLhVWsLsM3uWH3uYMX1iXsZRHy8r/Mmt\nhTQbBkyWv2KCyBu+yC37H/mNPGI+QEWacIBxzhE5TwKBgCMQ2ddQDLFTVgxzjxem\nD1pKyW3Kw85wujv2nu8HWY6qWVevwtiEEOLVoFhHwOXa6F6H6sWRCl6csRIJMvDX\nUTzPtwZjkwxAo25OkiLtrkUllswH8l+lM0aLOsdRgJuKVVKob1/rDjVriNxzq178\nTDu1r5CQwE9DQu4/1YAlwSRX\n-----END PRIVATE KEY-----\n",
    "client_email": "firebase-adminsdk-ci4gs@twitteranalytics-f965d.iam.gserviceaccount.com",
    "client_id": "100807203722813626573",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-ci4gs%40twitteranalytics-f965d.iam.gserviceaccount.com"
}

# crear cuenta de servicio
# -->https://console.cloud.google.com/iam-admin/serviceaccounts?hl=es-419&_ga=2.131111180.2037458349.1587329480-153142272.1587329480
if not firebase_admin._apps:
    cred = credentials.Certificate(config)
    default_app = firebase_admin.initialize_app(cred)
db = firestore.client()


# documentacion transacciones y escrituras en lotes
# -->https://firebase.google.com/docs/firestore/manage-data/transactions?hl=es_419
def batched_set_firestore(collection, data):
    slicing = 0
    increment = 400
    while slicing < len(data):
        batch = db.batch()
        for i in data[slicing:slicing+increment]:
            batch_ref = db.collection(collection).document()
            batch.set(batch_ref, i)
        slicing += increment
        batch.commit()


def add_firebase(collection, data):
    # Add a new doc in collection
    for d in data:
        db.collection(collection).document().set(d)


# Definimos la ruta de los archivos JSON
path_to_json = 'dataset2'
json_files = [pos_json for pos_json in os.listdir(
    path_to_json) if pos_json.endswith('.json')]

# ------------VARIABLES-------------
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

            # Convertimos cada uno de los JSON en String
            json_text = json.load(json_file)
            # Leemos nuestros objetos del Arreglo de datos
            jsonnn_tree = objectpath.Tree(json_text)

            # -----------------------------DB CIUDADES---------------------------------------

            # KPI 1 : Top de ubicaciones en donde se Twittea con mayor frecuencia
            locationtw = tuple(jsonnn_tree.execute('$..location'))

            for i in range(1, len(locationtw)):
                if(locationtw[i] != ''):
                    location.append(locationtw[i])
            # ------------------------------------------------------------------------

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

            # -------------------------------DB PERSONAS-------------------------------------

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
                tweets.append(
                    {"text": tweet, "created_at": created_at_tweet[i]})
            # ------------------------------------------------------------------------

        # insert tweets in batch
        #batched_set_firestore(collection, tweets)

    # Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_location = (word for word in location if word[:1].isupper())
    c = Counter(words_to_count_location)
    # Guardamos nuestro HashMap en un arreglo de objetos
    for i in c.most_common(10):
        list_location.append({"name": i[0], "total": i[1]})
    print("list_location: "+str(list_location)+"\n")

    # Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_all_hashtags = (
        word for word in allHashtags if word[:1].isupper())
    words_to_count_farc_hashtags = (
        word for word in farcHashtags if word[:1].isupper())
    c1 = Counter(words_to_count_all_hashtags)
    c2 = Counter(words_to_count_farc_hashtags)
    # Guardamos nuestro HashMap en un arreglo de objetos
    for i in c1.most_common(10):
        list_all_hashtags.append({"name": i[0], "total": i[1]})
    list_farc_hashtag = [{"name": c2[0], "total":c2[1]}]
    print("list_all_hashtags: "+str(list_all_hashtags)+"\n")
    print("list_farc_hashtag: "+str(list_farc_hashtag)+"\n")

    # Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_Device = (word for word in device if word[:1].isupper())
    c = Counter(words_to_count_Device)
    # Guardamos nuestro HashMap en un arreglo de objetos
    for i in c.most_common(10):
        list_device.append({"name": i[0], "total": i[1]})
    print("list_device: "+str(list_device)+"\n")

    # insert persons names into documents in firebase
    add_firebase("persons", collections_name)

    # insert locations into documents in firebase
    add_firebase("location", list_location)

    # insert hashtags into documents in firebase
    add_firebase("hashtags", list_all_hashtags)

    # insert farc_hashtag into documents in firebase
    add_firebase("farc_hashtag", list_farc_hashtag)

    # insert device into documents in firebase
    add_firebase("device", list_device)


if __name__ == '__main__':
    KPIs()
