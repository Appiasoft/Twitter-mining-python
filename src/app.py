import os, json
from pprint import pprint
import objectpath
from collections import Counter
from bs4 import BeautifulSoup
import json
from collections import OrderedDict



# Definimos la ruta de los archivos JSON
path_to_json = 'dataset'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]


#Declaramos nuestras variables principales de cada KPI


#------------DB CIUDADES-------------
#KP1
location = []
#KP2
location_followers = {}
#KP3
allHashtags = []
farcHashtags = []

#------------DB PERSONAS------------
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


def KPIs():
    # Recorremos cada uno de los JSON leidos para realizar el procesamiento de la informacion
    for index, js in enumerate(json_files):
        with open(os.path.join(path_to_json, js), encoding="utf8") as json_file:
            print(json_file)

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
    
    pprint('--------------------------------- KPI 1 - RESULT-----------------------------------------------------------')

    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_location = (word for word in location if word[:1].isupper())
    c = Counter(words_to_count_location)
    # Guardamos nuestro HashMap en una lista iterable
    list_location= c.most_common(10)
    print(list_location)
    
    pprint('--------------------------------- KPI 2 - RESULT-----------------------------------------------------------')

    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_all_hashtags = (word for word in allHashtags if word[:1].isupper())
    words_to_count_farc_hashtags = (word for word in farcHashtags if word[:1].isupper())
    c1 = Counter(words_to_count_all_hashtags)
    c2 = Counter(words_to_count_farc_hashtags)
    # Guardamos nuestro HashMap en una lista iterable
    list_all_hasgtags = c1.most_common(10)
    list_farc_hasgtags = c2.most_common(10)
    print(list_all_hasgtags)
    print(list_farc_hasgtags)
    
    pprint('---------------------------------KPI 3 - RESULT -----------------------------------------------------------')

    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_Device = (word for word in device if word[:1].isupper())
    c = Counter(words_to_count_Device)
    # Guardamos nuestro HashMap en una lista iterable
    list_device = c.most_common(10)
    print(list_device)
    
  
    

    #DB - CIUDADDES
    print('-------------KPI1')
    with open('result/location.json', 'w') as json_file:
            json.dump(location, json_file)
    print('-------------KPI2')
    with open('result/location_followers.json', 'w') as json_file:
            json.dump(location_followers, json_file)
    print('-------------KPI3')
    with open('result/listprHashtags.json', 'w') as json_file:
            json.dump(listprHashtags, json_file)
            
    #DB - PERSONAS
    print('-------------KPI1')
    with open('result/device.json', 'w') as json_file:
            json.dump(device, json_file)
    print('-------------KPI2')
    with open('result/name_followers.json', 'w') as json_file:
            json.dump(name_followers, json_file)
    print('-------------KPI3')
    with open('result/user_friend_dict.json', 'w') as json_file:
            json.dump(user_friend_dict, json_file)
    print('-------------KPI4')
    with open('result/user_old_dict.json', 'w') as json_file:
            json.dump(user_old_dict, json_file)
    


if __name__ == '__main__':
    KPIs()
