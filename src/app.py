import os, json
from pprint import pprint
import objectpath
from collections import Counter
from bs4 import BeautifulSoup
import pymysql
import json
from collections import OrderedDict



# Definimos la ruta de los archivos JSON
path_to_json = '../dataset/'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]


#Declaramos nuestras variables principales de cada KPI



#------------DB CIUDADES-------------
#KP1
location = []
#KP2
location_followers = {}
#KP3
media_dict = {}

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


def hinted_tuple_hook(obj):
    if '__tuple__' in obj:
        return tuple(obj['text'])
    else:
        return obj




def replace_value_with_definition(key_to_find, definition, dictionary):
    for key in dictionary.keys():
        if key == key_to_find:
            dictionary[key] = definition

def KPIs():
    # Recorremos cada uno de los JSON leidos para realizar el procesamiento de la informacion
    for index, js in enumerate(json_files):
        with open(os.path.join(path_to_json, js), encoding="utf8") as json_file:
            print(json_file)

            #Convertimos cada uno de los JSON en String
            json_text = json.load(json_file)
            #Leemos nuestros objetos del Arreglo de datos
            jsonnn_tree = objectpath.Tree(json_text)


            """
            -----------------------------DB CIUDADES---------------------------------------
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

            # KPI 3 : Top 10 ciudades que mas contenido multimedia incluyen en los Twitss

            # ------------------------------------------------------------------------

            # KPI 4 : Top 5 de ciudades que en el Hashtach incluyen la plabra "Farc"
            hashtags = tuple(jsonnn_tree.execute('$..hashtags'))
            print(type(hashtags))
            list(hashtags)
            listpr = {}

            listpr = list(hashtags)


            print(listpr)


            for i in range(1,len(hashtags)):
                if(len(hashtags[i]) > 0):
                    listpr =



            # ------------------------------------------------------------------------

            """
            -------------------------------DB PERSONAS-------------------------------------
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
            id = tuple(jsonnn_tree.execute('$..id'))

            user_tw_dict[name] = id.__len__()

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



            # ------------------------------------------------------------------------
            """
            -------------------------------DB TIEMPO-------------------------------------
            """

            """
            -------------------------------DB PALABRAS-------------------------------------
            """
    print('-------------KPI4')
    print(json.dumps(user_friend_dict, indent=4, sort_keys=True))
    print('-------------KPI5')
    print(json.dumps(user_old_dict, indent=4, sort_keys=True))


    """
    
    d_sorted_by_value = OrderedDict(sorted(location_followers.items(), key=lambda x: x[1]))

    json2 = json.dumps(d_sorted_by_value)

    new_list = []
    new_list.append(list(d_sorted_by_value.keys())[-1]+","+list(d_sorted_by_value.values())[-1])
    new_list.append(list(d_sorted_by_value.keys())[-2]+","+list(d_sorted_by_value.values())[-2])
    new_list.append(list(d_sorted_by_value.keys())[-3]+","+list(d_sorted_by_value.values())[-3])
    new_list.append(list(d_sorted_by_value.keys())[-4]+","+list(d_sorted_by_value.values())[-4])
    new_list.append(list(d_sorted_by_value.keys())[-5]+","+list(d_sorted_by_value.values())[-5])

    print(new_list)
    f = open("dict.json", "w")
    f.write(json2)
    f.close()
    pprint('---------------------------------LOCATION (KPI_1)-----------------------------------------------------------')

    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Localizacion, numero de repeticion)
    words_to_count = (word for word in location if word[:1].isupper())
    c = Counter(words_to_count)
    print(c.most_common(20))
    #Guardamos nuestro HashMap en una lista iterable
    list_kpi1 = c.most_common(5)

    
    #Realiazmos apertura de conexion y envio de informacion a MySQL
    try:
        with connection.cursor() as cursor:
            for i in range(0,5):
                QUERY_SQL_LOCATION = "INSERT INTO kp1(`location`, `number`) VALUES (%s, %s)"
                cursor.execute(QUERY_SQL_LOCATION,(list_kpi1[i][0],list_kpi1[i][1]))
                connection.commit()
    finally:
        print('Subio todo');
    




    pprint('---------------------------------DEVICE (KPI_2)-----------------------------------------------------------')

    #Realizamos un contento de palabras y vamos guardando en un HashMap asi: (Dispositivo, numero de repeticion)
    words_to_count_Device = (word for word in device if word[:1].isupper())
    c = Counter(words_to_count_Device)
    print(c.most_common(10))
    # Guardamos nuestro HashMap en una lista iterable
    list_device = c.most_common(10)
    
    
    # Realiazmos apertura de conexion y envio de informacion a MySQL
    try:
        with connection.cursor() as cursor2:
            for j in range(0, 10):
                QUERY_SQL_DEVICE = "INSERT INTO kp2(`device`, `number`) VALUES (%s, %s)"
                cursor2.execute(QUERY_SQL_DEVICE, (list_device[j][0],list_device[j][1]))
                connection.commit()
    finally:
        # Finalmente cerramos la conexion
        connection.close()
        print('Subio todo')
    
    """




if __name__ == '__main__':
    KPIs()
