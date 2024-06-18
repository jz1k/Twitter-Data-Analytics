import pandas as pd
import twikit
from twikit import Client
from datetime import datetime, timedelta
import time
import re

# Initialize client
client = Client('en-US')

# Credenciales de usuario
credentials = [
    {'username': 'mail1', 'email': 'correo@gmail.com1', 'password': 'password1'},
    {'username': 'mail2', 'email': 'correo@gmail.com2', 'password': 'password2'},
    {'username': 'mail3', 'email': 'correo@gmail.com3', 'password': 'password3'},
    {'username': 'mail4', 'email': 'correo@gmail.com4', 'password': 'password4'},
    {'username': 'mail5', 'email': 'correo@gmail.com5', 'password': 'password5'},
    {'username': 'mail6', 'email': 'correo@gmail.com6', 'password': 'password6'},
]

# Leer el DataFrame desde un archivo CSV si existe
try:
    df = pd.read_csv('tweets-beastmode.csv', sep='*')
    if df.empty:
        raise FileNotFoundError
except FileNotFoundError:
    df = pd.DataFrame(columns=['user_id', 'username', 'id', 'created_at_datetime', 'text', 'lang', 'reply_count', 'retweet_count', 'favorite_count', 'view_count', 'location', 'hashtags', 'in_reply_to', 'quote', 'quote_count'])

counter = 0
minutes = 0
index = 0
last_tweet_id = None
repeat_count = 0

# Inicializar la fecha de inicio de la búsqueda
start_date = '2024-12-31'
while True:
    try:
        # Login to the service with provided user credentials
        client.login(
            auth_info_1=credentials[index]['username'],
            auth_info_2=credentials[index]['email'],
            password=credentials[index]['password']
        )

        # Verificar si hay fechas en el DataFrame
        if not df.empty:
            # Obtener la fecha máxima sin problemas
            # Asegurarse de que todos los datos en 'created_at_datetime' son de tipo datetime
            df['created_at_datetime'] = pd.to_datetime(df['created_at_datetime'])
            start_date = df['created_at_datetime'].min().strftime('%Y-%m-%d')
        else:
            # Establecer la fecha de inicio como '2024-12-31' para que la búsqueda comience desde el principio
            start_date = '2024-12-31'
        
        # Realizar la búsqueda de tweets
        busqueda='<palabra a buscar, por ejemplo "viewnext"> until:' + start_date
        print("###################### NUEVA BUSQUEDA ######################")
        print("REALIZANDO BUSQUEDA: ",busqueda)

        tweets = client.search_tweet(busqueda, 'Latest')
        while True:
            
            for tweet in tweets:

                # ignorar los tweets que su idioma es 'hi', 'tl' o 'in'
                if tweet.lang == 'hi' or tweet.lang == 'tl' or tweet.lang == 'in':
                    continue

                #ignorar los tweets que son de usuarios que contengan 'ayesa' en su nombre exceptuando a '@AyesaNews'
                if 'ayesa' in tweet.user.screen_name.lower() and tweet.user.screen_name.lower() != 'ayesanews':
                    continue

                # Añadir cada tweet al DataFrame
                clean_text = re.sub(r'[^\w\s#@/:%.,_-]', '', tweet.text)
                
                df.loc[len(df)] = {
                    'user_id': tweet.user.id,  # Añadir el ID del usuario
                    'username': tweet.user.screen_name,  # Añadir el nombre de usuario
                    'id': tweet.id,
                    'created_at_datetime': tweet.created_at_datetime,
                    'text': clean_text.replace('\n', ' '),  # Reemplazar los saltos de línea por espacios y eliminar los emoticonos
                    'lang': tweet.lang if tweet.lang else 'N/A',
                    'reply_count': tweet.reply_count,
                    'retweet_count': tweet.retweet_count,
                    'favorite_count': tweet.favorite_count,
                    'view_count': tweet.view_count,
                    'location': tweet.user.location if tweet.user.location else 'N/A',
                    'hashtags': tweet.hashtags if tweet.hashtags else 'N/A',
                    'in_reply_to': tweet.in_reply_to if tweet.in_reply_to else 'N/A',
                    'quote': tweet.quote,
                    'quote_count': tweet.quote_count
                }

            # Asegurarse de que todos los datos en 'created_at_datetime' son de tipo datetime
            df['created_at_datetime'] = pd.to_datetime(df['created_at_datetime'])
            
            # Ahora puedes obtener la fecha máxima sin problemas
            start_date = df['created_at_datetime'].min().strftime('%Y-%m-%d')
            print("La nueva fecha maxima es: ", start_date)

            # Guardar el DataFrame en un archivo CSV
            df.to_csv('tweets-beastmode.csv', index=False, sep='*')

            tweets = tweets.next()
            counter += 1

            #Con este codigo tenemos un control sobre el numero de peticiones, 120 peticiones cada 15 minutos
            if counter % 20 == 0:
                print('Espera de seguridad, esperando 15 segundos...')
                time.sleep(15)
                print('Peticion Nº ', counter)
            else:
                print('5 segundos para la proxima peticion...')
                time.sleep(2)  # Esperar 6 segundos entre cada petición
                print('Peticion Nº ', counter)
            
            # Obtener el ID del último tweet
            current_tweet_id = df.iloc[-1]['id']

            # Comprobar si el ID del último tweet es el mismo que el anterior
            if current_tweet_id == last_tweet_id:
                repeat_count += 1
            else:
                repeat_count = 0

            # Actualizar el ID del último tweet
            last_tweet_id = current_tweet_id

            # Si el ID del último tweet es el mismo 3 veces seguidas, cambiar a las siguientes credenciales
            if repeat_count == 3:
                index = (index + 1) % len(credentials)
                repeat_count = 0
                raise twikit.errors.TooManyRequests

    except (IndexError, twikit.errors.TooManyRequests):
        # Restar un día a la fecha de inicio y cambiar a las siguientes credenciales de usuario
        start_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        index = (index + 1) % len(credentials)
        continue