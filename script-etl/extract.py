import os
import requests
import pandas as pd
from datetime import datetime
import logging

def extract_meteo(city: str, api_key: str, date: str) -> bool:
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',  
            'lang': 'en'
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status() 
        
        data = response.json()
        weather_data = {
            'ville': city,
            'date_extraction': datetime.now().strftime("%Y-%m-%d"),
            'temperature': data['main']['temp'],
            'temp_min': data['main']['temp_min'],
            'temp_max': data['main']['temp_max'],
            'humidite': data['main']['humidity'],
            'pression': data['main']['pressure'],
            'vent_vitesse': data['wind']['speed'],
            'vent_direction': data['wind'].get('deg', None),
            'precipitation': data.get('rain', {}).get('1h', 0),
            'description': data['weather'][0]['description'],
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon']
        }
        
        os.makedirs(f"/home/tojo/airflow/data/raw/{date}", exist_ok=True)
        
        pd.DataFrame([weather_data]).to_csv(
            f"/home/tojo/airflow/data/raw/{date}/meteo_{city}.csv", 
            index=False 
        )
        
        return True
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}")
        
    return False

