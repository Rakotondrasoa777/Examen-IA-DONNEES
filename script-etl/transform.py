import pandas as pd
import os

def transform_to_star() -> str:
    input_file = "/home/tojo/airflow/data/processed/meteo_global.csv"  
    output_dir = "/home/tojo/airflow/data/star_schema"               
    os.makedirs(output_dir, exist_ok=True)        
    
    meteo_data = pd.read_csv(input_file)

    meteo_data.drop_duplicates(inplace=True)
    
    convert_column = ['temperature', 'precipitation', 'vent_vitesse', 'temp_max', 'temp_min']
    for col in convert_column:
        if col in meteo_data.columns:
            meteo_data[col] = pd.to_numeric(meteo_data[col], errors='coerce')

    dim_ville_path = f"{output_dir}/dim_ville.csv"
    
    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=['ville_id', 'ville', 'latitude', 'longitude'])
    
    # Identifier les nouvelles villes
    villes_existantes = set(dim_ville['ville'])
    nouvelles_villes = set(meteo_data['ville']) - villes_existantes

    # Extraire les infos pour les nouvelles villes
    info_nouvelles_villes = meteo_data[meteo_data['ville'].isin(nouvelles_villes)][['ville', 'latitude', 'longitude']].drop_duplicates(subset=['ville'])

    
    # Ajouter les nouvelles villes avec des IDs incrémentaux
    if nouvelles_villes:
        nouveau_id = dim_ville['ville_id'].max() + 1 if not dim_ville.empty else 1
        nouvelles_lignes = info_nouvelles_villes.copy()
        nouvelles_lignes['ville_id'] = range(nouveau_id, nouveau_id + len(nouvelles_lignes))
        # Réordonner les colonnes
        nouvelles_lignes = nouvelles_lignes[['ville_id', 'ville', 'latitude', 'longitude']]

        dim_ville = pd.concat([dim_ville, nouvelles_lignes], ignore_index=True)
        dim_ville.to_csv(dim_ville_path, index=False)  # Sauvegarde
    
    # 4. Création de la table de faits
    faits_meteo = meteo_data.merge(
        dim_ville,
        on='ville',
        how='left'
    ).drop(columns=['ville'])

    # Supprimer les colonnes en double (de meteo_data)
    faits_meteo = faits_meteo.drop(columns=[col for col in ['latitude_x', 'longitude_x', 'latitude_y', 'longitude_y'] if col in faits_meteo.columns])

    # 5. Sauvegarde des faits
    faits_path = f"{output_dir}/fact_weather.csv"
    
    if os.path.exists(faits_path):
        anciens_faits = pd.read_csv(faits_path)
        faits_meteo = pd.concat([anciens_faits, faits_meteo], ignore_index=True)
    
    # Supprimer les doublons basés sur ville_id + date_extraction
    if 'ville_id' in faits_meteo.columns and 'date_extraction' in faits_meteo.columns:
        faits_meteo.drop_duplicates(subset=['ville_id', 'date_extraction'], keep='last', inplace=True)


    faits_meteo.to_csv(faits_path, index=False, float_format="%.2f")
    
    return faits_path