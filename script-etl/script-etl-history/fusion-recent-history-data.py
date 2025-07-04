import pandas as pd
import os

#Combine les donnees historique transformer avec les donnees recent transformer
def merge_history_to_recent() -> str:
    """Ajoute les nouvelles données au fichier global existant"""
    input_dir = f"/home/tojo/airflow/data/star_schema"
    output_file = "/home/tojo/airflow/data/star_schema/fact_weather.csv"
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    if os.path.exists(output_file):
        global_df = pd.read_csv(output_file)
    else:
        global_df = pd.DataFrame()
        print(f"{output_file} created!")
    
    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith('fact_weather_history') and file.endswith('.csv'):
            new_data.append(pd.read_csv(f"{input_dir}/{file}"))
    
    if not new_data:
        raise ValueError(f"Aucune nouvelle donnée à fusionner")
    
    updated_df = pd.concat([global_df] + new_data, ignore_index=True)
    updated_df = updated_df.drop_duplicates(
        subset=['ville_id', 'date_extraction'],  
        keep='last'                          
    )
    
    updated_df.to_csv(output_file, index=False)
    return output_file

if __name__ == "__main__":
    merge_history_to_recent()
    print("Fusion terminer!")