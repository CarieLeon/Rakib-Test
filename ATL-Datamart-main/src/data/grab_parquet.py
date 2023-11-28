from minio import Minio
import urllib.request
import pandas as pd
import sys
from minio.error import S3Error
import os
import datetime
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

prefix_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'



def main():
    # Initialiser le client MinIO
    minioClient = Minio('localhost:9000',
                    access_key='minio',
                    secret_key='minio123',
                    secure=False)
    grab_data()

 # Vérifier si le seau existe, sinon le créer
    if not minioClient.bucket_exists("mybucket"):
        minioClient.make_bucket("mybucket")


def get_filename(years: int, month: int):
    return f"yellow_tripdata_{years}-{'0' if month < 10 else ''}{month}.parquet"

def grab_data() -> None:
    output_directory = '../../data/raw/'

    for years in range(2023, 2023):
        for month in range(1, 13):
            filename = get_filename(years, month)
            url = f"{prefix_url}/{filename}"

            # Crée le répertoire de destination s'il n'existe pas
            os.makedirs(output_directory, exist_ok=True)

            destination_path = os.path.join(output_directory, filename)
            urllib.request.urlretrieve(url, destination_path)

if __name__ == '__main__':
    main()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """


def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())

 # Enregistrer les données dans le seau MinIO
    try:
        # Définir le nom de l'objet
        object_name = f"{data['timestamp']}.json"

        # Encodage des données en JSON
        data_json = json.dumps(data).encode('utf-8')

        # Enregistrement des données dans le bucket MinIO
        minioClient.put_object(
            "mybucket",
            object_name,
            data_json,
            len(data_json),
            content_type='application/json'
        )

    except S3Error as e:
        print("Error:", e)

if __name__ == "__main__":
  main()