import os
import zipfile
from utils.util import _make_folder

def extract_data():
    #Files in raw folder
    files_esta = os.listdir('airflow/data/raw')
    files_sup = os.listdir('airflow/data/raw/support')
    BRONZE_DIR = 'airflow/data/bronze/extract'
    BRONZE_DIR_SUP = 'airflow/data/bronze/support'


    _make_folder(BRONZE_DIR)
    _make_folder(BRONZE_DIR_SUP)


    #extract each file and dump into extract folder
    for file in files_esta:
        if file.endswith('.zip'):
            file_name = file.split('.')[0]
            file_path = os.path.join('airflow/data/raw',file)
            with zipfile.ZipFile(file_path,'r') as zip_ref:
                zip_ref.extractall(f'airflow/data/bronze/extract/{file_name}')
            print('Done')

    #extract each file and dump into extract folder
    for file in files_sup:
        if file.endswith('.zip'):
            file_name = file.split('.')[0]
            file_path = os.path.join('airflow/data/raw/support',file)
            with zipfile.ZipFile(file_path,'r') as zip_ref:
                zip_ref.extractall(f'airflow/data/bronze/support/{file_name}')
            print('Done')