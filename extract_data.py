import os
import zipfile

#Files in raw folder
files_esta = os.listdir('data/raw')
files_sup = os.listdir('data/raw/support')

#extract each file and dump into extract folder
for file in files_esta:
    if file.endswith('.zip'):
        file_name = file.split('.')[0]
        file_path = os.path.join('data/raw',file)
        with zipfile.ZipFile(file_path,'r') as zip_ref:
            zip_ref.extractall(f'data/bronze/extract/{file_name}')
        print('Done')

#extract each file and dump into extract folder
for file in files_sup:
    if file.endswith('.zip'):
        file_name = file.split('.')[0]
        file_path = os.path.join('data/raw/support',file)
        with zipfile.ZipFile(file_path,'r') as zip_ref:
            zip_ref.extractall(f'data/bronze/support/{file_name}')
        print('Done')