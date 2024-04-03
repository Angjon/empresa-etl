import os
import zipfile

#Files in raw folder
files = os.listdir('data/raw')

#extract each file and dump into extract folder
for file in files:
    if file.endswith('.zip'):
        file_name = file.split('.')[0]
        file_path = os.path.join('data/raw',file)
        with zipfile.ZipFile(file_path,'r') as zip_ref:
            zip_ref.extractall(f'data/raw/extract/{file_name}')
        print('Done')