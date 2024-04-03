import requests
from bs4 import BeautifulSoup
import os
import concurrent.futures

#Get href values for download func

url = 'https://dados.rfb.gov.br/CNPJ/'


urls = []
estabelecimentos = []

content = requests.get(url)

soup = BeautifulSoup(content.text,'html')


#Download function
def download_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(f'data/raw/{filename}', 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully as '{filename}'")
    else:
        print(f"Failed to download file from '{url}'")


#Get estabelecimento href value
for ancor in soup.find_all('a'):
    if 'estabelecimentos' in ancor.text.lower():
        estabelecimentos.append(ancor['href'])
    else:
        pass

#get urls to download files simultenously
for i in estabelecimentos:
    url = f'https://dados.rfb.gov.br/CNPJ/{i}'
    urls.append(url)

# get name of files in lowercase
filenames = [lower.lower() for lower in estabelecimentos]

#Define number of threads
num_threads = min(len(urls),10)

# Download files using multiple threads
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    executor.map(download_file, urls, filenames)    
