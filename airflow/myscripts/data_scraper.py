from bs4 import BeautifulSoup as bs
import requests
# import pyarrow.parquet as pq
# from io import BytesIO
import os

yellow_DIR = "data/yellowData"
green_DIR = "data/greenData"

def saveFile(DIR,filename,link):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 YaBrowser/19.6.1.153 Yowser/2.5 Safari/537.36'}
    response = requests.get(link.strip(), headers= headers)
    
    if response.status_code == 200:
        _path = os.path.join(DIR, filename)
        if not os.path.isfile(_path):
            open(_path,"wb").write(response.content)
            print("wrote", filename)
        # parquet_content = BytesIO(response.content)
        
        # parquet_table = pq.read_table(parquet_content)
        
        # pq.write_table(parquet_table, os.path.join(DIR, filename))
        
    else:
        print(response.status_code)
    


def extractFromA(lis):
    for li in lis:
        link = li.find("a", href = True)["href"]

        filename = link.split('/')[-1].strip()

        if "yellow" in filename:
            saveFile(yellow_DIR, filename,link.strip())

        # if "green" in filename:
        #     saveFile(green_DIR, filename,link)


def scraper():
    URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    FILETYPE = ".parquet"

    def getSoup(url):
        return bs(requests.get(url).text,'html.parser')

    var = getSoup(URL)

    first_table = var.find_all("table")[0]

    if not os.path.exists(yellow_DIR):
        os.makedirs(yellow_DIR)
    # if not os.path.exists(green_DIR):
    #     os.makedirs(green_DIR)
    
    
    for ul in first_table.find_all("ul"):
        extractFromA(ul.find_all("li")[:2])


if __name__ == "__main__":
    scraper()
