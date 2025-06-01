import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "http://vitibrasil.cnpuv.embrapa.br/index.php?opcao="  


def scrape_producao():
    url = f"{BASE_URL}opt_02"  
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tb_base tb_dados'})
    rows = table.find_all('tr')
    data=[]
    for row in rows:
        cells = row.find_all(['th', 'td']) 
        cells_text = [cell.get_text(strip=True) for cell in cells]
        tipo = None
        if cells[0].name == 'td':
            classes = cells[0].get('class', [])
            if 'tb_item' in classes:
                tipo = 'item'
            elif 'tb_subitem' in classes:
                tipo = 'subitem'
            cells_text.append(tipo)
        data.append(cells_text)
    header = ['produto','quantidade','tipo']
    df = pd.DataFrame(data[1:], columns=header)
    return df[:-1]

def scrape_processamento():
    url = f"{BASE_URL}opt_03"  
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tb_base tb_dados'})
    rows = table.find_all('tr')
    data=[]
    for row in rows:
        cells = row.find_all(['th', 'td']) 
        cells_text = [cell.get_text(strip=True) for cell in cells]
        tipo = None
        if cells[0].name == 'td':
            classes = cells[0].get('class', [])
            if 'tb_item' in classes:
                tipo = 'item'
            elif 'tb_subitem' in classes:
                tipo = 'subitem'
            cells_text.append(tipo)
        data.append(cells_text)
    header = ['cultivar','quantidade','tipo']
    df = pd.DataFrame(data[1:], columns=header)
    return df[:-1]

def scrape_comercializacao():
    url = f"{BASE_URL}opt_04"  
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tb_base tb_dados'})
    rows = table.find_all('tr')
    data=[]
    for row in rows:
        cells = row.find_all(['th', 'td']) 
        cells_text = [cell.get_text(strip=True) for cell in cells]
        tipo = None
        if cells[0].name == 'td':
            classes = cells[0].get('class', [])
            if 'tb_item' in classes:
                tipo = 'item'
            elif 'tb_subitem' in classes:
                tipo = 'subitem'
            cells_text.append(tipo)
        data.append(cells_text)
    header = ['produto','quantidade','tipo']
    df = pd.DataFrame(data[1:], columns=header)
    return df[:-1]

def scrape_importacao():
    url = f"{BASE_URL}opt_05"  
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tb_base tb_dados'})
    rows = table.find_all('tr')
    data=[]
    for row in rows:
        cells = row.find_all(['th', 'td']) 
        cells_text = [cell.get_text(strip=True) for cell in cells]
        data.append(cells_text)
    header = ['pais','quantidade','valor']
    df = pd.DataFrame(data[1:], columns=header)
    return df[:-1]

def scrape_exportacao():
    url = f"{BASE_URL}opt_06"  
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tb_base tb_dados'})
    rows = table.find_all('tr')
    data=[]
    for row in rows:
        cells = row.find_all(['th', 'td']) 
        cells_text = [cell.get_text(strip=True) for cell in cells]
        data.append(cells_text)
    header = ['pais','quantidade','valor']
    df = pd.DataFrame(data[1:], columns=header)
    return df[:-1]
