from azure.storage.blob import BlobServiceClient
import requests
import os
import json
from bs4 import BeautifulSoup
import time


# Conectando a Azure
storage_account_name = "anlhubdatalake"       
storage_accont_key = "crWTPUnuJytuEdOjJ/x0oIm509ltJ9pFum7uRV8oV9tpwDa1/zJhY1sihntbI4tVXYQMfU0UDdQl+ASt3JdtZg=="         
connection_string = "DefaultEndpointsProtocol=https;AccountName=anlhubdatalake;AccountKey=LTPv7rq5N2iaICgzYwGOvnl5SvLAD3uZxh0Hl8Qx3LvNUHTXy+6wliupXVCQkcRlDxOp1sf96KX1+AStcNnbjw==;EndpointSuffix=core.windows.net"  
container_name = "landing/books-to-scrap"
client_id     = "efb5984e-f6c9-414a-b380-f36859b51206"     
tenant_id     = "9345eb26-9c82-4616-9408-9d626d95732d"     
client_secret = "FxM8Q~P~It7HiNuHsjZ.u6OdRIVITZf9x6aVKb8y"  


# Cria o diretório para salvar os arquivos
directory = "books"
if not os.path.exists(directory):
    os.makedirs(directory)

# Número de livros a serem coletados
number_of_books = 100

# ID do livro
book_id = 1

# URL base para coleta dos dados
base_url = "http://books.toscrape.com/catalogue/page-{}.html"

# Loop para acessar cada página do catálogo
for page in range(1, number_of_books // 20 + 2):
    # Acessa a página do catálogo
    url = base_url.format(page)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Seleciona cada livro na página
    books = soup.select("article.product_pod")
    
    for book in books:
        # Se o ID do livro atual é maior que o número de livros desejado, para o loop
        if book_id > number_of_books:
            print("Concluído")
            break
        time.sleep(0.1)
        print(f"Processando dados... Total: {book_id}/{number_of_books}")
        
        # Extrai as informações do livro
        title = book.h3.a["title"]
        star_rating_class = book.select_one("p.star-rating").attrs["class"]
        star_rating = star_rating_class[1].split("-")[-1]
        availability = book.select_one("p.availability").get_text(strip=True)
        price = book.select_one("p.price_color").get_text(strip=True)
        
        # Armazena as informações do livro em um dicionário
        book_info = {
            "id": book_id,
            "title": title,
            "star_rating": star_rating,
            "availability": availability,
            "price": price
        }
        
        # Salva o livro em um arquivo .json
        file_path = os.path.join(directory, f"book_{book_id}.json")
        with open(file_path, "w") as file:
            json.dump(book_info, file)
        
        def uploadToBlobStorage(file_path, file_name):
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
                     
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data)
            print(f"uploaded {file_name} file")

        uploadToBlobStorage(f"C:/Users/Dell/Documents/Github/books-to-scrap/books/book_{book_id}.json", f"book-{book_id}.json")

        # Incrementa o ID do livro
        book_id += 1