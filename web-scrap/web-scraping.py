import requests
import os
import json
from bs4 import BeautifulSoup
import time

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
        
        # Incrementa o ID do livro
        book_id += 1
