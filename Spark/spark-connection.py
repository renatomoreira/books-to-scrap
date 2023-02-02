from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("bookScrepingProject") \
    .getOrCreate()

# Carregamento de dados de um arquivo no repositório ADLS2
df = spark.read.json("C:/Users/Dell/Documents/Github/books-to-scrap/books/*.json")

# Exibição do conteúdo do dataframe
df.show()

# Conectando a Azure
storage_account_name = "anlhubdatalake"       
storage_accont_key = "crWTPUnuJytuEdOjJ/x0oIm509ltJ9pFum7uRV8oV9tpwDa1/zJhY1sihntbI4tVXYQMfU0UDdQl+ASt3JdtZg=="         
connection_string = "DefaultEndpointsProtocol=https;AccountName=anlhubdatalake;AccountKey=LTPv7rq5N2iaICgzYwGOvnl5SvLAD3uZxh0Hl8Qx3LvNUHTXy+6wliupXVCQkcRlDxOp1sf96KX1+AStcNnbjw==;EndpointSuffix=core.windows.net"  
container_name = "landing/books"
client_id     = "efb5984e-f6c9-414a-b380-f36859b51206"     
tenant_id     = "9345eb26-9c82-4616-9408-9d626d95732d"     
client_secret = "FxM8Q~P~It7HiNuHsjZ.u6OdRIVITZf9x6aVKb8y"  

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", f"{client_id}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",f"{client_secret }")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Escrevendo o arquivo (NÃO FUNCIONA)
df.write.format("delta").save(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/books")

# Finalização da sessão Spark
spark.stop()