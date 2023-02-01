from pyspark.sql import SparkSession

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("bookScrepingProject") \
    .getOrCreate()

# Carregamento de dados de um arquivo no repositório ADLS2
df = spark.read.json("C:/Users/Dell/OneDrive - Analytics Hub/Github/books-to-scrap/books/*.json")

# Exibição do conteúdo do dataframe
df.show()

# Finalização da sessão Spark
spark.stop()

