from transformers import pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

# Inicializar SparkSession
spark = SparkSession.builder.getOrCreate()

# Leer el archivo usando Spark
df = spark.read.csv(rutaImput, header=True, inferSchema=True, sep='*')

# Limitar el DataFrame a 100 filas
#df = df.limit(100)

# Busca si hay valores nulos en el id
def idNull(id):
    if id is None:
        return 'Null'
    else:
        return 'Not Null'

# Crear la función UDF
idNull_udf = udf(idNull, StringType())

# Aplicar la función UDF al DataFrame
df = df.withColumn('idNull', idNull_udf(df['id']))

# Mostramos solo las columnas que idNull es Null y su dato anterior para encontrar
df = df.filter(df['idNull'] == 'Null')

df = df.select('id', 'idNull')

# Mostrar el DataFrame resultante
display(df)
