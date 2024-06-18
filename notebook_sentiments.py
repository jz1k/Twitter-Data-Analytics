# Configurar las credenciales del Blob Storage
spark.conf.set(
    "urldatalake",
    "token"
)

# Establecer la ubicación del archivo en el data lake
file_location = "wasbs://direccionBlobStorage/"

# Entra por parametro el nombre del archivo
ruta = dbutils.widgets.get("ruta")
#ruta = "prueba"


# Establecer la ruta de entrada y salida
rutaImput = file_location + "/raw/"+ruta+".csv"
rutaOutput = file_location + "/sentiments/"+ruta

from transformers import pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

# Inicializar SparkSession
spark = SparkSession.builder.getOrCreate()

# Inicializar el clasificador
clasificador = pipeline(
    model="lxyuan/distilbert-base-multilingual-cased-sentiments-student", 
    return_all_scores=True
)

# Función para calcular la polaridad
def polaridad(scores):
    scores = scores[0]
    positive_score = scores[0]['score']
    neutral_score = scores[1]['score']
    negative_score = scores[2]['score']
    polarity_score = positive_score - negative_score + neutral_score
    return positive_score, negative_score, neutral_score, polarity_score

def sentimiento(scores):
    positive_score, negative_score, neutral_score, polarity_score = polaridad(scores)

    # Si la puntuación positiva es la más alta y la puntuación de polaridad es mayor que 0.66
    if positive_score > max(negative_score, neutral_score) and polarity_score > 0.75:
        # Si la puntuación neutral es mayor que 0.1 o la puntuación positiva es menor que 0.6
        if neutral_score > 0.1 or positive_score < 0.7:
            return ['Neutral', polarity_score]
        else:
            return ['Positivo', polarity_score]
    # Si la puntuación negativa es la más alta y la puntuación de polaridad es menor que -0.3
    elif negative_score > max(positive_score, neutral_score) and polarity_score < -0.3:
        return ['Negativo', polarity_score]
    else:
        return ['Neutral', polarity_score]

# Leer el archivo usando Spark
df = spark.read.csv(rutaImput, header=True, inferSchema=True, sep='*')

# Limitar el DataFrame a 100 filas
df = df.limit(100)

# Recorre con spark el DataFrame, la columna text y clasifica los tuits
sentimientos = []

for row in df.collect():
    try:
        scores = clasificador(row['text'])
        sent = sentimiento(scores)
        sentimientos.append((row['id'],) + tuple(sent))
    except ValueError as e:
        print(f"Error al procesar el texto con ID {row['id']} : {e}")
        # Si ocurre un error, asignar sentimiento y polaridad como None
        #sentimientos.append((row['id'], 0, 0, 0))



# Convertir la lista de sentimientos en un DataFrame de Spark
sentimientos_df = spark.createDataFrame(sentimientos, ['id', 'Sentimiento', 'Polaridad'])

# Concatenar el DataFrame original con el DataFrame de sentimientos usando el ID del tuit
merged_df = df.join(sentimientos_df, "id", "left")

# Mostrar el DataFrame resultante
display(merged_df)

merged_df.write.mode("overwrite").csv(rutaOutput, sep='*', header=True)

