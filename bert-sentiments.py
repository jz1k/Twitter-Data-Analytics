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
#df = df.limit(100)

# Recorre con spark el DataFrame, la columna text y clasifica los tuits
sentimientos = []
ids_procesados = []


for row in df.collect():
    try:
        scores = clasificador(row['text'])
        sent = sentimiento(scores)
        sentimientos.append((row['id'],) + tuple(sent))
        ids_procesados.append(row['id'])
    except ValueError as e:
        print(f"Error al procesar el texto con ID {row['id']} : {e}")


# Filtrar el DataFrame original para que sólo contenga las filas que se procesaron correctamente
df = df.filter(df['id'].isin(ids_procesados))

# Convertir la lista de sentimientos en un DataFrame de Spark
sentimientos_df = spark.createDataFrame(sentimientos, ['id', 'Sentimiento', 'Polaridad'])

# Concatenar el DataFrame original con el DataFrame de sentimientos usando el ID del tuit
merged_df = df.join(sentimientos_df, "id", "left")

# Mostrar el DataFrame resultante
#display(merged_df)

# Hacemos el df una sola particion
df_single_partition = merged_df.coalesce(1)

# Escribir el DataFrame en el Blob Storage con un nombre de archivo personalizado
df_single_partition.write.mode("overwrite").option("header", "true").csv(rutaOutput + ruta, sep='*')

# Obtener el nombre del archivo generado (que comienza con "part-00000")
generated_files = dbutils.fs.ls(rutaOutput + ruta)
generated_file_name = next(file.name for file in generated_files if file.name.startswith("part-0000"))

# Renombrar el archivo generado con el nombre personalizado
dbutils.fs.mv(rutaOutput + ruta + "/" + generated_file_name, rutaOutput + ruta + "_sentiments.csv")
