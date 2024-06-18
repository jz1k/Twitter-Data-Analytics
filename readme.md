# Twitter Data Analytics

Este proyecto tiene como objetivo analizar datos de Twitter, abarcando desde la extracción y procesamiento de datos hasta su visualización en Power BI. A continuación se detallan los pasos y componentes del proyecto:

## Contenido

1. [Visión Personal del Proyecto](#visión-personal-del-proyecto)
2. [Creación Diagrama BBDD](#creación-diagrama-bbdd)
3. [Blob Storage, Pipelines y DataFlow](#blob-storage-pipelines-y-dataflow)
4. [Databricks](#databricks)
5. [OutputDataFlow](#outputdataflow)
6. [Power BI](#power-bi)

## Visión Personal del Proyecto

Este proyecto se centra en el análisis de datos de Twitter para extraer información valiosa a través de diferentes etapas, desde la ingesta de datos hasta su análisis y visualización.

![esquema proyecto](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/capturas/diagrama.png?raw=true)

## Creación Diagrama BBDD

- **Relaciones de la BBDD**:
  - `tuits.user_id < users.id`: Un usuario puede tener muchos tuits, pero cada tuit es creado por un único usuario.
  - `tuits.location_id - ubicaciones.id`: Cada tuit está asociado a una única ubicación.
  - `tuits.in_reply_to < tuits.id`: Un tuit puede tener muchas respuestas, pero cada respuesta solo puede ser a un único tuit.
  - `tuits.id_sentimiento < sentimientos.id`: Un tuit puede tener asociado uno o más sentimientos.
  - `tuits.id_tema_conversacion - temas_conversacion.id`: Un tuit puede estar asociado a un tema de conversación.

  ![diagrama e-r](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/capturas/bbdd.png?raw=true)

## Blob Storage, Pipelines y DataFlow

- **Organización de BlobStorage**:
  - Los datos se obtienen y filtran mediante variables, luego se procesan los datos de todos los CSV (viewnext, hiberus, minsait y nttdata).
    ![estructura](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/capturas/tree.jpg?raw=true)
  - Los datos RAW se combinan con la Localización y los Temas de cada empresa mediante un JOIN por ID.
    ![get](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/capturas/get.jpg?raw=true)
  - Se añade una columna indicando a qué empresa pertenecen esos datos para su visualización e implementación en la BBDD SQL.
    ![addempresa](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/capturas/addempresa.jpg?raw=true)


## Databricks
![parametrizacion databricks](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/capturas/paradata.jpg?raw=true)

- **Código Databricks**:
  ```python
  from transformers import pipeline
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import udf
  from pyspark.sql.types import ArrayType, StringType

  spark = SparkSession.builder.getOrCreate()
  clasificador = pipeline(model="lxyuan/distilbert-base-multilingual-cased-sentiments-student", return_all_scores=True)

  def polaridad(scores):
      scores = scores[0]
      positive_score = scores[0]['score']
      neutral_score = scores[1]['score']
      negative_score = scores[2]['score']
      polarity_score = positive_score - negative_score + neutral_score
      return positive_score, negative_score, neutral_score, polarity_score

  def sentimiento(scores):
      positive_score, negative_score, neutral_score, polarity_score = polaridad(scores)
      if positive_score > max(negative_score, neutral_score) and polarity_score > 0.75:
          if neutral_score > 0.1 or positive_score < 0.7:
              return ['Neutral', polarity_score]
          else:
              return ['Positivo', polarity_score]
      elif negative_score > max(positive_score, neutral_score) and polarity_score < -0.3:
          return ['Negativo', polarity_score]
      else:
          return ['Neutral', polarity_score]

  df = spark.read.csv(rutaImput, header=True, inferSchema=True, sep='*')

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

  df = df.filter(df['id'].isin(ids_procesados))
  sentimientos_df = spark.createDataFrame(sentimientos, ['id', 'Sentimiento', 'Polaridad'])
  merged_df = df.join(sentimientos_df, "id", "left")
  df_single_partition = merged_df.coalesce(1)
  df_single_partition.write.mode("overwrite").option("header", "true").csv(rutaOutput + ruta, sep='*')

  generated_files = dbutils.fs.ls(rutaOutput + ruta)
  generated_file_name = next(file.name for file in generated_files if file.name.startswith("part-0000"))
  dbutils.fs.mv(rutaOutput + ruta + "/" + generated_file_name, rutaOutput + ruta + "_sentiments.csv")

## OutputDataFlow

- **Primer intento**: No completado debido a limitaciones de tiempo.
    ![output fail](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/capturas/outputfail.jpg?raw=true)


- **DataFlow Final**: Debido a temas de tiempo, se realizó una estructura mucho mas simple
  - Se parametriza con el nombre de la empresa y se hace la ingesta de los datos procesados y los sentimientos.
  - Se unen con un JOIN RIGHT unificando IDs para evitar duplicaciones, agrupando por ID.
  - Se envía mediante el SINK parametrizado a la base de datos SQL.
    ![output](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/capturas/output.jpg?raw=true)

## Power BI

- **Análisis en Power BI**:
  - Se obtienen los datos desde Azure SQL Database.
  - Análisis sobre Tuits, Temas, Sentimientos y Ubicación.
  - Funcionalidad para visualizar datos de cada empresa individualmente o en conjunto.
  - Problema con DirectQuery solucionado importando la BBDD para un gráfico específico.
  
  [Visualización datos en Power BI](https://github.com/jz1k/Twitter-Data-Analytics/blob/main/Estadisticas/Visualizacion_Bailon_TwitterData.pdf)
