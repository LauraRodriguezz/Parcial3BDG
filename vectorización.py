from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf, concat_ws
from pyspark.sql.types import StringType
import datetime

# Configura la sesión de Spark
spark = SparkSession.builder \
    .appName("NewsProcessingPipeline") \
    .getOrCreate()

# Ruta al archivo de datos
bucket = 'headlinesfinal023'
key = f"periodico=ElTiempo/year={datetime.datetime.now().strftime('%Y')}/month={datetime.datetime.now().strftime('%m')}/day={datetime.datetime.now().strftime('%d')}.csv"
file_path = f"s3://{bucket}/{key}"

# Carga los datos del archivo CSV
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Define las etapas del pipeline
regex_tokenizer = RegexTokenizer(inputCol="Titular", outputCol="words", pattern="\\W")
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
count_vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="raw_features")
idf = IDF(inputCol="raw_features", outputCol="features")

# Crea el pipeline
pipeline = Pipeline(stages=[regex_tokenizer, stopwords_remover, count_vectorizer, idf])

# Aplica el pipeline a los datos
pipeline_model = pipeline.fit(df)
processed_data = pipeline_model.transform(df)

# Convierte la columna "features" en una cadena de texto
array_to_string_udf = udf(lambda arr: ' '.join(str(x) for x in arr), StringType())
processed_data = processed_data.withColumn("features_str", array_to_string_udf(processed_data["features"]))

# Muestra los resultados
processed_data.show()

# Guarda los resultados en un nuevo archivo CSV
output_path = "s3://parcialfinal11/processed_data"
processed_data.select("Categoria", "Titular", "Enlace", "features_str").write.csv(output_path, header=True, mode="overwrite")
