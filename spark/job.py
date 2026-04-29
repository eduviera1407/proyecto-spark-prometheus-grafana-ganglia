from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, split, explode, desc

# Crea la sesión de Spark y asigna un nombre a la aplicación
spark = (
    SparkSession.builder
    .appName("NetflixAnalysis")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

# Reduce el nivel de logs 
spark.sparkContext.setLogLevel("ERROR")



# Ruta del archivo CSV dentro del contenedor
csv_path = "/opt/spark/work-dir/netflix_titles.csv"

# Lee el CSV con cabecera, detección automática de tipos
# y soporte para campos multilínea y comillas escapadas
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("escape", "\"")
    .csv(csv_path)
)

# Limpia espacios en blanco al inicio y final de columnas clave
# para evitar errores o duplicados por formato
df = (
    df.withColumn("type", trim(col("type")))
      .withColumn("country", trim(col("country")))
      .withColumn("director", trim(col("director")))
      .withColumn("listed_in", trim(col("listed_in")))
)

# Filtra solo los registros cuyo tipo sea "Movie" o "TV Show"
df_valid_types = df.filter(col("type").isin("Movie", "TV Show"))

# Cuenta cuántos títulos hay de cada tipo y los ordena alfabéticamente
# El collect() trae el resultado al driver para poder imprimirlo
titles_by_type = (
    df_valid_types.groupBy("type")
                  .count()
                  .orderBy("type")
                  .collect()
)

# Filtra registros con país no nulo
# Divide los países cuando hay varios en una misma celda
# y los expande en varias filas para analizarlos por separado
country_df = (
    df.filter(col("country").isNotNull())
      .withColumn("country_exploded", explode(split(col("country"), ",")))
      .withColumn("country_exploded", trim(col("country_exploded")))
)

# Agrupa por país, cuenta cuántas producciones tiene cada uno
# y se queda con el país con más apariciones
top_country = (
    country_df.groupBy("country_exploded")
              .count()
              .orderBy(desc("count"))
              .first()
)

# Agrupa por año de lanzamiento, cuenta títulos
# y obtiene el año con mayor número de lanzamientos
top_year = (
    df.filter(col("release_year").isNotNull())
      .groupBy("release_year")
      .count()
      .orderBy(desc("count"))
      .first()
)

# Filtra registros con director no nulo
# Divide directores múltiples en filas independientes
director_df = (
    df.filter(col("director").isNotNull())
      .withColumn("director_exploded", explode(split(col("director"), ",")))
      .withColumn("director_exploded", trim(col("director_exploded")))
)

# Agrupa por director, cuenta títulos
# y obtiene el director con más apariciones
top_director = (
    director_df.groupBy("director_exploded")
               .count()
               .orderBy(desc("count"))
               .first()
)

# Filtra registros con categoría no nula
# Divide varias categorías en filas separadas
category_df = (
    df.filter(col("listed_in").isNotNull())
      .withColumn("category_exploded", explode(split(col("listed_in"), ",")))
      .withColumn("category_exploded", trim(col("category_exploded")))
)

# Agrupa por categoría, cuenta ocurrencias
# y obtiene la categoría más frecuente
top_category = (
    category_df.groupBy("category_exploded")
               .count()
               .orderBy(desc("count"))
               .first()
)

# Imprime una cabecera para localizar fácilmente los resultados en la salida
print("===== RESULTADOS DEL ANÁLISIS =====")

# Muestra el número de títulos por tipo
print("\n1. Número de títulos por tipo:")
for row in titles_by_type:
    print(f"{row['type']}: {row['count']}")

# Muestra los resultados principales del análisis
print(f"\n2. País con más producciones: {top_country['country_exploded']} ({top_country['count']})")
print(f"3. Año con más lanzamientos: {top_year['release_year']} ({top_year['count']})")
print(f"4. Director con más títulos: {top_director['director_exploded']} ({top_director['count']})")
print(f"5. Categoría más frecuente: {top_category['category_exploded']} ({top_category['count']})")

# Cierra la sesión de Spark al finalizar
spark.stop()