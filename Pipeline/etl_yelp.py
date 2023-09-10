import etl_functions as etl
from pyspark.sql.functions import collect_list, regexp_replace, col, date_format, struct,to_json
import pandas as pd
import logging
import os
# Establecer nivel de log a ERROR para evitar los warnings no deseados
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

print("***")
print("A continuación ingrese uno a uno los estados que quiere actualizar:")
states_list = []
flag = "stop"
while True:
    user_input = input("Ingrese el nombre del estado o 'stop' para finalizar: ")
    if user_input.lower() == flag.lower():
        break
    state_name = user_input.title()
    states_list.append(state_name)
print("***")
print("Estados que serán cargados:")
print(states_list)

for state in states_list:
    #Obtengo el acrónimo
    state_acronym = etl.get_state_acronym(state)
    spark = etl.create_spark_session()
    ruta_archivo_pickle = r'raw_data/Yelp/business_v1.pkl'
    df_pandas = pd.read_pickle(ruta_archivo_pickle)
    df = spark.createDataFrame(df_pandas)
    df_state = df.filter(df.state == state_acronym)
    #Seleccionar las columnas que serán usadas:
    selected_columns = ["business_id", "name", "address", "latitude", "longitude", "stars", "categories", "hours"]
    df_state = df_state.select(selected_columns)
    #Cambio el nombre a avg_stars para que no haya confusion con las stars de las reseñas individuales
    df_state = df_state.withColumnRenamed("stars", "avg_stars")    
    #Guardar el DataFrame filtrado como un archivo parquet
    df_state.write.format("parquet").save(f"states_files_ongoing/yelp_business_{state}")
    #guardo los bussines id que corresponden a florida:
    yelp_id_list = df_state.select("business_id").distinct().rdd.flatMap(lambda x: x).collect()
    yelp_id_set = set(yelp_id_list)
    with open(f"states_files_ongoing/yelp_ids_{state}.txt", "w") as file:
        for yelpid in yelp_id_set:
            file.write(yelpid + "\n")    
    spark.stop()
    #PARTE 2 - Obtener reseñas:
    spark = etl.create_spark_session()
    df2 = spark.read.json(r"raw_data/Yelp/review-002.json")
    #filtro solo las review del estado:
    # Leer el archivo con los yelp_id únicos
    with open(f"states_files_ongoing/yelp_ids_{state}.txt", "r") as file:
        yelp_ids = file.read().splitlines()
    # Filtrar las filas que contienen yelpid presentes en el archivo
    df2_state = df2.filter(df2['business_id'].isin(yelp_ids))
        #Seleccionar las columnas que serán usadas:
    selected_columns = ["business_id", "date", "stars", "text"]
    df2_state = df2_state.select(selected_columns)     
    # Guardar el DataFrame filtrado como un archivo parquet
    df2_state.write.format("parquet").save(f"states_files_ongoing/yelp_reviews_{state}")
    spark.stop()

    #UNIR RESEÑAS CON BUSINESS:
    spark = etl.create_spark_session()
    #business
    ruta_directorio_parquet = f"states_files_ongoing/yelp_business_{state}"
    df = spark.read.parquet(ruta_directorio_parquet)
    #reseñas
    ruta_directorio_parquet = f"states_files_ongoing/yelp_reviews_{state}"
    df2 = spark.read.parquet(ruta_directorio_parquet)
    #AGRUPAR RESEÑAS:🍀🤞🏽
    # Reemplazar comas (',') por punto y coma (';') en la columna 'text'
    df2 = df2.withColumn("text", regexp_replace("text", ",", ";"))
    df2 = df2.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))

    grouped_df = df2.groupBy("business_id") \
        .agg(
            to_json(collect_list(struct("text", "stars", "date"))).alias("reviews")
        )

    #JOIN RESEÑAS CON BUSINESS:🍀🤞🏽
    # Unir los DataFrames utilizando la columna "business_id" como clave
    merged_df = df.join(grouped_df, on="business_id", how="left_outer")
    merged_df_single_partition = merged_df.coalesce(1)
    # Guardar el DataFrame unido
    merged_df_single_partition.write.format("parquet").save(f"states_files_ongoing/yelp_dataset_{state}")
    spark.stop()
