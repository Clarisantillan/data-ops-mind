from pyspark.sql.functions import collect_list, regexp_replace, col, struct,to_json
import etl_functions as etl
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

    # PARTE 1: Cargar las review para el estado seleccionado, unirlas en un solo archivo y guardar todos los gmapid:
    spark = etl.create_spark_session()
    #Leo la información.
    ruta_json = f"raw_data/Google_Maps/reviews-estados/review-{state}"
    archivos_en_ruta = os.listdir(ruta_json)
    cant_json = len(archivos_en_ruta)
    archivos_a_unir = [f"{ruta_json}/{i}.json" for i in range(1,cant_json+1)] 
    print(cant_json)
    #Uno todos los json de ese estado.
    dfre = spark.read.json(archivos_a_unir[0])
    for archivo in archivos_a_unir[1:]: 
        print(archivo)
        df_archivo = spark.read.json(archivo)
        dfre = dfre.unionByName(df_archivo)
    # Elimino los duplicados
    dfre = dfre.drop_duplicates()
    #Guardo los gmapid de ese estado en un archivo txt que usaré más adelante.
    gmap_id_list = dfre.select("gmap_id").distinct().rdd.flatMap(lambda x: x).collect()
    gmap_id_set = set(gmap_id_list)
    with open(f"states_files_ongoing/gmap_ids_{state}.txt", "w") as file:
        for gmap_id in gmap_id_set:
            file.write(gmap_id + "\n")
    #Filtro las columnas que serán usadas:
    selected_columns = ["gmap_id", "rating", "text", "time"]
    dfre = dfre.select(selected_columns)
    dfre = dfre.withColumn("rating", col("rating").cast("float"))
    #Guardo como parquet las reviews del estado.
    dfre.write.format("parquet").save(f"states_files_ongoing/gmaps_reviews_{state}")
    #Para salvar memoria creo otra sesión.
    spark.stop()

    # PARTE 2: Cargar la metadata de todas las empresas para el estado seleccionado. 
    spark = etl.create_spark_session()
    #Leo la información.
    ruta_json = "raw_data/Google_Maps/metadata-sitios"
    archivos_en_ruta = os.listdir(ruta_json)
    cant_json = len(archivos_en_ruta)
    archivos_a_unir = [f"{ruta_json}/{i}.json" for i in range(1,cant_json+1)] 
    print(cant_json)
    #Uno todos los json de metadata.
    schema = etl.get_schema_meta()
    archivos_a_unir[0]
    dfmd = spark.read.json(archivos_a_unir[0], schema=schema)
    for archivo in archivos_a_unir[1:]:
        df_archivo = spark.read.json(archivo, schema=schema)
        dfmd = dfmd.unionByName(df_archivo)
    # Elimino los duplicados
    dfmd = dfmd.drop_duplicates()
    #Leo la información con los id del estado
    with open(f"states_files_ongoing/gmap_ids_{state}.txt", "r") as file:
        gmap_ids = file.read().splitlines()
    # Filtrar las filas que contienen gmap_ids presentes en el archivo
    dfmd_state = dfmd.filter(dfmd['gmap_id'].isin(gmap_ids))
    #Filtro las columnas que serán usadas:
    selected_columns = ["gmap_id", "name", "address", "category", "latitude", "longitude", "avg_rating", "hours"]
    dfmd_state = dfmd_state.select(selected_columns)
    #Guardo como parquet la metadata del estado.
    dfmd_state.write.format("parquet").save(f"states_files_ongoing/gmaps_metadata_{state}")
    #Para salvar memoria creo otra sesión.
    spark.stop()

    # PARTE 3: Unir reseñas con empresas 
    spark = etl.create_spark_session()
    ruta_directorio_parquet = f"states_files_ongoing/gmaps_metadata_{state}"
    df = spark.read.parquet(ruta_directorio_parquet)
    ruta_directorio_parquet = f"states_files_ongoing/gmaps_reviews_{state}"
    df2 = spark.read.parquet(ruta_directorio_parquet)

    #AGRUPAR RESEÑAS:🍀🤞🏽
    # Reemplazar comas (',') por punto y coma (';') en la columna 'text'
    df2 = df2.withColumn("text", regexp_replace("text", ",", ";"))
    df2 = df2.withColumn("date", etl.convert_timestamp_to_date(col("time")))

    grouped_df = df2.groupBy("gmap_id") \
        .agg(
            to_json(collect_list(struct("text", "rating", "date"))).alias("reviews")
        )

    # JOIN RESEÑAS CON BUSINESS:🍀🤞🏽
    # Unir los DataFrames utilizando la columna "gmap_id" como clave
    merged_df = df.join(grouped_df, on="gmap_id", how="left_outer")
    merged_df_single_partition = merged_df.coalesce(1)
    #GUARDAR COMO PARQUET O CARGAR A LA DATABASE LUEGO.
    merged_df_single_partition.write.format("parquet").save(f"states_files_ongoing/gmaps_dataset_{state}")