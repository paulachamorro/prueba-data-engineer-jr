from pyspark.sql import functions as F

def procesamiento(spark, config):

    #Lectura archivo global_mobility_data_entrega_productos
    df = (spark.read
          .option('header', True)
          .option('inferSchema', True)
          .csv(config.paths.input))
    
    print('Arcivo leído')
    
    #df de captura de errores
    df_val = df.withColumn("tipo_error", F.lit(""))
    def add_error(col, condition, error_msg):
        return F.when(condition, 
                  F.concat_ws(", ",
                              F.array_remove(F.array(col, F.lit(error_msg)), "")
                 )).otherwise(col)

    #---------Validación de datos y calidad------------
    df.summary().show()
    df.select('tipo_entrega').distinct().show()
    df.select('unidad').distinct().show()

    #1. Se descartan los registros que no cuentan con información en 'material' y 'pais'
    df_val = df_val.withColumn(
    "tipo_error",
    add_error(F.col("tipo_error"), F.col("material").isNull(), "material_null"))

    df_val = df_val.withColumn(
    "tipo_error",
    add_error(F.col("tipo_error"), F.col("pais").isNull(), "pais_null"))

    df = df.filter(F.col('material').isNotNull())
    print('Vacíos eliminados en material')

    df = df.filter(F.col('pais').isNotNull())
    print('Vacíos eliminados en pais')

    #2. Valores de 'tipo_entrega' que entran en el análisis
    tipo_entrega_validos = (
        list(config.filters.valid_tipo_entrega.rutina)
        + list(config.filters.valid_tipo_entrega.con_bonificacion))
    
    df_val = df_val.withColumn(
    "tipo_error",
    add_error(F.col("tipo_error"),
              ~F.col("tipo_entrega").isin(tipo_entrega_validos),
              "tipo_entrega_invalido"))
    
    df = df.filter(F.col('tipo_entrega').isin(tipo_entrega_validos))
    print('Base filtrada por tipos de entrega de análisis')

    #3. Descartar valores negativos en 'precio' y 'cantidad'
    df_val = df_val.withColumn(
    "tipo_error",
    add_error(F.col("tipo_error"),
              (F.col("precio") <= 0) | (F.col("cantidad") <= 0),
              "valores_negativos"))

    df = df.filter(F.col('precio') > 0) 
    print('Valores negativos en precio descartados')

    df = df.filter(F.col('cantidad') > 0)
    print('Valores negativos en cantidad descartados')

    df_errors = df_val.filter(F.col("tipo_error") != "")

    #-----------------Aplicación de filtros requeridos------------
    #Filtro de fechas
    df = df.filter(
        (F.col('fecha_proceso') >= config.start_date) &
        (F.col('fecha_proceso') <= config.end_date))
    
    print('Verficación filtro de fechas:')
    df.show(5)

    #Filtro de país
    df = df.filter(F.col('pais') == config.country)
    print('Verficación filtro país:')
    df.show(5)

    #-----Escritura registros que no entraron en análisis por calidad de datos-------
    fechas_r = [row['fecha_proceso'] for row in df_errors.select("fecha_proceso").distinct().collect()]

    for fecha in fechas_r:
        rej_path = f"{config.paths.errors}/{fecha}"
        df_errors.filter(F.col("fecha_proceso") == fecha) \
             .write.mode("overwrite") \
             .parquet(rej_path)
    
    print(f'Registros rechazados para análisis: ', df_errors.count())
    df_errors.show(5)

    #-------Estandarizar unidades---------
    var_cs = config.units.CS
    var_st = config.units.ST

    df = df.withColumn('cantidad_estandarizada',
                       F.when(F.col('unidad') == 'CS', F.col('cantidad') * F.lit(var_cs))
                       .when(F.col('unidad') == 'ST', F.col('cantidad') * F.lit(var_st))
                       .otherwise(F.lit(None)))
    
    print('Estandarización de unidades exitosa')
    df.show(5)
    

    #---------Columnas tipo de entrega-------------
    for tipo in tipo_entrega_validos:
        df = df.withColumn(
            f'is_{tipo.lower()}',
            F.when(F.col('tipo_entrega') == tipo, F.lit(1)).otherwise(F.lit(0)))
        
    #Identificación rutina/bonificación
    rutina = list(config.filters.valid_tipo_entrega.rutina)
    con_bonificacion = list(config.filters.valid_tipo_entrega.con_bonificacion)

    df = df.withColumn(
        'is_rutina',
        F.when(F.col('tipo_entrega').isin(rutina), 1).otherwise(0))
    
    
    df = df.withColumn(
        'is_bonificacion',
        F.when(F.col('tipo_entrega').isin(con_bonificacion), 1).otherwise(0))
    
    print('Creación de columnas exitosa')
    df.show(5)
    
    #--------Escritura dataset final procesado-------

    fechas_p = [row['fecha_proceso'] for row in df.select("fecha_proceso").distinct().collect()]

    for fecha in fechas_p:
        out_path = f"{config.paths.output}/{fecha}"
        (
            df.filter(F.col("fecha_proceso") == fecha)
            .write.mode("overwrite")
            .parquet(out_path))
    
    print('Dataset escrito exitosamente en ', config.paths.output)

    return df