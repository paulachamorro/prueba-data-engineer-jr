from pyspark.sql import functions as F

def procesamiento(spark, config):

    #Lectura archivo global_mobility_data_entrega_productos
    df = (spark.read
          .option('header', True)
          .option('inferSchema', True)
          .csv(config.paths.input))
    
    print('Arcivo leído')
    
    #---------Validación de datos y calidad------------
    df.summary().show()
    df.select('tipo_entrega').distinct().show()
    df.select('unidad').distinct().show()

    #Se descartan los registros que no cuentan con información en 'material' y 'pais'
    df = df.filter(F.col('material').isNotNull())
    print('Vacíos eliminados en material')

    df = df.filter(F.col('pais').isNotNull())
    print('Vacíos eliminados en pais')

    #Valores de 'tipo_entrega' que entran en el análisis
    tipo_entrega_validos = ['ZPRE', 'ZVE1', 'Z04', 'Z05']
    df = df.filter(F.col('tipo_entrega').isin(tipo_entrega_validos))
    print('Base filtrada por tipos de entrega de análisis')

    #Descartar valores negativos en 'precio' y 'cantidad'
    df = df.filter(F.col('precio') > 0) 
    print('Valores negativos en precio descartados')

    df = df.filter(F.col('cantidad') > 0)
    print('Valores negativos en cantidad descartados')

    #-----------------Aplicación de filtros requeridos------------
    #Filtro de fechas
    df = df.filter(
        (F.col('fecha_proceso') >= config.start_date) &
        (F.col('fecha_proceso') <= config.end_date))
    
    df.show(5)

    #Filtro de país
    df = df.filter(F.col('pais') == config.country)
    df.show(5)

    return df