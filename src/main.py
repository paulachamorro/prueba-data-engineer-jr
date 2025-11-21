from omegaconf import OmegaConf
from pyspark.sql import SparkSession
from transformations import procesamiento

def iniciar_spark():
    spark = (
        SparkSession.builder
        .master("local[*]")       
        .appName("pipeline_prueba")
        .getOrCreate()
    )
    return spark

def main():
    config = OmegaConf.load('config/config.yaml')
    spark = iniciar_spark()
    procesamiento(spark, config)

    print('Ejecución exitosa')

if __name__ == '__main__':
    main()
