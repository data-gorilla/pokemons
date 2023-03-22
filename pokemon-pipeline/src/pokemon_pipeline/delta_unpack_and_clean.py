from pyspark.sql import SparkSession, DataFrame, Row, functions as F
from tempfile import TemporaryDirectory
import json
import copy
from pokemon_pipeline.json_to_delta import main, stream_json_to_delta
import os
from pokemon_pipeline.utils import init_spark_session


def main(spark, source01: str, destination: str):
    
    # providing a starting version
    
    df = spark.read.format("delta").load(source01)

    df_out = df.select("name", 
                       "id", 
                       "base_experience", 
                       "weight", 
                       "height",
                       F.col("sprites.front_default").alias("sprite"),
                       F.explode_outer("types").alias("types"),
                       "game_indices"
              )
    
    df_out_1 = (df_out.withColumn("game_indice", F.explode_outer("game_indices"))) # Applied foot-gunning intensely here.
    
    df_out_2 = (df_out_1.select("name", 
                             "id", 
                             "base_experience", 
                             "weight", 
                             "height",
                             "game_indices",
                             "sprite",
                             F.col("types.slot").alias("slot_prio"),
                             F.col("types.type.name").alias("slot_name"),
                             F.col("game_indice.version.name").alias("game_name")
                            )
                .withColumn("type_slots", F.struct("slot_prio", "slot_name"))
    )

    
    df_out_3 = (df_out_2
                .groupBy("name", "id", "base_experience", "weight", "height", "sprite")
                .agg(F.map_from_entries(F.collect_set("type_slots")).alias("type_slots"), 
                     F.collect_set("game_name").alias("game_names"))
                .withColumn("height", F.col("height") / 10) # convert from decimetres to centimeters.
                .withColumn("weight", F.col("weight") / 10) # convert from hectakilo to kilo.
                .withColumn("bmi", F.round(F.col("weight") / F.col("height")**2, 2))
                .withColumn("name", F.initcap("name"))
                )
    
    df_out_3.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(destination)

    metrics = {}
    return metrics


if __name__=="__main__":

    spark = init_spark_session()
    source = "data/json_to_delta/"
    destination = "data/delta_unpack_and_clean_delta/"

    main(spark, source01=source, destination=destination)
