
from pyspark.sql import SparkSession, DataFrame, Row, functions as F
from tempfile import TemporaryDirectory
import json
import copy
from pokemon_pipeline.json_to_delta import main, stream_json_to_delta
import os
from delta import DeltaTable
from pokemon_pipeline.utils import init_spark_session

RELEVANT_UPDATE_TYPES = ["insert", "update_postimage"]


def merge_stream(microBatchOutputDF, batchId, spark, destination):
    df_relevant_rows = (microBatchOutputDF.filter(F.col("_change_type").isin(RELEVANT_UPDATE_TYPES)))
    df_out = (df_relevant_rows.select("name", 
                        "id", 
                        "base_experience", 
                        "weight", 
                        "height",
                        "_change_type",
                        "_commit_version",
                        "_commit_timestamp",
                        F.col("sprites.front_default").alias("sprite"),
                        F.explode("types").alias("types"),
                        "game_indices"))


    df_out_1 = (df_out.select("*", F.explode("game_indices").alias("game_indice")))

    df_out_2 = (df_out_1.select("name",
                                 "id", 
                                "base_experience", 
                                "weight", 
                                "height",
                                "game_indices",
                                "sprite",
                                "_change_type",
                                "_commit_version",
                                "_commit_timestamp",
                                F.col("types.slot").alias("slot_prio"),
                                F.col("types.type.name").alias("slot_name"),
                                F.col("game_indice.version.name").alias("game_name")
                                )
                                 .withColumn("type_slots", F.struct("slot_prio", "slot_name"))
                 )

    df_out_3 = (df_out_2
                .groupBy("name", "id", "base_experience", "weight", "height", "sprite", "_change_type", "_commit_version", "_commit_timestamp")
                .agg(F.map_from_entries(F.collect_set("type_slots")).alias("type_slots"),
                     F.collect_set("game_name").alias("game_names"))
                .withColumn("bmi", F.round(F.col("weight") / F.col("height")**2, 2))
                .withColumn("name", F.initcap("name"))
    )
     
    # init delta file if not already exists
    if not DeltaTable.isDeltaTable(spark, destination):
         (df_out_3
            .write
            .format("delta")
            .save(destination)
         )
    else:
        DELTA_UNPACK_AND_CLEAN_DT = DeltaTable.forPath(spark,destination) 
        (DELTA_UNPACK_AND_CLEAN_DT.alias("t").merge(
               df_out_3.alias("s"),
               "t.id = s.id")
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute()
        )


def main(spark, source01: str, destination: str):
    df_out = (spark.readStream.format("delta") 
        .option("readChangeFeed", "true") 
        .load(source01)
    )

    stream_out = (df_out
     .writeStream
     .queryName("delta_unpack_and_clean_stream")
     .trigger(once=True)
     .format("delta")
     .foreachBatch(lambda df, epochId: merge_stream(df, epochId, spark, destination))
     .outputMode("update")
     .option("checkpointLocation", f"{destination}/_checkpoints/")
     .start(destination)
    )
    stream_out.awaitTermination()
    stream_metrics = stream_out.lastProgress
    return stream_metrics


if __name__=="__main__":

    spark = init_spark_session()
    source = "data/json_to_delta/"
    destination = "data/delta_unpack_and_clean/"

    main(spark, source01=source, destination=destination)



