import os
from pokemon_pipeline.utils import init_spark_session
from delta import DeltaTable 
from datetime import datetime
from pyspark.sql import SparkSession, functions as F


def stream_json_to_delta(spark: SparkSession, source, source_schema, destination_sink: str, destination_checkpoint: str):

    df_out = spark.readStream.schema(source_schema).json(source)

    stream_out = (df_out
                  .writeStream
                  .queryName("ChanceTheRapper")
                  .trigger(once=True)
                  .format("delta")
                  .option("checkpointLocation", destination_checkpoint)
                  .outputMode("append")
                  .start(destination_sink)
                  )
    stream_out.awaitTermination()
    stream_metrics = stream_out.lastProgress
    return stream_metrics


def merge_json_to_delta(spark: SparkSession, source01: str, destination: str):

    nested_cols = [
            "abilities",
            "forms",
            "game_indices",
            "held_items",
            "moves",
            "past_types",
            "species",
            "sprites",
            "stats",
            "types"
            ]

    df_in = spark.read.format("json").load(source01)

    # Ignore primary key ID and the commit_dt since it will always differ.
    hash_key_cols = [x for x in df_in.columns if x not in ["id", "commit_dt"]]
    # Create tmp_cols for json strucs
    hash_key_cols_w_tmp = [f"tmp_{x}" if x in nested_cols else x for x in hash_key_cols]
    tmp_cols = [x for x in hash_key_cols_w_tmp if x.startswith("tmp_")]
    print(hash_key_cols_w_tmp)
    print(tmp_cols)
    # Add commit_dt
    now = datetime.now()

    # Add hashing mechanism for simple row-change-comparison
    for c in nested_cols:
        print(f" NOW PROCESSING {c}")
        df_in = df_in.withColumn(f"tmp_{c}", F.to_json(F.col(c)))

    print(f"hashing on columns: {hash_key_cols}")
    df_in_w_hash = (df_in
            .withColumn("hash", F.sha2(F.concat_ws("|", *hash_key_cols_w_tmp), 256))
            .withColumn("commit_dt", F.lit(now))
    )

    for c in tmp_cols:
        df_in_w_hash = df_in_w_hash.drop(c)

    if not DeltaTable.isDeltaTable(spark, destination):
        (df_in_w_hash
            .write
            .option("delta.enableChangeDataFeed", "true")
            .format("delta")
            .save(destination)
         )
    else:
        dt_dest = DeltaTable.forPath(spark, destination)
        (
            dt_dest.alias("destination")
            .merge(df_in_w_hash.alias("updates"),
                   condition="destination.id = updates.id")
            .whenMatchedUpdateAll(condition="destination.hash != updates.hash")
            .whenNotMatchedInsertAll()
            .execute()
        )


def get_latest_json():
    dataset_path = "data/hook_pokemon_to_json/"

    json_files = os.listdir(dataset_path)
    json_files.sort(reverse=True)
    return f"{dataset_path}/{json_files[0]}"


def main(spark: SparkSession, source: str, destination: str):
    merge_json_to_delta(spark, source, destination)




if __name__=="__main__":

    source = get_latest_json() 
    spark = init_spark_session()
    # Enable schema evolution for merge strategy
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    main(spark=spark, source=source, destination="data/json_to_delta/")
