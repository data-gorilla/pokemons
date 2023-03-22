from pyspark.sql import Row, functions as F
from pokemon_pipeline.delta_unpack_and_clean_stream import main
from pokemon_pipeline.json_to_delta import main as main_json_to_delta
from tempfile import TemporaryDirectory
import chispa
import pytest
import shutil
import os
import json


def test_delta_unpack_and_clean_stream(spark):
    
    with TemporaryDirectory() as tmpdir:
        json_dest = f"{tmpdir}/source/source01.json"
        json_source = "tests/data/bulbasaur_pretty.json"
        json_to_delta = f"{tmpdir}/json_to_delta"
        delta_unpack_and_clean_stream_path = f"{tmpdir}/delta_unpack_and_clean_stream"

        # 1.a Copy over json file.
        os.mkdir(f"{tmpdir}/source")
        os.mkdir(f"{tmpdir}/json_to_delta")
        
        shutil.copy(json_source, json_dest)
        
        # 1.b run json_to_delta once.
        main_json_to_delta(spark, source=json_dest, destination=json_to_delta)
        # 1.c run delta_unpack_and_clean_stream once.
        main(spark=spark, source01=json_to_delta, destination=delta_unpack_and_clean_stream_path) 
        tmp_out_1 = spark.read.format("delta").load(delta_unpack_and_clean_stream_path)
        print(tmp_out_1)

        # 2.a Copy over json file 2 (only change name = BULBASAUR).
        json_dest2 = f"{tmpdir}/source/source02.json"
        with open(json_source, "r") as source_file:
            json_file = json.load(source_file)
            json_file["name"] = "SIR.BULBA"
        with open(json_dest2, "w") as dest_path2:
            dest_path2.write(json.dumps(json_file))
        # 2.b Run json_to_delta second time.
        main_json_to_delta(spark, source=json_dest2, destination=json_to_delta)
        
        # 2.c run delta_unpack_and_clean_stream once.
        main(spark=spark, source01=json_to_delta, destination=delta_unpack_and_clean_stream_path) 
        #print("output of table after second run")
        tmp_out_2 = spark.read.format("delta").load(delta_unpack_and_clean_stream_path)
        print(tmp_out_2.show())
        # TODO: Create proper chispa 
        #chispa.assert_df_equality(df_out.drop("game_names"), df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
