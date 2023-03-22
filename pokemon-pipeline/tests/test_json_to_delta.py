import pytest
from pyspark.sql import SparkSession, DataFrame, Row
from tempfile import TemporaryDirectory
import json
import copy
from pokemon_pipeline.json_to_delta import main, stream_json_to_delta
import os
import chispa


def test_stream_json_dumps(spark):

    """Test that first add bulbasaur/squirtle-json file and then Bulbasaur-json
        This is done to be able to account for updates and deletes.
    """
    with open("tests/data/unioned.json", "r") as in_file:
        stp1_in_json = json.load(in_file)
        stp2_in_json = copy.deepcopy(stp1_in_json[:1])
        stp2_in_json[0]["name"] = "Bulbasaur"

        assert stp1_in_json[0]["name"] == "bulbasaur"
        assert stp1_in_json[1]["name"] == "squirtle"
        assert stp2_in_json[0]["name"] == "Bulbasaur"

        with TemporaryDirectory() as tmpdir:

            in_path = f"{tmpdir}/pokemons/"
            os.mkdir(f"{tmpdir}/pokemons")
            print(f"using tmpdir = {tmpdir}")
            in_file1_path = f"{in_path}in1.json"
            in_file2_path = f"{in_path}in2.json"

            # first run  
            with open(in_file1_path, "w") as in1_file:
                in1_file.write(json.dumps(stp1_in_json))

                in_schema = spark.read.format("json").load(f"{in_path}*.json").schema

                print("Now going through 0'th iteration")
                metrics = stream_json_to_delta(spark,
                                               source=f"{tmpdir}/pokemons",
                                               source_schema=in_schema,
                                               destination_sink=f"{tmpdir}/data",
                                               destination_checkpoint=f"{tmpdir}/checkpoint"
                                               )


            # second_run
            with open(in_file2_path, "w") as in2_file:
                in2_file.write(json.dumps(stp2_in_json))

                print("Now going through 1'st iteration")
                metrics = stream_json_to_delta(spark,
                                               source=f"{tmpdir}/pokemons",
                                               source_schema=in_schema,
                                               destination_sink=f"{tmpdir}/data",
                                               destination_checkpoint=f"{tmpdir}/checkpoint"
                                               )


@pytest.mark.skipif(os.environ.get("CREATE_SAMPLE_DATA") is None, reason="only use if you wish to create sample data in the data folder.")
def test_sample_json_to_delta_stream(spark):

        in_schema = spark.read.format("json").load(f"data/pokemon_to_json/*.json").schema

        print("Now going through 0'th iteration")
        metrics = stream_json_to_delta(spark,
                  source=f"data/pokemon_to_json/*.json",
                  source_schema=in_schema,
                  destination_sink=f"data/json_to_delta/data",
                  destination_checkpoint=f"data/json_to_delta/checkpoint"
        )


def test_main(spark):

    with open("tests/data/unioned.json", "r") as in_file:
        stp1_in_json = json.load(in_file)
        stp2_in_json = copy.deepcopy(stp1_in_json[:1])
        stp2_in_json[0]["name"] = "Bulbasaur"

        assert stp1_in_json[0]["name"] == "bulbasaur"
        assert stp1_in_json[1]["name"] == "squirtle"
        assert stp2_in_json[0]["name"] == "Bulbasaur"

        with TemporaryDirectory() as tmpdir:

            in_path = f"{tmpdir}/pokemons/"
            os.mkdir(f"{tmpdir}/pokemons")
            print(f"using tmpdir = {tmpdir}")
            in_file1_path = f"{in_path}in1.json"
            in_file2_path = f"{in_path}in2.json"
            out_path = f"{tmpdir}/out"

            with open(in_file1_path, "w") as in1_file:
                in1_file.write(json.dumps(stp1_in_json))
            with open(in_file2_path, "w") as in2_file:
                in2_file.write(json.dumps(stp2_in_json))

            # First
            main(spark, in_file1_path, out_path)

            # Second
            main(spark, in_file2_path, out_path)

            df_out = spark.read.format("delta").load(out_path)

            df_expect = spark.createDataFrame([Row(name="Bulbasaur"), Row(name="squirtle")])

            chispa.assert_df_equality(df_out.select("name"),
                                      df_expect,
                                      ignore_nullable=True,
                                      ignore_column_order=True,
                                      ignore_row_order=True)

            # TODO: Also write test-case for changeDataFeed info.
