from pyspark.sql import Row
from tempfile import TemporaryDirectory
from pokemon_pipeline.delta_unpack_and_clean import main
import chispa


def test_delta_unpack_and_clean(spark):

    with TemporaryDirectory() as tmpdir:

        df_out = main(spark, source01="data/json_to_delta/data/", destination=tmpdir)

        df_expected = spark.createDataFrame([
            Row(name="Bulbasaur", id=1, base_experience=64, weight=69, height=7, bmi=1.41, type_slots=[Row(slot_prio=2, slot_name="poison"), Row(slot_prio=1, slot_name="grass")]),
            Row(name="Ivysaur", id=2, base_experience=142, weight=130, height=10, bmi=1.30, type_slots=[Row(slot_prio=2, slot_name="poison"), Row(slot_prio=1, slow_name="grass")])
        ])
        # Drop "game_names" as we are not testing that logic here.
        chispa.assert_df_equality(df_out.drop("game_names"), df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
