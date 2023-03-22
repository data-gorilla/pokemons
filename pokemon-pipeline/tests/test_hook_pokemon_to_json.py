import os
from tempfile import TemporaryDirectory
from pokemon_pipeline.hook_pokemon_to_json import (request_pokemon,
                                                   download_pokemons_in_range,
                                                   save_json_list_to_file,
                                                   main)
import json
from datetime import datetime
import pytest


def test_request_pokemon_integration():
    response_bulbasaur = request_pokemon(id=1)
    assert response_bulbasaur.status_code == 200


def test_download_pokemons_in_range():

    json_list = download_pokemons_in_range(1, 1)

    assert json_list[0]["name"] == "bulbasaur"


def test_save_json_list_to_file():

    with open("data/bulbasaur_pretty.json", "r") as f_bulb:
        with open("data/squirtel_pretty.json", "r") as f_squir:
            bulbasaur_json = json.load(f_bulb)
            squirtle_json = json.load(f_squir)

            with TemporaryDirectory() as tmpdir:
                out_file_path = f"{tmpdir}/unioned.json"

                save_json_list_to_file([bulbasaur_json, squirtle_json], destination_path=out_file_path)

                with open(out_file_path, "r") as f_out:
                    out_json = json.load(f_out)

                    assert out_json[0]["name"] == "bulbasaur"

                    assert out_json[1]["name"] == "squirtle"


def test_create_unioned_outfile_example():

    with open("tests/data/bulbasaur_pretty.json", "r") as f_bulb:
        with open("tests/data/squirtle_pretty.json", "r") as f_squir:
            bulbasaur_json = json.load(f_bulb)
            squirtle_json = json.load(f_squir)

            out_file_path = f"tests/data/unioned.json"

            save_json_list_to_file([bulbasaur_json, squirtle_json], destination_path=out_file_path)

            with open(out_file_path, "r") as f_out:
                out_json = json.load(f_out)

                assert out_json[0]["name"] == "bulbasaur"

                assert out_json[1]["name"] == "squirtle"


def test_main():

    now_str = datetime.now().timestamp()
    dataset_name = "pokemon_to_json"

    with TemporaryDirectory() as tmpdir:
        dest_path = f"{tmpdir}/{now_str}_{dataset_name}.json"

        main(destination_path=dest_path)
        assert os.path.isfile(dest_path)


        with open(dest_path, "r") as f_out:
            out_json = json.load(f_out)

            assert out_json[0]["name"] == "bulbasaur"

            assert out_json[1]["name"] == "ivysaur"


@pytest.mark.skipif(os.environ.get("CREATE_SAMPLE_DATA") is None, reason="only use if you wish to create sample data in the data folder.")
def test_main_output_example():

    now_str = datetime.now().timestamp()
    dataset_name = "pokemon_to_json"
    dest_path = f"data/{dataset_name}/{now_str}_{dataset_name}.json"

    main(destination_path=dest_path)
    assert os.path.isfile(dest_path)
