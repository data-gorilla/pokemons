from typing import List, Union
from pyspark.sql import SQLContext, SparkSession
import requests
import json
from datetime import datetime

URL = "https://pokeapi.co/api/v2/pokemon/"


def request_pokemon(id=int):
    r = requests.get(f"{URL}{id}")
    return r


def download_pokemons_in_range(first: int = 1, last: int = 151):
    """
    preset to max 151 from back in the days when mew was the last pokemon and
    there were only 3 star wars movies.
    """
    json_responses = []
    for i in range(first, last+1):
        # TODO: Consider adding retry mechanism.
        response = request_pokemon(id=i)
        if response.status_code == 200:
            json_responses.append(response.json())

    return json_responses


def save_json_to_delta(spark: SparkSession,
                       source_path: List,
                       destination_path: str) -> Union[bool, Exception]:
    # TODO: Do I want this as stream or batch?
    df = spark.read.format("json").load(source_path)
    df.write.format("delta").save(destination_path)

    return True


def save_json_list_to_file(json_list: List, destination_path: str):

    with open(destination_path, "w") as f_out:
        out_data = json.dumps(json_list)
        f_out.write(out_data)
        return True


def main(destination_path: str):


    json_list = download_pokemons_in_range(1, 1010)  # TODO remove limitation
    save_json_list_to_file(json_list=json_list, destination_path=destination_path)


if __name__=="__main__":

    now_str = int(datetime.now().timestamp())

    destination_path = f"data/hook_pokemon_to_json/{now_str}.json"

    main(destination_path=destination_path)
