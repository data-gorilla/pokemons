from os import wait
import streamlit as st 
from deltalake import DeltaTable
import pandas as pd
from typing import List, Union, Optional
import itertools


st.title("Pokemon Dashboard!")
st.text("A place all your poke-questions!")


@st.cache_data
def load_data():
    dt = DeltaTable("../pokemon-pipeline/data/delta_unpack_and_clean_delta")
    df = dt.to_pandas()
    df["type_slots_ordered"] = df["type_slots"].apply(lambda x: [x[0][1], None] if (len(x) == 1) else [x[0][1], x[1][1]] if (x[0][1] == 1) else [x[1][1], x[0][1]])
    df = df.drop("type_slots", axis=1)

    return df


df = load_data()

#pd.DataFrame(df2.teams.tolist(), index= df2.index)


# ----------------------------------------------------------------------------------
# -------------------------------- Choose games of interest ------------------------
# ----------------------------------------------------------------------------------

distinct_games = list(set(itertools.chain(*df["game_names"].to_list())))

games_of_interest = st.sidebar.multiselect("Games you are interested in", distinct_games)

df_games_subset = df[df.apply(lambda row: set(games_of_interest).issubset(row["game_names"]), axis=1)]

# TODO: Add some filtering on selected games.

# ----------------------------------------------------------------------------------
# -------------------------------- Choose pokemon ----------------------------------
# ----------------------------------------------------------------------------------
# Using object notation
option = st.sidebar.selectbox(
        "Pokemon of interest:",
        df_games_subset["name"].unique()
        )
st.spinner("Finding your pokemon!")
df_poke_row = df_games_subset[df_games_subset["name"] == option]
st.table(df_poke_row)

type_slot_list = df_poke_row["type_slots_ordered"].to_list()[0]

col1, col2 = st.columns(2)
col1.metric(label="Primary Type", value=type_slot_list[0])
col2.metric(label="Secondary Type", value=type_slot_list[1])


sprite_text = df_poke_row["sprite"].tolist()[0]


if option == "Pikachu":
    st.balloons()

st.image(sprite_text, width=400)

