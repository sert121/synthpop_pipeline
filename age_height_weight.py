import random
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from typing import Tuple
###################### reading data ######################################

def load_data(synthetic_pop:str,ihds:str) -> Tuple[DataFrame,DataFrame]:
    '''
    synthetic_population = pd.read_csv("/content/drive/MyDrive/synthpop/kolkata_synthpop.csv")
    ihds_df = pd.read_stata("/content/drive/MyDrive/synthpop/36151-0001-Data.dt a")
    '''
    synthetic_population = pd.read_csv(synthetic_pop)
    ihds_df = pd.read_stata(ihds)

    return synthetic_population, ihds_df

def modify_age_gender(synthetic_population:DataFrame) -> DataFrame: 
    ###################### converting age and gender columns ######################################
    synthetic_population["age"] = synthetic_population["age"].apply(
        lambda x: random.randint(80, 95) if (x == "80p") else int(x.split("to")[0])
    ) + np.random.randint(0, 5, size=len(synthetic_population))

    synthetic_population["gender"] = synthetic_population["gender"].astype(str)
    return synthetic_population

def renaming_columns(ihds_df:DataFrame) -> DataFrame:
    ################################# selecting appropriate columns from IHDS data ##########################
    columns_to_keep = [
        "RO3",
        "RO5",
        "HHID",
        "ID11",
        "AP5",
        "AP8",
        "STATEID",
        "DISTRICT",
    ]
    columns_rename_dict = {
        "RO3": "Sex",
        "RO5": "Age",
        "HHID": "HHID",
        "ID11": "Religion",
        "AP5": "Height",
        "AP8": "Weight",
        "STATEID": "State",
        "DISTID": "District",
    }
    ihds_df = ihds_df[columns_to_keep].rename(columns=columns_rename_dict)
    return ihds_df


################################# selecting appropriate columns from IHDS data ##########################
################################ create boundary_df according to district,sex ##########################
def boundary_df(ihds_df:DataFrame, district:str):
    boundary_ihds_df = ihds_df[ihds_df["DISTRICT"] == district]
    boundary_ihds_df = (
        boundary_ihds_df[["Age", "Height", "Weight", "Sex"]]
        .dropna()
        .reset_index(drop=True)
    )
    # boundary_ihds_df
    boundary_ihds_age_male_subset = boundary_ihds_df[
        boundary_ihds_df["Sex"] == "Male 1"
    ]
    boundary_ihds_age_female_subset = boundary_ihds_df[
        boundary_ihds_df["Sex"] == "Female 2"
    ]

    return boundary_ihds_age_male_subset, boundary_ihds_age_female_subset


################################ create boundary_df according to district ##########################
############################################# polyfit ####################################
# args: sex: "female","male", parameter: "Weight","Height"
def assign_height_weight(df:DataFrame, sex:str, parameter:str):
    poly = np.polyfit(
        pow(df["Age"], 1 / 4),
        df[parameter],
        deg=8,
    )
    index = synthetic_population["gender"] == sex
    poly_result = np.polyval(
        poly,
        pow(synthetic_population[index]["age"].apply(lambda x: min(x, 75)), 1 / 4),
    )
    poly_result = np.round(
        poly_result + np.random.normal(0, 0.5, size=poly_result.shape), 2
    )

    synthetic_population.loc[index, parameter] = poly_result


def age_height_weight(path_to_synthetic_pop:str,path_to_ihds:str):

    # path_to_synthetic_pop = "synthpop/kolkata_synthpop.csv"
    # path_to_ihds = "synthpop/36151-0001-Data.dta"

    synthetic_population, ihds_df = load_data(path_to_synthetic_pop, path_to_ihds)
    synthetic_population = modify_age_gender(synthetic_population)
    ihds_df = renaming_columns(ihds_df)

########################################## polyfit ####################################
    boundary_ihds_age_male_subset, boundary_ihds_age_female_subset = boundary_df(ihds_df)
    assign_height_weight(boundary_ihds_age_male_subset, "male", "Weight")
    assign_height_weight(boundary_ihds_age_male_subset, "male", "Height")
    assign_height_weight(boundary_ihds_age_male_subset, "male", "Height")
    assign_height_weight(boundary_ihds_age_male_subset, "male", "Height")

    # save as csv
    # synthetic_population.to_csv(" ")
    
    return synthetic_population
