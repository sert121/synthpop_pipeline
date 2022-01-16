# !pip install geopandas
from HlatHlongAddition import HLatHlongAgeAddition
import pandas as pd
import numpy as np
from tqdm import tqdm
tqdm.pandas()
from IPU import IPU
from age_height_weight import age_height_weight
# !git clone https://github.com/UDST/synthpop.git
# !git clone https://github.com/gopiprasanthpotipireddy/A-perspective-on-indian-society



#Set source for marginals and samples from IHDS survey
householdh_marginal_filename = "/content/drive/MyDrive/synthpop/kolkata_household - kolkata_household.csv"
individuals_marginal_filename = "/content/drive/MyDrive/synthpop/kolkata_individual - kolkata_individual.csv"

ihds_individuals_filename = "/content/drive/MyDrive/synthpop/36151-0001-Data.tsv"
ihds_household_filename = "/content/A-perspective-on-indian-society/IHDS Analysys/data/DS0002/36151-0002-Data.tsv"

#The geojson must have ward names as well. This should match with the population file defined below
admin_units_geojson_filename = "https://raw.githubusercontent.com/datameet/Municipal_Spatial_Data/master/Kolkata/kolkata.geojson"

#From https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/ind_pd_2020_1km_ASCII_XYZ.zip unzip this
population_density_filename = "/content/drive/MyDrive/synthpop/ind_pd_2020_1km_ASCII_XYZ.csv"

# ward wise population
admin_units_population_filename="/content/drive/MyDrive/synthpop/geopop - geopop (1).csv"
#admin_units_population['TOT_P'] = admin_units_population['TOT_P'].apply(int)
 
# select state number and name appropriately
state_id = 19

#Select distid within state
district_ids = [17]

output_file_name = 'output.csv'

b=pd.read_csv("/content/drive/MyDrive/synthpop/geopop - geopop (1).csv")
b['Name']=b['Name'].astype(object)

ihds_individuals_data = pd.read_csv(ihds_individuals_filename, sep='\t')

filtered_ihds_individuals_data = ihds_individuals_data.loc[ihds_individuals_data.STATEID==state_id]

filtered_ihds_individuals_data = filtered_ihds_individuals_data[filtered_ihds_individuals_data['DISTID'].apply(lambda x : x in district_ids)]

ihds_households_data = pd.read_csv(ihds_household_filename, sep='\t')

filtered_ihds_households_data = ihds_households_data.loc[ihds_households_data.STATEID==state_id] 

filtered_ihds_households_data = filtered_ihds_households_data[filtered_ihds_households_data['DISTID'].apply(lambda x : x in district_ids)]
###Reading and filtering survey sample data

###IPU Step
ipu_object = IPU()

#passing all requirements to the IPU function
synthetic_households, synthetic_individuals, synthetic_stats = ipu_object.generate_data(filtered_ihds_individuals_data, filtered_ihds_households_data, householdh_marginal_filename, individuals_marginal_filename)
###IPU Step

synthetic_households.head()

###Adding hlat hlong and age
hlat_hlong_age_object = HLatHlongAgeAddition(admin_units_geojson_filename, admin_units_population_filename, population_density_filename)

# for i in hlat_hlong_age_object.admin_unit_wise_population.iterrows():
#   print(i[0],"\n------\n",i[1]['Latitude '])
hlat_hlong_age_object.admin_units['WARD']=hlat_hlong_age_object.admin_units['WARD'].astype(int)

# print(hlat_hlong_age_object.admin_unit_wise_population.dtypes)
# print(hlat_hlong_age_object.admin_units.dtypes)
# print(hlat_hlong_age_object.admin_units["geometry"])
# print(hlat_hlong_age_object.population_density_sampler.population_density_data)

#passing base population and getting the joined population with newly added columns [AdminUnitName, AdminUnitLat, AdminUnitLong, household_id, h_lat, h_long]
#also age is now in numbers rather than in bins
new_synthetic_population = hlat_hlong_age_object.perform_transforms(synthetic_individuals, synthetic_households)

# len(synthetic_households['AdminUnitName'].unique())

#saving file

#loading dta IHDS data
path_to_ihds_data_DTA = "path_to_IHDS_data[DTA format]"

# adding height weight and age
new_synthetic_population = age_height_weight(output_file_name,path_to_ihds_data_DTA)

# new_synthetic_population.to_csv("/content/drive/MyDrive/synthpop/kolkata_synthpop.csv", index=False)
new_synthetic_population.to_csv(output_file_name, index=False)


# adding places 


# res=pd.read_csv("/content/drive/MyDrive/synthpop/kolkata_synthpop.csv")
# res.head()
