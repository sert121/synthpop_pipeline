import pandas as pd
import numpy as np
from population_density_sampler import PopulationDensitySampler
import geopandas as gpd

class HLatHlongAgeAddition:
    def __init__(
        self,
        admin_units_geojson_filename,
        admin_units_population_filename,
        population_density_filename,
    ):
        self.admin_units = gpd.read_file(admin_units_geojson_filename)
        self.admin_units.sort_values(by="WARD", inplace=True)
        self.admin_units.reset_index(drop=True, inplace=True)
        self.population_density_sampler = PopulationDensitySampler(
            population_density_filename
        )

        self.admin_unit_wise_population = pd.read_csv(admin_units_population_filename)

        self.admin_unit_wise_population["lower_limit"] = (
            self.admin_unit_wise_population["TOT_P"].cumsum()
            - self.admin_unit_wise_population["TOT_P"]
        )
        self.admin_unit_wise_population[
            "upper_limit"
        ] = self.admin_unit_wise_population["TOT_P"].cumsum()

        for admin_unit in self.admin_units.iterrows():
            admin_unit_centroid = admin_unit[1]["geometry"].centroid
            self.population_density_sampler.add_point(
                admin_unit_centroid.y, admin_unit_centroid.x
            )

        self.total_population = int(
            np.ceil(self.admin_unit_wise_population["TOT_P"].sum() / 10000) * 10000
        )

    def perform_transforms(self, synthetic_population, synthetic_households):
        synthetic_households["hhsize"] = synthetic_population.groupby(
            "household_id"
        ).size()

        for (
            admin_unit_wise_population_info
        ) in self.admin_unit_wise_population.iterrows():
            subset_index = (
                synthetic_households["hhsize"].cumsum()
                >= admin_unit_wise_population_info[1]["lower_limit"]
            ) & (
                synthetic_households["hhsize"].cumsum()
                <= admin_unit_wise_population_info[1]["upper_limit"]
            )
            synthetic_households.loc[
                subset_index, "AdminUnitName"
            ] = admin_unit_wise_population_info[1]["Name"].astype(object)
            synthetic_households.loc[
                subset_index, "AdminUnitLatitude"
            ] = admin_unit_wise_population_info[1]["Latitude"]
            synthetic_households.loc[
                subset_index, "AdminUnitLongitude"
            ] = admin_unit_wise_population_info[1]["Longitude"]

        synthetic_households.dropna(inplace=True)

        synthetic_households[["H_Lat", "H_Lon"]] = None

        for admin_unit_name in synthetic_households["AdminUnitName"].unique():
            print(admin_unit_name)
            admin_unit_polygon = self.admin_units[
                self.admin_units["WARD"] == admin_unit_name
            ]["geometry"].iloc[0]
            admin_unit_houses_index = (
                synthetic_households["AdminUnitName"] == admin_unit_name
            )
            n_houses_in_admin_unit = len(synthetic_households[admin_unit_houses_index])
            points = self.population_density_sampler.get_lat_long_samples(
                n_houses_in_admin_unit, admin_unit_polygon
            )
            synthetic_households.loc[
                admin_unit_houses_index, ["H_Lon", "H_Lat"]
            ] = points

        synthetic_households.index.name = "hh_index"

        columns_to_join = [
            "household_id",
            "H_Lat",
            "H_Lon",
            "AdminUnitName",
            "AdminUnitLatitude",
            "AdminUnitLongitude",
        ]
        return pd.merge(
            synthetic_population,
            synthetic_households[columns_to_join],
            on="household_id",
        )
