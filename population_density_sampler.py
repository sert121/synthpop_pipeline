import pandas as pd
import numpy as np
from shapely.geometry import Point

class PopulationDensitySampler:
    def __init__(self, population_density_filename):
        self.population_density_data = pd.read_csv(population_density_filename)
        columns_rename = {"X": "longitude", "Y": "latitude", "Z": "population_density"}
        self.population_density_data["X"] = self.population_density_data["X"].round(6)
        self.population_density_data["Y"] = self.population_density_data["Y"].round(6)
        self.population_density_data.rename(columns_rename, axis=1, inplace=True)
        self.population_density_data[
            "point_object"
        ] = self.population_density_data.progress_apply(
            lambda x: Point(x["longitude"], x["latitude"]), axis=1
        )

    def add_point(self, latitude, longitude):
        distances = pow(self.population_density_data["latitude"] - latitude, 2) + pow(
            self.population_density_data["longitude"] - longitude, 2
        )
        sorted_df = self.population_density_data.loc[distances.sort_values().index]
        mean_population_density = sorted_df.iloc[:4]["population_density"].mean()

        new_row_index = len(self.population_density_data)

        self.population_density_data.at[new_row_index, "longitude"] = longitude
        self.population_density_data.at[new_row_index, "latitude"] = latitude
        self.population_density_data.at[
            new_row_index, "population_density"
        ] = mean_population_density
        self.population_density_data.at[new_row_index, "point_object"] = Point(
            longitude, latitude
        )

    def get_lat_long_samples(self, n, polygon):
        subset = self.population_density_data[
            self.population_density_data["point_object"].progress_apply(
                polygon.contains
            )
        ]

        if len(subset) == 0:
            raise Exception("No points within the given polygon")

        sample = subset.sample(
            weights="population_density", n=(n * 10), replace=True
        ).copy()

        sample.reset_index(drop=True, inplace=True)

        sample["latitude"] = sample["latitude"] + np.random.uniform(
            -0.015, 0.015, size=sample.shape[0]
        )

        sample["longitude"] = sample["longitude"] + np.random.uniform(
            -0.015, 0.015, size=sample.shape[0]
        )

        points = sample.progress_apply(
            lambda x: Point(x["longitude"], x["latitude"]), axis=1
        )

        contained = points.progress_apply(polygon.contains)

        return (
            sample[contained][["longitude", "latitude"]].sample(n, replace=True).values
        )
