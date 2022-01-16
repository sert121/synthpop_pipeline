from tqdm import tqdm
from shapely.ops import unary_union
from shapely.geometry import Point, MultiPoint
import geopandas as gpd
import pandas as pd
import numpy as np
import logging
from multiprocessing import Pool
from population_density_sampler import PopulationDensitySampler

# !pip3 install geopandas
# !wget https: // data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/ind_pd_2020_1km_ASCII_XYZ.zip
# !unzip ind_pd_2020_1km_ASCII_XYZ.zip


tqdm.pandas()

population_density_filename = "ind_pd_2020_1km_ASCII_XYZ.csv"
population_density_sampler = PopulationDensitySampler(population_density_filename)
admin_units = gpd.read_file(
    "https://raw.githubusercontent.com/datameet/Municipal_Spatial_Data/master/Kolkata/kolkata.geojson"
)
admin_units.sort_values(by="WARD", inplace=True)
admin_units.reset_index(drop=True, inplace=True)


combined_boundary = unary_union(admin_units["geometry"])
within_combined_boundary_indicies = population_density_sampler.population_density_data[
    "point_object"
].progress_apply(combined_boundary.contains)
population_density_data = population_density_sampler.population_density_data[
    within_combined_boundary_indicies
].reset_index(drop=True)

synthetic_population = pd.read_csv(
    "/content/drive/MyDrive/synthpop/kolkata_synthpop.csv"
)


class Places:
    def __init__(self, city_id, city_population, n_process=1):
        assert type(city_id) == int
        self.city_id = city_id
        self.city_population = city_population
        self.n_process = n_process

    def generate_workplaces(self, workplace_type_list):
        n_random_workplaces = int(
            self.city_population * np.random.normal(0.5, 0.1) / 100
        )
        random_workplace_types = np.random.choice(
            workplace_type_list, n_random_workplaces, replace=True
        )
        workplace_types = (
            list(random_workplace_types) + list(set(workplace_type_list)) + ["Teachers"]
        )
        lat_lon_pairs = population_density_sampler.get_lat_long_samples(
            len(workplace_types), combined_boundary
        )
        workplace_lats = lat_lon_pairs.T[1]
        workplace_longs = lat_lon_pairs.T[0]
        workplace_names = [
            2 * pow(10, 12) + self.city_id * pow(10, 9) + counter
            for counter in range(len(workplace_types))
        ]
        self.workplaces = pd.DataFrame(
            [workplace_names, workplace_types, workplace_lats, workplace_longs]
        ).T
        self.workplaces.columns = [
            "workplace_name",
            "workplace_type",
            "workplace_lat",
            "workplace_long",
        ]

    def generate_schools(self):
        teachers_workplaces = self.workplaces[
            self.workplaces["workplace_type"] == "Teachers"
        ]
        self.schools = pd.DataFrame(
            [
                teachers_workplaces["workplace_name"],
                teachers_workplaces["workplace_lat"],
                teachers_workplaces["workplace_long"],
            ]
        ).T.copy()
        self.schools.columns = ["school_name", "school_lat", "school_long"]
        self.schools["school_type"] = pd.Series(
            ["school" for _ in range(self.schools.shape[1])]
        )

    def generate_public_places(self):
        public_places_number = int(
            self.city_population * np.random.normal(0.5, 0.1) / 1000
        )
        lat_lon_pairs = population_density_sampler.get_lat_long_samples(
            public_places_number, combined_boundary
        )
        public_place_lats = lat_lon_pairs.T[1]
        public_place_longs = lat_lon_pairs.T[0]
        public_place_names = [
            3 * pow(10, 12) + self.city_id * pow(10, 9) + counter
            for counter in range(public_places_number)
        ]
        public_place_types = np.random.choice(
            ["park", "mall", "gym"], public_places_number, replace=True
        )
        self.public_places = pd.DataFrame(
            [
                public_place_names,
                public_place_types,
                public_place_lats,
                public_place_longs,
            ]
        ).T
        self.public_places.columns = [
            "public_place_name",
            "public_place_type",
            "public_place_lat",
            "public_place_long",
        ]

    def save_places(self):
        self.workplaces.to_csv(f"workplaces_{self.city_id}.csv")
        self.schools.to_csv(f"schools_{self.city_id}.csv")
        self.public_places.to_csv(f"public_places_{self.city_id}.csv")

    def assign_workplace_individual(self, individual):
        if individual["WorksAtSameCategory"]:
            possible_workplaces = self.workplaces[
                (self.workplaces["workplace_type"] == individual["JobLabel"])
            ]
        else:
            possible_workplaces = self.workplaces[
                (self.workplaces["workplace_type"] != individual["JobLabel"])
            ]
        distances = 1 / (
            pow(possible_workplaces["workplace_lat"] - individual["H_Lat"], 2)
            + pow(possible_workplaces["workplace_long"] - individual["H_Lon"], 2)
        ).astype(np.float64).apply(np.sqrt)
        try:
            return possible_workplaces.sample(weights=distances).iloc[0][
                ["workplace_name", "workplace_lat", "workplace_long"]
            ]
        except exception as ex:
            logging.exception(f"{dict(individual)}_{possible_workplaces}", exc_info=ex)
            return self.workplaces.sample().iloc[0][
                ["workplace_name", "workplace_lat", "workplace_long"]
            ]

    def _assign_workplaces(self, population):
        return population.progress_apply(self.assign_workplace_individual, axis=1)

    def assign_workplaces(self, adult_population):
        to_different_category = 0.05
        same_category = (
            np.random.random(size=len(adult_population)) > to_different_category
        )
        adult_population["WorksAtSameCategory"] = same_category
        adult_population[["WorkPlaceID", "W_Lat", "W_Lon"]] = parallelize_dataframe(
            adult_population, self._assign_workplaces, self.n_process
        )
        return adult_population

    def assign_school_individual(self, individual):
        distances = 1 / (
            pow(self.schools["school_lat"] - individual["H_Lat"], 2)
            + pow(self.schools["school_long"] - individual["H_Lon"], 2)
        ).astype(np.float64).apply(np.sqrt)
        return self.schools.sample(weights=distances).iloc[0][
            ["school_name", "school_lat", "school_long"]
        ]

    def _assign_schools(self, population):
        return population.progress_apply(self.assign_school_individual, axis=1)

    def assign_schools(self, children_population):
        children_population[
            ["school_id", "school_lat", "school_long"]
        ] = parallelize_dataframe(
            children_population, self._assign_schools, self.n_process
        )
        return children_population

    def assign_public_place_individual(self, individual):
        distances = 1 / (
            pow(self.public_places["public_place_lat"] - individual["H_Lat"], 2)
            + pow(self.public_places["public_place_long"] - individual["H_Lon"], 2)
        ).astype(np.float64).apply(np.sqrt)
        return self.public_places.sample(weights=distances).iloc[0][
            ["public_place_name", "public_place_lat", "public_place_long"]
        ]

    def _assign_public_places(self, population):
        return population.progress_apply(self.assign_public_place_individual, axis=1)

    def assign_public_places(self, population):
        population[
            ["public_place_id", "public_place_lat", "public_place_long"]
        ] = parallelize_dataframe(
            population, self._assign_public_places, self.n_process
        )
        return population


def parallelize_dataframe(df, func, n_cores=8):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


synthetic_population = synthetic_population.sample(n=10000).reset_index()

n_processes = 16  # Multiprocessing Adjustments

places_object = Places(1, len(synthetic_population), n_processes)
synthetic_population.drop(synthetic_population.columns[0], axis=1, inplace=True)
synthetic_population.drop(synthetic_population.columns[0], axis=1, inplace=True)
synthetic_population

places_object.generate_workplaces(list(synthetic_population["JobLabel"]))
places_object.generate_schools()
places_object.generate_public_places()

adults = synthetic_population[synthetic_population["age"] > 18]
adults = places_object.assign_workplaces(adults)

children = synthetic_population[synthetic_population["age"] < 19]
children = places_object.assign_schools(children)

total_population = pd.concat([adults, children], axis=0)
total_population = places_object.assign_public_places(total_population)

print(total_population.head())
total_population.to_csv(
    "/content/drive/MyDrive/synthpop/kolkata_synthpop_additions.csv"
)
