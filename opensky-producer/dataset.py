import gzip
from io import BytesIO

import numpy as np
import pandas
import pandas as pd
from sklearn.neighbors import BallTree

class Dataset:

    def __init__(self, tar_tuple: tuple[str, callable]):
        self.tar_key, self.fetch_handler = tar_tuple

    def request_df(self):
        file = self.fetch_handler()

        for name, content in file.items():
            if name.lower().endswith(".csv.gz"):
                with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                    csv_bytes = gz.read()
                    df = pd.read_csv(
                        BytesIO(csv_bytes),
                    )
                    return df

        raise FileNotFoundError("No CSV file found in the tar archive.")

def df_near_coordinates(df: pandas.DataFrame, center: tuple[float, float], radius_miles: float) -> pandas.DataFrame:
    """
    Find all rows in the DataFrame within a given radius of a center coordinate.

    :param df: DataFrame containing 'lat' and 'lon' columns.
    :param center: Tuple of (latitude, longitude) for the center point.
    :param radius_miles: Radius in miles to search around the center.
    :return: DataFrame with rows within the specified radius.
    """
    df = df.copy()

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    df = df.dropna(subset=["lat", "lon"]).reset_index(drop=True)

    coords = np.deg2rad(df[["lat", "lon"]].values)
    tree = BallTree(coords, metric="haversine")

    center_rad = np.deg2rad([center])
    earth_radius_miles = 3958.8
    radius_radians = radius_miles / earth_radius_miles

    indices = tree.query_radius(center_rad, r=radius_radians)[0]

    return df.iloc[indices].reset_index(drop=True)
