#!/usr/bin/env python3
import json
from typing import Tuple

import numpy as np
import pandas as pd
from shapely.geometry import Polygon, Point, MultiPolygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from scipy.ndimage import gaussian_filter
from skimage import measure
from main import main
import os

def compute_envelopes_from_df(
        df: pd.DataFrame,
        grid_size: Tuple[int,int],
        precision_blur_sigma: float,
        transition_blur_sigma: float,
        precision_threshold: float,
        transition_threshold_range: Tuple[float,float],
        transition_threshold_min_area: float,
) -> Tuple[Polygon, Polygon]:
    """
    Returns (precision_envelope, transition_envelope)
    """
    # ——— same rasterization & H buildup as before ———
    # Build list of flight paths
    trajs = []
    for icao, grp in df.groupby("icao24"):
        coords = list(zip(grp["lon"], grp["lat"], grp["geoaltitude"]))
        if len(coords) >= 2:
            trajs.append(coords)

    # Determine bounding box
    all_lons = np.hstack([[p[0] for p in t] for t in trajs])
    all_lats = np.hstack([[p[1] for p in t] for t in trajs])
    all_alts = np.hstack([[p[2] for p in t] for t in trajs])
    min_lon, max_lon = all_lons.min(), all_lons.max()
    min_lat, max_lat = all_lats.min(), all_lats.max()
    min_alt, max_alt = all_alts.min(), all_alts.max()

    nx, ny , nz = grid_size 
    H = np.zeros((ny, nx, nz), dtype=float)

    for traj in trajs:
        for (x0, y0, z0), (x1, y1, z1) in zip(traj, traj[1:]):
            steps = max(int(np.hypot(x1 - x0, y1 - y0) * nx), 2)
            xs = np.linspace(x0, x1, steps)
            ys = np.linspace(y0, y1, steps)
            zs = np.linspace(z0, z1, steps)
            ix = ((xs - min_lon) / (max_lon - min_lon) * (nx - 1)).astype(int).clip(0, nx - 1)
            iy = ((ys - min_lat) / (max_lat - min_lat) * (ny - 1)).astype(int).clip(0, ny - 1)
            iz = ((zs - min_alt) / (max_alt - min_alt) * (nz - 1)).astype(int).clip(0, nz - 1)
            H[iy, ix, iz] += 1

    """    def extract(mask: np.ndarray) -> BaseGeometry:
            contours = measure.find_contours(mask.astype(int), 0.5)
            polys = []
            for cnt in contours:
                lons = min_lon + (cnt[:,1]/(nx-1))*(max_lon-min_lon)
                lats = min_lat + (cnt[:,0]/(ny-1))*(max_lat-min_lat)
                poly = Polygon(zip(lons, lats))
                if poly.is_valid and poly.area > 0:
                    polys.append(poly)
            return unary_union(polys)

        # 1) precision envelope
        Hf_prec  = gaussian_filter(H, sigma=precision_blur_sigma)
        mask_prec = Hf_prec >= precision_threshold
        prec_env = extract(mask_prec)

        # 2) transition envelope, then filter by min_area
        Hf_tr      = gaussian_filter(H, sigma=transition_blur_sigma)
        low, high  = transition_threshold_range
        mask_tr    = (Hf_tr >= low) & (Hf_tr <= high)
        raw_trans  = extract(mask_tr)

        # --- NEW: drop any polygon parts smaller than transition_threshold_min_area ---
        if isinstance(raw_trans, Polygon):
            trans_env = raw_trans if raw_trans.area >= transition_threshold_min_area else Polygon()
        elif isinstance(raw_trans, MultiPolygon):
            kept = [p for p in raw_trans.geoms if p.area >= transition_threshold_min_area]
            trans_env = unary_union(kept) if kept else Polygon()
        else:
            # in case extract ever returns a GeometryCollection
            all_polys = [g for g in raw_trans.geoms if isinstance(g, Polygon)]
            kept      = [p for p in all_polys if p.area >= transition_threshold_min_area]
            trans_env = unary_union(kept) if kept else Polygon()
    """
    def extract_slice(mask2d, alt_index) -> BaseGeometry:
        contours = measure.find_contours(mask2d.astype(int), 0.5)
        polys = []
        for cnt in contours:
            lons = min_lon + (cnt[:, 1] / (nx - 1)) * (max_lon - min_lon)
            lats = min_lat + (cnt[:, 0] / (ny - 1)) * (max_lat - min_lat)
            poly = Polygon(zip(lons, lats))
            if poly.is_valid and poly.area > 0:
                polys.append(poly)
        return unary_union(polys) if polys else Polygon()
    # Percision
    H_prec = gaussian_filter(H, sigma=precision_blur_sigma)
    precision_slices = []
    for iz in range(nz):
        mask = H_prec[:, :, iz] >= precision_threshold
        poly = extract_slice(mask,iz)
        if not poly.is_empty:
            alt_val = min_alt + (iz / (nz - 1)) * (max_alt - min_alt)
            precision_slices.append((alt_val, poly))

    #  Transition
    low, high = transition_threshold_range
    H_tr = gaussian_filter(H, sigma=transition_blur_sigma)
    transition_slices = []
    for iz in range(nz):
        mask = (H_tr[:, :, iz] >= low) & (H_tr[:, :, iz] <= high)
        poly = extract_slice(mask,iz)
        if not poly.is_empty:
            alt_val = min_alt + (iz / (nz - 1)) * (max_alt - min_alt)
            transition_slices.append((alt_val, poly))

    return precision_slices, transition_slices

def annotate_3d(df: pd.DataFrame, slices: list[Tuple[float, BaseGeometry]], colname: str, tolerance: float = 500):
    """
    Annotates the DataFrame with a boolean column for whether each point falls into any slice,
    given a tolerance in feet for altitude binning.
    """
    results = []
    for _, row in df.iterrows():
        pt = Point(row["lon"], row["lat"])
        alt = row["geoaltitude"]

        in_any = False
        for slice_alt, poly in slices:
            if abs(alt - slice_alt) <= tolerance:
                if poly.contains(pt):
                    in_any = True
                    break
        results.append(in_any)

    df[colname] = results



if __name__ == "__main__":
    # 1) Get the full DataFrame from your pipeline
    if(os.path.exists('annotated_flights.csv')):
        df = pd.read_csv('annotated_flights.csv')
    else:    
        df = main(should_sum_dfs=True, should_send_to_kafka=False)
    df = df.dropna(subset=["geoaltitude"])
    df = df[df["geoaltitude"] <= 10000]
    # Ensure it has 'lon' and 'lat' columns
    assert "lon" in df.columns and "lat" in df.columns and "geoaltitude" in df.columns

    precision_blur_sigma = 0.00625
    transition_blur_sigma = 0.01
    precision_treshold = 15.0
    transition_threshold = (3.0, 25.0)
    transition_threshold_min_area = 0.0001

    prec_env, trans_env = compute_envelopes_from_df(
        df,
        grid_size=(3000, 3000, 20),
        precision_blur_sigma=precision_blur_sigma,
        transition_blur_sigma=transition_blur_sigma,
        precision_threshold=precision_treshold,
        transition_threshold_range=transition_threshold,
        transition_threshold_min_area=transition_threshold_min_area,
    )


    # dump both into one GeoJSON
    features = []
    for name, env_list, params in [
        ("precision",  prec_env,  {"sigma":0.00625, "thresh":15.0}),
        ("transition", trans_env, {"sigma":0.02, "thresh_range": (5.0, 14.9)})
    ]:
        for altitude, geom in env_list:
            if not geom.is_empty:
                features.append({
                    "type": "Feature",
                    "geometry": geom.__geo_interface__,
                    "properties": {
                        "type": name,
                        "geoaltitude": altitude,
                        **params
                    }
                })
    with open("flight_envelopes_3d.geojson", "w") as f:
        json.dump({
            "type": "FeatureCollection",
            "features": features
        }, f)
    # annotate your DF with two flags
    annotate_3d(df, prec_env, "in_precision_env_3d")
    annotate_3d(df, trans_env, "in_transition_env_3d")
    
    df.to_csv("annotated_flights_3d.csv", index=False)
    print(f"Precision inside: {df['in_precision_env'].sum()} / {len(df)}")
    print(f"Transition inside: {df['in_transition_env'].sum()} / {len(df)}")
