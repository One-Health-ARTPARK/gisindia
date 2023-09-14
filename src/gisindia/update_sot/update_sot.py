import pandas as pd
import os
from pathlib import Path


lgd_sot_columns = ["regionID", "regionName", "parentID"]

revenue_cols = ["district_code", "district_name", "subdistrict_code",
                "subdistrict_name", "village_code", "village_name"]
revenue_hierarchy = ["village", "subdistrict", "district", "state", "country"]
ulb_hierarchy = ["ulb", "district", "state"]
sot_path = "regionIDs/regionIDs.csv"
sot_path = Path(__file__).parent / sot_path


def update_revenue(regions: pd.DataFrame, state_code: str, state_name: str) -> pd.DataFrame:
    """_summary_

    Args:
        regions (pd.DataFrame): _description_
        parentID (str): _description_

    Returns:
        pd.DataFrame: _description_
    """

    if os.path.exists(sot_path):
        regionIDs = pd.read_csv(sot_path)
    else:
        regionIDs = pd.DataFrame(columns=lgd_sot_columns)

    assert set(regions.columns.values).issubset(revenue_cols), \
        "provide a valid regions dataframe to update the source of truth"

    regions = regions[revenue_cols]
    regions["state_name"] = state_name.upper()
    regions["state_code"] = state_code
    regions["country_name"] = "INDIA"
    regions["country_code"] = "IN"
    regionIDs = update_lgd_sot(lgd_sot=regionIDs, lgd_to_add=regions, hierarchy=revenue_hierarchy)

    return regionIDs


def update_ulb(regions: pd.DataFrame, ulb_code: int, ulb_name: str) -> pd.DataFrame:
    """_summary_

    Args:
        regions (pd.DataFrame): regions to be added
        ulb_code (int): _description_
        ulb_name (str): _description_
        ward_name (bool, optional): _description_. Defaults to True.

    Returns:
        pd.DataFrame: _description_
    """

    if set(regions.columns.values) == {"ward_no", "zone_name"}:
        regions["ward_name"] = "Ward No. " + regions["ward_no"].astype(str)

    regionIDs = pd.read_csv(sot_path)

    ulbID = "ulb_" + str(ulb_code)

    zones_df = regions[["zone_name"]].drop_duplicates().reset_index(drop=True).copy()
    zones_df = zones_df.sort_values("zone_name").reset_index(drop=True)
    zones_df["zone_ID"] = [ulbID + "-zone-" + str(i) for i in range(1, len(zones_df)+1)]

    wards_df = regions.copy()
    wards_df = wards_df.merge(zones_df, on="zone_name")
    wards_df["ward_ID"] = ulbID + "-ward-" + wards_df["ward_no"].astype(str)
    wards_df.rename(mapper={"ward_ID": "regionID", "ward_name": "regionName", "zone_ID": "parentID"},
                    axis="columns", inplace=True)
    wards_df = wards_df.sort_values("ward_no").reset_index(drop=True)

    zones_df["parentID"] = ulbID
    zones_df.rename(mapper={"zone_name": "regionName", "zone_ID": "regionID"}, axis="columns", inplace=True)

    wards_df = wards_df[lgd_sot_columns]
    zones_df = zones_df[lgd_sot_columns]

    regionIDs = pd.concat([regionIDs, zones_df, wards_df], ignore_index=True)

    return regionIDs


def update_lgd_sot(lgd_sot: pd.DataFrame, lgd_to_add: pd.DataFrame, hierarchy: list) -> pd.DataFrame:
    """_summary_

    Args:
        lgd_sot (pd.DataFrame): Existing Source of Truth to Update
        lgd_to_add (pd.DataFrame): lgd to be added. must contain level_code, level_name for each level in hierarchy
        hierarchy (list): revenue vs ulb

    Returns:
        pd.DataFrame: Updated LGD Source of Truth
    """

    for i in range(len(hierarchy) - 1):

        region_level = hierarchy[i]
        parent_level = hierarchy[i+1]

        temp_df = lgd_to_add.copy()  # Creating temporary copy of lgd codes to bring out given level only
        temp_df = temp_df[[region_level + "_code", region_level + "_name",
                           parent_level + "_code"]].drop_duplicates().reset_index(drop=True)

        # Capitalising all names for uniformity
        temp_df[region_level + "_name"] = temp_df[region_level + "_name"].str.upper()

        # creating region id from code (concatenating level & code i.e. state 29 becomes state_29 for KARNATAKA)
        temp_df[region_level + "_code"] = region_level + "_" + temp_df[region_level + "_code"].astype(str)
        temp_df[parent_level + "_code"] = parent_level + "_" + temp_df[parent_level + "_code"].astype(str)

        temp_df.columns = lgd_sot_columns
        lgd_sot = pd.concat([lgd_sot, temp_df], ignore_index=True)

    lgd_sot.drop_duplicates(inplace=True)

    return lgd_sot


if __name__ == "__main__":

    states = {"29": "Karnataka"}  # States to add

    for state_code, state_name in states.items():
        src_path = "regionIDs/regionIDs_src/districts/" + "state_" + str(state_code) + ".csv"
        src_path = Path(__file__).parent / src_path
        regions = update_revenue(regions=pd.read_csv(src_path),
                                 state_code=state_code, state_name=state_name)
        regions.to_csv(sot_path, index=False)

    ulb_path = "regionIDs/regionIDs_src/ulbs/ulbs.csv"
    ulb_path = Path(__file__).parent / ulb_path
    regions = update_lgd_sot(lgd_sot=regions, lgd_to_add=pd.read_csv(ulb_path), hierarchy=ulb_hierarchy)
    regions.to_csv(sot_path, index=False)

    ulbs = {"276600": "BBMP", "251528": "Pimpri Chinchwad"}  # ulbs to add

    for ulb_code, ulb_name in ulbs.items():
        src_path = "regionIDs/regionIDs_src/ulbs/" + "ulb_" + str(ulb_code) + ".csv"
        src_path = Path(__file__).parent / src_path
        regions = update_ulb(regions=pd.read_csv(src_path),
                             ulb_code=ulb_code, ulb_name=ulb_name)
        regions.to_csv(sot_path, index=False)

    # Update Pune Data

    ulbs_prabhags = {251530: "PUNE"}
    no_prabhags = {251530: 58}

    for ulb_code, ulb_name in ulbs_prabhags.items():
        prabhags = pd.DataFrame(columns=["regionID", "regionName", "parentID"])

        regionNos = pd.Series(list(range(1, no_prabhags[ulb_code]+1)))
        regionIDs = "ulb_" + str(ulb_code) + "-prabhag-" + regionNos.astype(str)
        regionNames = "Prabhag No. " + regionNos.astype(str)

        prabhags["regionID"] = regionIDs
        prabhags["regionName"] = regionNames
        prabhags["parentID"] = "ulb_" + str(ulb_code)

        regions = pd.read_csv(sot_path)
        regions = pd.concat([regions, prabhags], ignore_index=True)
        regions.to_csv(sot_path, index=False)
