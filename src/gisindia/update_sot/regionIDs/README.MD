# Unique Region IDs for India: Source of Truth

### CHANGELOG
2023-09-14: Adding Pune Municipal Corporation, renaming PCMC to Pimpri Chinchwad, restructuring folder to be more intuitive

### About

This subpackage in ```gisindia``` is attempting to solve the problem with varying names for different regions across India. Here, we create a source of truth and assign a unique regionID to every region in India.

The latest updated source of truth for all regionIDs can be found at [```regionIDs.csv```](regionIDs.csv).

This documentation will be further updated moving forward.

Each entry in the file contains a unique regionID, the name of the region, and the parentID. 

The Country India has the code IN, based on the ISO 3166 alpha-2 code. 

The IDs for states, districts, urban local bodies (ulbs), subdistricts, and villages are based on [LGD](https://lgdirectory.gov.in). Therefore, an LGD code of 25 for a district results in a regionID of district_25.

The IDs for wards and zones are constructed based on the parent ULB. Zones are assigned a number based on a alphabetical sorting. If the parent ULB has a code of 25 and the zone is the first alphabetical zone, the ID is constructed as ulb_25-zone-1. Wherease for wards, the numbers are taken from the ward numbers as assigned by the respective Municipal Corporation. ward 26 of ulb_25 is therefore ulb_25-ward-26. 

If a ward name is not available for a ward with number 1, the ward is simply named as "Ward No. 1".

It is currently recommended that, for readability, the regionIDs be used for computational purposes, but the regionID be paired with the regionName for readability in user-facing applications. For example, the Koramangala Ward can potentially be represented as ```ulb_276600-ward-151_Koramangala```.

It is also recommend that, while using these IDs of truth, when unknown, a code of -1 is assigned. (state_-1, zone_-1, etc).