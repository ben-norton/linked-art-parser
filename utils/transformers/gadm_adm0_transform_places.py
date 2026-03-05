import csv
import json

# ── Configuration ────────────────────────────────────────────────────────────
INPUT_CSV  = "gadm41_adm0.csv"
OUTPUT_JSON = "gadm41_adm0_linked_art.json"

BASE_URI      = "https://gadm.org/place/"
NATION_AAT    = "http://vocab.getty.edu/aat/300128207"   # Nation
TERRITORY_AAT = "http://vocab.getty.edu/aat/300264237"   # Territory (disputed Z-codes)
PREF_NAME_AAT = "http://vocab.getty.edu/aat/300404670"   # Preferred Name
ISO3_AAT      = "http://vocab.getty.edu/aat/300404629"   # ISO 3166-1 alpha-3

# GADM uses Z01–Z09 for disputed territories (India/China/Pakistan border areas)
DISPUTED_CODES = {f"Z{str(i).zfill(2)}" for i in range(1, 10)}

# ── Transform ────────────────────────────────────────────────────────────────
records = []
with open(INPUT_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        iso3 = row["iso3166-a3"].strip()
        name = row["country_name"].strip()

        if iso3 in DISPUTED_CODES:
            classified = [{"id": TERRITORY_AAT, "type": "Type", "_label": "Territory"}]
        else:
            classified = [{"id": NATION_AAT, "type": "Type", "_label": "Nation"}]

        place = {
            "@context": "https://linked.art/ns/v1/linked-art.json",
            "id": f"{BASE_URI}{iso3}",
            "type": "Place",
            "_label": name,
            "classified_as": classified,
            "identified_by": [
                {
                    "type": "Name",
                    "content": name,
                    "classified_as": [{"id": PREF_NAME_AAT, "type": "Type", "_label": "Preferred Name"}]
                },
                {
                    "type": "Identifier",
                    "content": iso3,
                    "classified_as": [{"id": ISO3_AAT, "type": "Type", "_label": "ISO 3166-1 alpha-3"}]
                }
            ]
        }
        records.append(place)

output = {"@context": "https://linked.art/ns/v1/linked-art.json", "records": records}
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print(f"Done: {len(records)} records → {OUTPUT_JSON}")
