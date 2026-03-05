#!/usr/bin/env python3
"""
Transform GADM ADM1 CSV to Linked Art Places Model (JSON-LD)
Documentation: https://linked.art/model/place/

Input:  sources\gadm\
			gadm41_adm1.csv
Output: output\gadm\
			gadm41_adm1_linked_art.json  (JSON array of Place records)
        	gadm41_countries_linked_art.json (country-level stub Place records)
"""

import csv
import json
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LINKED_ART_CONTEXT = "https://linked.art/ns/v1/linked-art.json"
BASE_URI = "https://gadm.org/place/"          # Base URI for all GADM Places
GADM_ID_AAT = "http://vocab.getty.edu/aat/300404626"   # Local Number / identifier
ISO_ID_AAT  = "http://vocab.getty.edu/aat/300411725"   # ISO / international standard number
PREF_NAME_AAT = "http://vocab.getty.edu/aat/300404670" # Preferred term
ALT_NAME_AAT  = "http://vocab.getty.edu/aat/300264273" # Alternate name / variant spelling
NATION_AAT    = "http://vocab.getty.edu/aat/300128207" # Nation

# ---------------------------------------------------------------------------
# AAT classification mapping for fod_type_en values
# Falls back to generic "Administrative Division" where no better match exists.
# ---------------------------------------------------------------------------

ADM_DIVISION_AAT = ("http://vocab.getty.edu/aat/300387428", "Administrative Division")

TYPE_AAT_MAP: dict[str, tuple[str, str]] = {
    # ---- Standard administrative types ----
    "Province":                      ("http://vocab.getty.edu/aat/300000776", "Province"),
    "Autonomous Province":           ("http://vocab.getty.edu/aat/300000776", "Province"),
    "State":                         ("http://vocab.getty.edu/aat/300000776", "Province"),  # AAT uses same node
    "Free State":                    ("http://vocab.getty.edu/aat/300000776", "Province"),
    "Region":                        ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Autonomous Region":             ("http://vocab.getty.edu/aat/300182722", "Region"),
    "'Autonomous Region'":           ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Autononous Region":             ("http://vocab.getty.edu/aat/300182722", "Region"),  # typo in source
    "Metropolitan Autonomous City":  ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Metropolian Region":            ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Statistical Region":            ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Sub-Region":                    ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Development Region":            ("http://vocab.getty.edu/aat/300182722", "Region"),
    "District":                      ("http://vocab.getty.edu/aat/300000792", "District"),
    "Capital District":              ("http://vocab.getty.edu/aat/300000792", "District"),
    "National District":             ("http://vocab.getty.edu/aat/300000792", "District"),
    "Federal District":              ("http://vocab.getty.edu/aat/300000792", "District"),
    "Autonomous district":           ("http://vocab.getty.edu/aat/300000792", "District"),
    "Districts of Republican Subordin": ("http://vocab.getty.edu/aat/300000792", "District"),
    "Special Administrative Region": ("http://vocab.getty.edu/aat/300000792", "District"),
    "County":                        ("http://vocab.getty.edu/aat/300000771", "County"),
    "Department":                    ("http://vocab.getty.edu/aat/300265612", "Department"),
    "City":                          ("http://vocab.getty.edu/aat/300008389", "City"),
    "Capital City":                  ("http://vocab.getty.edu/aat/300008389", "City"),
    "Autonomous City":               ("http://vocab.getty.edu/aat/300008389", "City"),
    "Metropolitan City":             ("http://vocab.getty.edu/aat/300008389", "City"),
    "Capital Metropolitan City":     ("http://vocab.getty.edu/aat/300008389", "City"),
    "Metropolitan City":             ("http://vocab.getty.edu/aat/300008389", "City"),
    "Special City":                  ("http://vocab.getty.edu/aat/300008389", "City"),
    "Metropolis":                    ("http://vocab.getty.edu/aat/300008389", "City"),
    "Directly Governed City":        ("http://vocab.getty.edu/aat/300008389", "City"),
    "Captial City District":         ("http://vocab.getty.edu/aat/300008389", "City"),
    "Municipality":                  ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Special Municipality":          ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Autonomous Commune":            ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Commune":                       ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Municipality|Prefecture":       ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Territory":                     ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Capital Territory":             ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Federal Territory":             ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Union Territory":               ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Union territory":               ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Autonomous Territory":          ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Indigenous Territory":          ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Sovereign Base Area":           ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Dependency":                    ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Outer Islands":                 ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Canton":                        ("http://vocab.getty.edu/aat/300387555", "Canton"),
    "Island":                        ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Atoll":                         ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Atol":                          ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Autonomous Island":             ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Autonomous island":             ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Island Group":                  ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Group of islands":              ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Island Council":                ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Parish":                        ("http://vocab.getty.edu/aat/300000843", "Parish"),
    "Parish District":               ("http://vocab.getty.edu/aat/300000843", "Parish"),
    "Borough":                       ("http://vocab.getty.edu/aat/300000774", "Borough"),
    "Prefecture":                    ("http://vocab.getty.edu/aat/300387556", "Prefecture"),
    "Urban Prefecture":              ("http://vocab.getty.edu/aat/300387556", "Prefecture"),
    "Economic Prefecture":           ("http://vocab.getty.edu/aat/300387556", "Prefecture"),
    "Governorate":                   ("http://vocab.getty.edu/aat/300387428", "Governorate"),
    "Emirate":                       ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Republic":                      ("http://vocab.getty.edu/aat/300128207", "Nation"),
    "Autonomous Republic":           ("http://vocab.getty.edu/aat/300128207", "Nation"),
    "Constituent Country":           ("http://vocab.getty.edu/aat/300128207", "Nation"),
    "Kingdom":                       ("http://vocab.getty.edu/aat/300128207", "Nation"),
    "Voivodeship":                   ("http://vocab.getty.edu/aat/300000776", "Province"),
    "Division":                      ("http://vocab.getty.edu/aat/300000792", "District"),
    "Circuit":                       ("http://vocab.getty.edu/aat/300387428", "Administrative Division"),
    "Arrondissement":                ("http://vocab.getty.edu/aat/300387428", "Administrative Division"),
    "Intendancy":                    ("http://vocab.getty.edu/aat/300387428", "Administrative Division"),
    "Commissiary":                   ("http://vocab.getty.edu/aat/300387428", "Administrative Division"),
    "Administrative Area":           ADM_DIVISION_AAT,
    "Administrative subdivisions":   ADM_DIVISION_AAT,
    "Autonomous Community":          ADM_DIVISION_AAT,
    "Autonomous Sector":             ADM_DIVISION_AAT,
    "Centrally Administered Area":   ADM_DIVISION_AAT,
    "Capital City":                  ("http://vocab.getty.edu/aat/300008389", "City"),
    "Decentralized administration":  ADM_DIVISION_AAT,
    "Entity":                        ADM_DIVISION_AAT,
    "Territorial Unit":              ADM_DIVISION_AAT,
    "Special Region|Zone":           ADM_DIVISION_AAT,
    "Village District":              ("http://vocab.getty.edu/aat/300000792", "District"),
    "Town":                          ("http://vocab.getty.edu/aat/300008347", "Town"),
    "Independent Town":              ("http://vocab.getty.edu/aat/300008347", "Town"),
    "Town Council":                  ("http://vocab.getty.edu/aat/300008347", "Town"),
    "Town District":                 ("http://vocab.getty.edu/aat/300008347", "Town"),
    "Independent City":              ("http://vocab.getty.edu/aat/300008389", "City"),
    "Quarter":                       ("http://vocab.getty.edu/aat/300000792", "District"),
    "National Park":                 ("http://vocab.getty.edu/aat/300387428", "Administrative Division"),
    "Water body":                    ("http://vocab.getty.edu/aat/300008626", "Body of Water"),
    "Reef":                          ("http://vocab.getty.edu/aat/300132693", "Reef"),
    "?":                             ADM_DIVISION_AAT,
    "NA":                            ADM_DIVISION_AAT,
}


def safe_slug(value: str) -> str:
    """Create a URI-safe slug from a string."""
    return re.sub(r"[^A-Za-z0-9._\-]", "_", value)


def build_place_uri(gadm_identifier: str) -> str:
    return f"{BASE_URI}{safe_slug(gadm_identifier)}"


def build_country_uri(gadm_id: str) -> str:
    return f"{BASE_URI}{gadm_id}"


def get_type_classification(fod_type: str) -> tuple[str, str]:
    """Return (aat_uri, label) for a given type string."""
    return TYPE_AAT_MAP.get(fod_type.strip(), ADM_DIVISION_AAT)


def build_linked_art_place(row: dict) -> dict:
    """Convert a single CSV row to a Linked Art Place JSON-LD record."""
    fod_id      = row["fod_gadm_identifier"].strip()
    pref_label  = row["pref_label"].strip()
    alt_labels  = row["alt_labels"].strip()
    fod_type    = row["fod_type_en"].strip()
    gadm_id     = row["gadm_id"].strip()
    country     = row["country_name"].strip()
    fod_iso     = row["fod_iso"].strip()

    aat_uri, aat_label = get_type_classification(fod_type)

    # ---- Core record ----
    place = {
        "@context": LINKED_ART_CONTEXT,
        "id": build_place_uri(fod_id),
        "type": "Place",
        "_label": pref_label,
        "classified_as": [
            {
                "id": aat_uri,
                "type": "Type",
                "_label": aat_label
            }
        ],
        "identified_by": [],
        "part_of": [
            {
                "id": build_country_uri(gadm_id),
                "type": "Place",
                "_label": country,
                "classified_as": [
                    {
                        "id": NATION_AAT,
                        "type": "Type",
                        "_label": "Nation"
                    }
                ]
            }
        ]
    }

    # ---- Preferred name ----
    place["identified_by"].append({
        "type": "Name",
        "content": pref_label,
        "classified_as": [
            {
                "id": PREF_NAME_AAT,
                "type": "Type",
                "_label": "Preferred Name"
            }
        ]
    })

    # ---- Alternate names (pipe-separated) ----
    if alt_labels and alt_labels.lower() not in ("na", ""):
        for alt in alt_labels.split("|"):
            alt = alt.strip()
            if alt and alt != pref_label:
                place["identified_by"].append({
                    "type": "Name",
                    "content": alt,
                    "classified_as": [
                        {
                            "id": ALT_NAME_AAT,
                            "type": "Type",
                            "_label": "Alternate Name"
                        }
                    ]
                })

    # ---- GADM identifier ----
    place["identified_by"].append({
        "type": "Identifier",
        "content": fod_id,
        "classified_as": [
            {
                "id": GADM_ID_AAT,
                "type": "Type",
                "_label": "GADM Identifier"
            }
        ]
    })

    # ---- ISO identifier (when present) ----
    if fod_iso and fod_iso.upper() not in ("NA", ""):
        place["identified_by"].append({
            "type": "Identifier",
            "content": fod_iso,
            "classified_as": [
                {
                    "id": ISO_ID_AAT,
                    "type": "Type",
                    "_label": "ISO 3166-2 Code"
                }
            ]
        })

    return place


def build_country_stub(gadm_id: str, country_name: str) -> dict:
    """Build a minimal Linked Art Place record for a country."""
    return {
        "@context": LINKED_ART_CONTEXT,
        "id": build_country_uri(gadm_id),
        "type": "Place",
        "_label": country_name,
        "classified_as": [
            {
                "id": NATION_AAT,
                "type": "Type",
                "_label": "Nation"
            }
        ],
        "identified_by": [
            {
                "type": "Name",
                "content": country_name,
                "classified_as": [
                    {
                        "id": PREF_NAME_AAT,
                        "type": "Type",
                        "_label": "Preferred Name"
                    }
                ]
            },
            {
                "type": "Identifier",
                "content": gadm_id,
                "classified_as": [
                    {
                        "id": "http://vocab.getty.edu/aat/300404629",  # ISO 3166-1 alpha-3
                        "type": "Type",
                        "_label": "ISO 3166-1 Alpha-3 Code"
                    }
                ]
            }
        ]
    }


def main():
    input_path   = Path("/mnt/user-data/uploads/gadm41_adm1.csv")
    adm1_out     = Path("/home/claude/gadm41_adm1_linked_art.json")
    country_out  = Path("/home/claude/gadm41_countries_linked_art.json")

    places = []
    countries: dict[str, str] = {}  # gadm_id → country_name

    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            place = build_linked_art_place(row)
            places.append(place)
            gadm_id = row["gadm_id"].strip()
            if gadm_id not in countries:
                countries[gadm_id] = row["country_name"].strip()

    country_records = [
        build_country_stub(gid, name)
        for gid, name in sorted(countries.items())
    ]

    # Write ADM1 output
    with open(adm1_out, "w", encoding="utf-8") as f:
        json.dump(places, f, ensure_ascii=False, indent=2)

    # Write country stubs output
    with open(country_out, "w", encoding="utf-8") as f:
        json.dump(country_records, f, ensure_ascii=False, indent=2)

    print(f"✓ ADM1 places:   {len(places):,} records → {adm1_out}")
    print(f"✓ Country stubs: {len(country_records):,} records → {country_out}")
    print(f"\nSample ADM1 record:")
    print(json.dumps(places[0], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
