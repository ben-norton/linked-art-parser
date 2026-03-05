#!/usr/bin/env python3
"""
Transform GADM ADM2 CSV to Linked Art Places Model (JSON-LD)
Documentation: https://linked.art/model/place/

Input:  sources/gadm/gadm41_adm2.csv
Output: output/gadm/gadm41_adm2_linked_art.json  (JSON array of Place records)

Hierarchy expressed via part_of:
    ADM2 place  →  part_of  →  ADM1 parent  (fod_gadm_id / fod_label)
    ADM1 parent →  part_of  →  ADM0 country (country_gadm_id / country_name)
"""

import csv
import json
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LINKED_ART_CONTEXT = "https://linked.art/ns/v1/linked-art.json"
BASE_URI      = "https://gadm.org/place/"
GADM_ID_AAT   = "http://vocab.getty.edu/aat/300404626"   # Local Number / identifier
PREF_NAME_AAT = "http://vocab.getty.edu/aat/300404670"   # Preferred term
ALT_NAME_AAT  = "http://vocab.getty.edu/aat/300264273"   # Alternate name / variant spelling
NATION_AAT    = "http://vocab.getty.edu/aat/300128207"   # Nation

# ---------------------------------------------------------------------------
# AAT classification mapping for sod_type values
# Falls back to generic "Administrative Division" where no better match exists.
# ---------------------------------------------------------------------------

ADM_DIVISION_AAT = ("http://vocab.getty.edu/aat/300387428", "Administrative Division")

TYPE_AAT_MAP: dict[str, tuple[str, str]] = {
    # ── Province / State ────────────────────────────────────────────────────
    "Province":            ("http://vocab.getty.edu/aat/300000776", "Province"),
    "Provincie":           ("http://vocab.getty.edu/aat/300000776", "Province"),
    "Sub-Province":        ("http://vocab.getty.edu/aat/300000776", "Province"),
    # ── Region ──────────────────────────────────────────────────────────────
    "Region":              ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Capital Region":      ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Municipal Region":    ("http://vocab.getty.edu/aat/300182722", "Region"),
    "Subregion":           ("http://vocab.getty.edu/aat/300182722", "Region"),
    # ── District ────────────────────────────────────────────────────────────
    "District":            ("http://vocab.getty.edu/aat/300000792", "District"),
    "Distict":             ("http://vocab.getty.edu/aat/300000792", "District"),  # typo in source
    "District Council":    ("http://vocab.getty.edu/aat/300000792", "District"),
    "Federal District":    ("http://vocab.getty.edu/aat/300000792", "District"),
    "Metropolitan District":  ("http://vocab.getty.edu/aat/300000792", "District"),
    "Minor district":      ("http://vocab.getty.edu/aat/300000792", "District"),
    "Municipal District":  ("http://vocab.getty.edu/aat/300000792", "District"),
    "Municipal district":  ("http://vocab.getty.edu/aat/300000792", "District"),
    "Raion":               ("http://vocab.getty.edu/aat/300000792", "District"),
    "Regional District":   ("http://vocab.getty.edu/aat/300000792", "District"),
    "Sub-district":        ("http://vocab.getty.edu/aat/300000792", "District"),
    "Subdistrict":         ("http://vocab.getty.edu/aat/300000792", "District"),
    "Unitary District":    ("http://vocab.getty.edu/aat/300000792", "District"),
    "Unitary District (City)": ("http://vocab.getty.edu/aat/300000792", "District"),
    "Urban District":      ("http://vocab.getty.edu/aat/300000792", "District"),
    "Circle":              ("http://vocab.getty.edu/aat/300000792", "District"),
    "Division":            ("http://vocab.getty.edu/aat/300000792", "District"),
    "Autonomous district": ("http://vocab.getty.edu/aat/300000792", "District"),
    "Autonomous Okurg":    ("http://vocab.getty.edu/aat/300000792", "District"),
    "Capital District":    ("http://vocab.getty.edu/aat/300000792", "District"),
    "Special Woreda":      ("http://vocab.getty.edu/aat/300000792", "District"),
    "Distrito Metropolitano": ("http://vocab.getty.edu/aat/300000792", "District"),
    # ── County ──────────────────────────────────────────────────────────────
    "County":              ("http://vocab.getty.edu/aat/300000771", "County"),
    "Administrative County": ("http://vocab.getty.edu/aat/300000771", "County"),
    "Unitary Authority (County)": ("http://vocab.getty.edu/aat/300000771", "County"),
    "United County":       ("http://vocab.getty.edu/aat/300000771", "County"),
    "United Counties":     ("http://vocab.getty.edu/aat/300000771", "County"),
    # ── Department ──────────────────────────────────────────────────────────
    "Department":          ("http://vocab.getty.edu/aat/300265612", "Department"),
    # ── City ────────────────────────────────────────────────────────────────
    "City":                ("http://vocab.getty.edu/aat/300008389", "City"),
    "Autonomous City":     ("http://vocab.getty.edu/aat/300008389", "City"),
    "Capital":             ("http://vocab.getty.edu/aat/300008389", "City"),
    "City Council":        ("http://vocab.getty.edu/aat/300008389", "City"),
    "City and Borough":    ("http://vocab.getty.edu/aat/300008389", "City"),
    "City and County":     ("http://vocab.getty.edu/aat/300008389", "City"),
    "City council":        ("http://vocab.getty.edu/aat/300008389", "City"),
    "City of Regional Significance": ("http://vocab.getty.edu/aat/300008389", "City"),
    "City with powiat rights": ("http://vocab.getty.edu/aat/300008389", "City"),
    "Independent City":    ("http://vocab.getty.edu/aat/300008389", "City"),
    "Metropolis":          ("http://vocab.getty.edu/aat/300008389", "City"),
    "Prefecture City":     ("http://vocab.getty.edu/aat/300008389", "City"),
    "Provincial City":     ("http://vocab.getty.edu/aat/300008389", "City"),
    "Rural City":          ("http://vocab.getty.edu/aat/300008389", "City"),
    "Statutory City":      ("http://vocab.getty.edu/aat/300008389", "City"),
    "Statutory city":      ("http://vocab.getty.edu/aat/300008389", "City"),
    "Unitary Authority (City)": ("http://vocab.getty.edu/aat/300008389", "City"),
    # ── Municipality ────────────────────────────────────────────────────────
    "Municipality":        ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "City Municipality":   ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Commune":             ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Commune-Cotiere":     ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Commune|Municipality":("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "District Municipality":("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Districts|Municipals":("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Municipiality":       ("http://vocab.getty.edu/aat/300265609", "Municipality"),  # typo
    "Municpality|City Council": ("http://vocab.getty.edu/aat/300265609", "Municipality"),  # typo
    "Regional County Municipality": ("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Regional Municipality":("http://vocab.getty.edu/aat/300265609", "Municipality"),
    "Special Municipality":("http://vocab.getty.edu/aat/300265609", "Municipality"),
    # ── Territory ───────────────────────────────────────────────────────────
    "Territory":           ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Area Outside Territorial Authori": ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Dependency":          ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Emirate":             ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Federal Territory":   ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Indigenous Territory":("http://vocab.getty.edu/aat/300264237", "Territory"),
    "Reserve":             ("http://vocab.getty.edu/aat/300264237", "Territory"),
    "State reserve":       ("http://vocab.getty.edu/aat/300264237", "Territory"),
    # ── Canton ──────────────────────────────────────────────────────────────
    "Canton":              ("http://vocab.getty.edu/aat/300387555", "Canton"),
    # ── Island ──────────────────────────────────────────────────────────────
    "Island":              ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Island area":         ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Islands":             ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Atol":                ("http://vocab.getty.edu/aat/300008156", "Island"),
    "Reef":                ("http://vocab.getty.edu/aat/300132693", "Reef"),
    # ── Parish ──────────────────────────────────────────────────────────────
    "Parish":              ("http://vocab.getty.edu/aat/300000843", "Parish"),
    # ── Borough ─────────────────────────────────────────────────────────────
    "Borough":             ("http://vocab.getty.edu/aat/300000774", "Borough"),
    "Borough District":    ("http://vocab.getty.edu/aat/300000774", "Borough"),
    "Metropolitan Borough":("http://vocab.getty.edu/aat/300000774", "Borough"),
    "Metropolitan Borough (City)": ("http://vocab.getty.edu/aat/300000774", "Borough"),
    "Special Ward":        ("http://vocab.getty.edu/aat/300000774", "Borough"),
    # ── Prefecture ──────────────────────────────────────────────────────────
    "Prefecture":          ("http://vocab.getty.edu/aat/300387556", "Prefecture"),
    "Autonomous Prefecture":("http://vocab.getty.edu/aat/300387556", "Prefecture"),
    "Sub-Prefecture":      ("http://vocab.getty.edu/aat/300387556", "Prefecture"),
    "Sub-prefecture":      ("http://vocab.getty.edu/aat/300387556", "Prefecture"),
    "Subprefecture":       ("http://vocab.getty.edu/aat/300387556", "Prefecture"),
    # ── Town ────────────────────────────────────────────────────────────────
    "Town":                ("http://vocab.getty.edu/aat/300008347", "Town"),
    "Town Council":        ("http://vocab.getty.edu/aat/300008347", "Town"),
    "Town|Municipal":      ("http://vocab.getty.edu/aat/300008347", "Town"),
    "Township":            ("http://vocab.getty.edu/aat/300008347", "Town"),
    "Village":             ("http://vocab.getty.edu/aat/300008347", "Town"),
    # ── Water body ──────────────────────────────────────────────────────────
    "Water Body":          ("http://vocab.getty.edu/aat/300008626", "Body of Water"),
    "Water body":          ("http://vocab.getty.edu/aat/300008626", "Body of Water"),
    "Waterbody":           ("http://vocab.getty.edu/aat/300008626", "Body of Water"),
    # ── Fallback: Administrative Division ───────────────────────────────────
    "Aboriginal Council":  ADM_DIVISION_AAT,
    "Administrative Area": ADM_DIVISION_AAT,
    "Administrative Zone": ADM_DIVISION_AAT,
    "Area":                ADM_DIVISION_AAT,
    "Area council":        ADM_DIVISION_AAT,
    "Arrondissement":      ADM_DIVISION_AAT,
    "Assembly":            ADM_DIVISION_AAT,
    "Census Area":         ADM_DIVISION_AAT,
    "Census Division":     ADM_DIVISION_AAT,
    "Chef-Lieu-Wilaya":    ADM_DIVISION_AAT,
    "City council":        ("http://vocab.getty.edu/aat/300008389", "City"),
    "Conservancy":         ADM_DIVISION_AAT,
    "Constituency":        ADM_DIVISION_AAT,
    "Corregimiento Departamental": ADM_DIVISION_AAT,
    "Delegation":          ADM_DIVISION_AAT,
    "Governorate":         ADM_DIVISION_AAT,
    "Headquarter":         ADM_DIVISION_AAT,
    "League":              ADM_DIVISION_AAT,
    "Local Authority":     ADM_DIVISION_AAT,
    "Local council":       ADM_DIVISION_AAT,
    "Mukim":               ADM_DIVISION_AAT,
    "NA":                  ADM_DIVISION_AAT,
    "National Park":       ADM_DIVISION_AAT,
    "Neighbourhood Democratic": ADM_DIVISION_AAT,
    "Not Classified":      ADM_DIVISION_AAT,
    "Part":                ADM_DIVISION_AAT,
    "Quarter":             ADM_DIVISION_AAT,
    "Raion":               ("http://vocab.getty.edu/aat/300000792", "District"),
    "Regency":             ADM_DIVISION_AAT,
    "Regional Council":    ADM_DIVISION_AAT,
    "Ressort":             ADM_DIVISION_AAT,
    "Sector":              ADM_DIVISION_AAT,
    "Shire":               ADM_DIVISION_AAT,
    "Sub-chief":           ADM_DIVISION_AAT,
    "Subdivision":         ADM_DIVISION_AAT,
    "Sum":                 ADM_DIVISION_AAT,
    "Traditional Authority": ADM_DIVISION_AAT,
    "Unincorporated Area": ADM_DIVISION_AAT,
    "Unitary Authority":   ADM_DIVISION_AAT,
    "Unknown":             ADM_DIVISION_AAT,
    "Urban":               ADM_DIVISION_AAT,
    "Village block":       ADM_DIVISION_AAT,
    "Ward":                ADM_DIVISION_AAT,
    "Zone":                ADM_DIVISION_AAT,
}


def safe_slug(value: str) -> str:
    """Create a URI-safe slug from a string."""
    return re.sub(r"[^A-Za-z0-9._\-]", "_", value)


def build_place_uri(gadm_id: str) -> str:
    return f"{BASE_URI}{safe_slug(gadm_id)}"


def get_type_classification(sod_type: str) -> tuple[str, str]:
    """Return (aat_uri, label) for a given type string."""
    return TYPE_AAT_MAP.get(sod_type.strip(), ADM_DIVISION_AAT)


def build_linked_art_place(row: dict) -> dict:
    """Convert a single ADM2 CSV row to a Linked Art Place JSON-LD record."""
    country_gadm_id = row["country_gadm_id"].strip()
    country_name    = row["country_name"].strip()
    fod_gadm_id     = row["fod_gadm_id"].strip()
    fod_label       = row["fod_label"].strip()
    sod_gadm_id     = row["sod_gadm_id"].strip()
    pref_label      = row["pref_label"].strip()
    alt_labels      = row["alt_labels"].strip()
    sod_type        = row["sod_type"].strip()

    aat_uri, aat_label = get_type_classification(sod_type)

    # ---- Core record ----
    place = {
        "@context": LINKED_ART_CONTEXT,
        "id": build_place_uri(sod_gadm_id),
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
        # ADM2 → part_of → ADM1, which itself is part_of → country
        "part_of": [
            {
                "id": build_place_uri(fod_gadm_id),
                "type": "Place",
                "_label": fod_label,
                "part_of": [
                    {
                        "id": build_place_uri(country_gadm_id),
                        "type": "Place",
                        "_label": country_name,
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

    # ---- Alternate names (pipe-separated; "NA" = no alternates) ----
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
        "content": sod_gadm_id,
        "classified_as": [
            {
                "id": GADM_ID_AAT,
                "type": "Type",
                "_label": "GADM Identifier"
            }
        ]
    })

    return place


def main():
    repo_root  = Path(__file__).resolve().parents[2]
    input_path = repo_root / "sources" / "gadm" / "gadm41_adm2.csv"
    output_path = repo_root / "output" / "gadm" / "gadm41_adm2_linked_art.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    places = []
    unmapped_types: set[str] = set()

    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sod_type = row["sod_type"].strip()
            if sod_type not in TYPE_AAT_MAP:
                unmapped_types.add(sod_type)
            place = build_linked_art_place(row)
            places.append(place)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(places, f, ensure_ascii=False, indent=2)

    print(f"✓ ADM2 places: {len(places):,} records → {output_path}")
    if unmapped_types:
        print(f"\n⚠  {len(unmapped_types)} unmapped type(s) → fell back to Administrative Division:")
        for t in sorted(unmapped_types):
            print(f"   {repr(t)}")
    print("\nSample ADM2 record:")
    print(json.dumps(places[0], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
