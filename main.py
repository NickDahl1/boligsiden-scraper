import os
from datetime import date

import pandas as pd
import requests
from supabase import create_client


def fetch_properties_from_municipality(municipality: str, per_page: int = 1000):
    url = "https://api.boligsiden.dk/search/cases"
    params = {"municipalities": municipality, "per_page": per_page}
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get("cases", [])
    except Exception as e:
        print(f"❌ fejl ved {municipality}: {e}")
        return []


def flatten_entry(entry, update_date=None):
    if update_date is None:
        update_date = str(date.today())

    address = entry.get("address", {}) or {}
    time_on_market = entry.get("timeOnMarket", {}) or {}

    return {
        "kommune": address.get("municipality", {}).get("name")
        if isinstance(address.get("municipality"), dict)
        else None,
        "salgspris_kr": entry.get("priceCash"),
        "pris_per_m2_kr": entry.get("perAreaPrice"),
        "dage_paa_marked_nu": time_on_market.get("current", {}).get("days")
        if isinstance(time_on_market.get("current"), dict)
        else None,
        "boligareal_m2": address.get("livingArea"),
        "opdateringsdato": update_date,
        "adresse": address.get("roadName"),
        "husnummer": address.get("houseNumber"),
        "postnummer": address.get("zipCode"),
        "by": address.get("city", {}).get("name")
        if isinstance(address.get("city"), dict)
        else None,
    }


def scrape_sydjylland_boliger():
    municipalities = [
        "Billund",
        "Esbjerg",
        "Varde",
        "Vejen",
        "Vejle",
        "Kolding",
        "Hedensted",
        "Horsens",
        "Fredericia",
        "Middelfart",
    ]

    all_properties = []
    print(f"🚀 henter boliger fra {len(municipalities)} kommuner...")

    for mun in municipalities:
        props = fetch_properties_from_municipality(mun)
        all_properties.extend(props)
        print(f"✅ {mun}: {len(props)} boliger")

    print(f"✅ total hentet: {len(all_properties)} boliger")

    flat_data = [flatten_entry(p) for p in all_properties]
    df = pd.DataFrame(flat_data)

    numeric_cols = ["salgspris_kr", "pris_per_m2_kr", "dage_paa_marked_nu", "boligareal_m2"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def calculate_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    print("📊 beregner daglig statistik...")
    today = date.today()

    stats = {
        "Dato": str(today),  # matcher din tabel: text
        "Total_Antal_Boliger": int(len(df)),
        "Total_Gns_Pris_kr": float(df["salgspris_kr"].mean()) if "salgspris_kr" in df.columns else None,
        "Total_Median_Pris_kr": float(df["salgspris_kr"].median()) if "salgspris_kr" in df.columns else None,
        "Total_Gns_M2_Pris_kr": float(df["pris_per_m2_kr"].mean()) if "pris_per_m2_kr" in df.columns else None,
        "Total_Gns_Liggetid_dage": float(df["dage_paa_marked_nu"].mean()) if "dage_paa_marked_nu" in df.columns else None,
        "Total_Samlet_Udbud_mia_kr": float(df["salgspris_kr"].sum() / 1_000_000_000)
        if "salgspris_kr" in df.columns
        else None,
    }

    if "kommune" in df.columns:
        kommuner = df["kommune"].dropna().unique()
        for kommune in sorted(kommuner):
            k_df = df[df["kommune"] == kommune]
            prefix = str(kommune).replace(" ", "_")  # fx "Billund"

            stats[f"{prefix}_Antal"] = int(len(k_df))
            stats[f"{prefix}_Gns_Pris"] = float(k_df["salgspris_kr"].mean()) if "salgspris_kr" in k_df.columns else None
            stats[f"{prefix}_Gns_M2_Pris"] = float(k_df["pris_per_m2_kr"].mean()) if "pris_per_m2_kr" in k_df.columns else None
            stats[f"{prefix}_Gns_Liggetid"] = float(k_df["dage_paa_marked_nu"].mean()) if "dage_paa_marked_nu" in k_df.columns else None

    stats_df = pd.DataFrame([stats])

    # Runde kun numeriske kolonner
    num_cols = stats_df.select_dtypes(include=["float64", "int64"]).columns
    stats_df[num_cols] = stats_df[num_cols].round(1)

    return stats_df


def _row_to_payload(stats_df: pd.DataFrame) -> dict:
    """
    Konverterer 1 række stats_df til dict, og sikrer at NaN -> None.
    """
    row = stats_df.iloc[0].to_dict()

    # pandas kan have NaN (float), som Supabase ikke kan lide
    for k, v in list(row.items()):
        if pd.isna(v):
            row[k] = None

    return row


def upsert_daily_stats_to_supabase(stats_df: pd.DataFrame):
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        raise RuntimeError(
            "mangler env vars: SUPABASE_URL og/eller SUPABASE_SERVICE_ROLE_KEY"
        )

    client = create_client(supabase_url, supabase_key)

    payload = _row_to_payload(stats_df)

    # Upsert på 'Dato' kræver at du har unique constraint/index på kolonnen Dato.
    # Hvis du IKKE har unique på Dato endnu, så opret den i Supabase (Indexes -> Unique).
    res = (
        client.table("daily_stats")
        .upsert(payload, on_conflict="Dato")
        .execute()
    )

    if getattr(res, "error", None):
        raise RuntimeError(f"supabase upsert fejl: {res.error}")

    print("✅ upsert til supabase: daily_stats ok")


def main():
    df = scrape_sydjylland_boliger()
    stats_df = calculate_daily_stats(df)
    upsert_daily_stats_to_supabase(stats_df)


if __name__ == "__main__":
    main()
