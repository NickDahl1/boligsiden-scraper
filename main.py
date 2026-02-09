import os
from datetime import date
import requests
import pandas as pd


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

    flattened = {
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
    return flattened


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


def calculate_daily_stats(df: pd.DataFrame):
    print("📊 beregner daglig statistik...")
    today = date.today()

    stats = {
        "dato": str(today),
        "total_antal_boliger": int(len(df)),
        "total_gns_pris_kr": float(df["salgspris_kr"].mean()) if "salgspris_kr" in df.columns else None,
        "total_median_pris_kr": float(df["salgspris_kr"].median()) if "salgspris_kr" in df.columns else None,
        "total_gns_m2_pris_kr": float(df["pris_per_m2_kr"].mean()) if "pris_per_m2_kr" in df.columns else None,
        "total_gns_liggetid_dage": float(df["dage_paa_marked_nu"].mean()) if "dage_paa_marked_nu" in df.columns else None,
        "total_samlet_udbud_mia_kr": float(df["salgspris_kr"].sum() / 1_000_000_000)
        if "salgspris_kr" in df.columns
        else None,
    }

    if "kommune" in df.columns:
        kommuner = df["kommune"].dropna().unique()
        for kommune in sorted(kommuner):
            k_df = df[df["kommune"] == kommune]
            prefix = str(kommune).replace(" ", "_").lower()

            stats[f"{prefix}_antal"] = int(len(k_df))
            stats[f"{prefix}_gns_pris"] = float(k_df["salgspris_kr"].mean()) if "salgspris_kr" in k_df.columns else None
            stats[f"{prefix}_gns_m2_pris"] = float(k_df["pris_per_m2_kr"].mean()) if "pris_per_m2_kr" in k_df.columns else None
            stats[f"{prefix}_gns_liggetid"] = float(k_df["dage_paa_marked_nu"].mean()) if "dage_paa_marked_nu" in k_df.columns else None

    stats_df = pd.DataFrame([stats])

    # rund kun numeriske kolonner
    numeric_cols = stats_df.select_dtypes(include=["float64", "int64"]).columns
    stats_df[numeric_cols] = stats_df[numeric_cols].round(1)

    return stats_df


def update_history_file(new_stats_df: pd.DataFrame, filename: str = "bolig_statistik_historik.csv"):
    if os.path.isfile(filename):
        try:
            history_df = pd.read_csv(filename)
            today_str = str(date.today())

            if "dato" in history_df.columns:
                if today_str in history_df["dato"].astype(str).values:
                    print(f"⚠️ data for {today_str} findes allerede. overskriver dagens række.")
                    history_df = history_df[history_df["dato"].astype(str) != today_str]

            updated_df = pd.concat([history_df, new_stats_df], ignore_index=True)
            if "dato" in updated_df.columns:
                updated_df = updated_df.sort_values("dato")
        except Exception as e:
            print(f"❌ fejl: {e}. opretter ny fil.")
            updated_df = new_stats_df
    else:
        print(f"🆕 opretter ny historik-fil: {filename}")
        updated_df = new_stats_df

    updated_df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"✅ historik gemt i: {filename}")
    return updated_df


def main():
    df = scrape_sydjylland_boliger()
    stats_df = calculate_daily_stats(df)
    update_history_file(stats_df)


if __name__ == "__main__":
    main()
