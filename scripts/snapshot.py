"""
Golden Hour — Daily Forecast Snapshot
Runs every morning, captures forecasts for all locations/models,
stores in Supabase for later accuracy verification.
"""

import os, json, requests
from datetime import datetime, timedelta, timezone

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

LOCATIONS = [
    {"name": "Lake Bled",           "lat": 46.3683, "lon": 14.1146, "elev": 475,  "fog_affinity": "high"},
    {"name": "Lake Bohinj",         "lat": 46.2796, "lon": 13.8815, "elev": 526,  "fog_affinity": "high"},
    {"name": "Ljubljana",           "lat": 46.0569, "lon": 14.5058, "elev": 295,  "fog_affinity": "medium"},
    {"name": "Soca Valley (Bovec)", "lat": 46.3378, "lon": 13.6526, "elev": 434,  "fog_affinity": "medium"},
    {"name": "Lago Predil (Italy)", "lat": 46.4188, "lon": 13.5651, "elev": 964,  "fog_affinity": "low"},
    {"name": "Vrsic Pass",          "lat": 46.4329, "lon": 13.7431, "elev": 1611, "fog_affinity": "none"},
    {"name": "Gabrska Gora",        "lat": 46.1374, "lon": 14.2053, "elev": 890,  "fog_affinity": "low"},
    {"name": "St Thomas Church",    "lat": 46.1751, "lon": 14.2276, "elev": 720,  "fog_affinity": "low"},
    {"name": "Kranjska Gora",       "lat": 46.4857, "lon": 13.7863, "elev": 810,  "fog_affinity": "low"},
]

MODELS = {
    "ecmwf":  "",               # default best blend
    "icon":   "icon_seamless",
    "gfs":    "gfs_seamless",
    "metno":  "metno_seamless",
}

WINDOWS = {
    "sunrise": (5, 9),
    "sunset":  (17, 21),
}

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}


def fetch_forecast(lat, lon, model_param):
    params = (
        f"latitude={lat}&longitude={lon}"
        f"&hourly=cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,"
        f"precipitation_probability,visibility,windspeed_10m,dewpoint_2m,temperature_2m"
        f"&forecast_days=4&timezone=Europe/Ljubljana"
    )
    if model_param:
        params += f"&models={model_param}"
    r = requests.get(f"https://api.open-meteo.com/v1/forecast?{params}", timeout=20)
    r.raise_for_status()
    return r.json()


def avg_window(hourly, times, date_str, start_h, end_h):
    """Average hourly values over a time window for a specific date."""
    idxs = [
        i for i, t in enumerate(times)
        if t.startswith(date_str) and start_h <= int(t[11:13]) <= end_h
    ]
    if not idxs:
        return None

    def avg(key):
        vals = [hourly[key][i] for i in idxs if hourly[key][i] is not None]
        return round(sum(vals) / len(vals), 2) if vals else None

    return {
        "cloud_total": avg("cloudcover"),
        "cloud_low":   avg("cloudcover_low"),
        "cloud_mid":   avg("cloudcover_mid"),
        "cloud_high":  avg("cloudcover_high"),
        "precip":      avg("precipitation_probability"),
        "visibility":  avg("visibility"),
        "windspeed":   avg("windspeed_10m"),
        "dewpoint":    avg("dewpoint_2m"),
        "temperature": avg("temperature_2m"),
    }


def photo_score(d):
    """Colour photo score — mirrors app logic."""
    if not d:
        return 50
    score = 0
    high, mid, low = d["cloud_high"] or 0, d["cloud_mid"] or 0, d["cloud_low"] or 0
    precip = d["precip"] or 0
    vis = d["visibility"]

    good_cloud = high * 0.6 + mid * 0.4
    if   20 <= good_cloud <= 70: score += 50
    elif 70 <  good_cloud <= 85: score += 30
    elif 10 <= good_cloud < 20:  score += 25
    elif good_cloud <= 10:       score += 10
    else:                        score += 15

    if   low <= 10: score += 30
    elif low <= 20: score += 20
    elif low <= 35: score += 8

    if   precip <= 10: score += 20
    elif precip <= 25: score += 10
    elif precip <= 45: score += 3

    if low > 70:    score -= 25
    elif low > 50:  score -= 12
    if precip > 70: score -= 20
    elif precip > 45: score -= 10
    if vis is not None:
        if vis < 500:  score -= 20
        elif vis < 1500: score -= 5

    return max(0, min(100, round(score)))


def fog_score(d, fog_affinity):
    """Fog photo score — mirrors app logic."""
    if not d or fog_affinity == "none":
        return 0
    score = 0
    vis   = d["visibility"]
    low   = d["cloud_low"] or 0
    precip = d["precip"] or 0
    wind  = d["windspeed"]
    temp  = d["temperature"]
    dew   = d["dewpoint"]

    if vis is not None:
        if   300 <= vis <= 800:  score += 45
        elif vis <= 2000:        score += 35
        elif vis <= 4000:        score += 18
        elif vis <= 8000:        score += 6
        else:                    score += 0
    else:
        score += 8

    if   low >= 60: score += 25
    elif low >= 35: score += 15
    elif low >= 15: score += 6

    if temp is not None and dew is not None:
        spread = temp - dew
        if   spread <= 1: score += 20
        elif spread <= 2: score += 14
        elif spread <= 4: score += 6

    if wind is not None:
        if   wind <= 5:  score += 10
        elif wind <= 10: score += 4
        elif wind > 20:  score -= 10

    if   precip > 40: score -= 20
    elif precip > 20: score -= 8

    boosts = {"high": 10, "medium": 4, "low": -5}
    score += boosts.get(fog_affinity, 0)

    return max(0, min(100, round(score)))


def save_snapshots(records):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/forecast_snapshots",
        headers=HEADERS,
        json=records,
        timeout=30,
    )
    if r.status_code not in (200, 201):
        print(f"  Supabase error {r.status_code}: {r.text}")
    else:
        print(f"  Saved {len(records)} records")


def main():
    captured_at = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).date()

    # Capture forecasts for tomorrow and day after
    forecast_dates = [
        (today + timedelta(days=1)).isoformat(),
        (today + timedelta(days=2)).isoformat(),
        (today + timedelta(days=3)).isoformat(),
    ]

    all_records = []

    for loc in LOCATIONS:
        print(f"\n{loc['name']}")
        for model_key, model_param in MODELS.items():
            print(f"  {model_key}...", end=" ")
            try:
                data = fetch_forecast(loc["lat"], loc["lon"], model_param)
                times = data["hourly"]["time"]

                for fdate in forecast_dates:
                    lead_days = (
                        datetime.fromisoformat(fdate).date() - today
                    ).days

                    for window_name, (start_h, end_h) in WINDOWS.items():
                        w = avg_window(data["hourly"], times, fdate, start_h, end_h)
                        if not w:
                            continue

                        record = {
                            "captured_at":    captured_at,
                            "forecast_date":  fdate,
                            "lead_days":      lead_days,
                            "location":       loc["name"],
                            "lat":            loc["lat"],
                            "lon":            loc["lon"],
                            "elev":           loc["elev"],
                            "fog_affinity":   loc["fog_affinity"],
                            "model":          model_key,
                            "time_window":         window_name,
                            "cloud_total":    w["cloud_total"],
                            "cloud_low":      w["cloud_low"],
                            "cloud_mid":      w["cloud_mid"],
                            "cloud_high":     w["cloud_high"],
                            "precip":         w["precip"],
                            "visibility":     w["visibility"],
                            "windspeed":      w["windspeed"],
                            "dewpoint":       w["dewpoint"],
                            "temperature":    w["temperature"],
                            "photo_score":    photo_score(w),
                            "fog_score":      fog_score(w, loc["fog_affinity"]),
                        }
                        all_records.append(record)

                print("ok")
            except Exception as e:
                print(f"ERROR: {e}")

    print(f"\nSaving {len(all_records)} total records...")
    # Save in batches of 100
    for i in range(0, len(all_records), 100):
        save_snapshots(all_records[i:i+100])

    print("Snapshot complete.")


if __name__ == "__main__":
    main()
