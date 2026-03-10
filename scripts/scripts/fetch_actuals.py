"""
Golden Hour — Fetch Actuals
Runs daily, looks for forecast snapshots whose date has now passed,
fetches real observed weather from Open-Meteo archive, stores actuals,
then computes accuracy scores.
"""

import os, requests
from datetime import datetime, timedelta, timezone

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

WINDOWS = {
    "sunrise": (5, 9),
    "sunset":  (17, 21),
}


def get_pending_actuals():
    """Find unique location+date combos that have passed but have no actual yet."""
    yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/forecast_snapshots"
        f"?select=location,lat,lon,forecast_date"
        f"&forecast_date=lte.{yesterday}"
        f"&actual_fetched=eq.false"
        f"&limit=500",
        headers=HEADERS, timeout=20
    )
    r.raise_for_status()
    rows = r.json()
    # Deduplicate by location+date
    seen = set()
    unique = []
    for row in rows:
        key = (row["location"], row["forecast_date"])
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


def fetch_actual(lat, lon, date_str):
    """Fetch ERA5 reanalysis / observed data from Open-Meteo archive."""
    params = (
        f"latitude={lat}&longitude={lon}"
        f"&hourly=cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,"
        f"precipitation,visibility,windspeed_10m,dewpoint_2m,temperature_2m"
        f"&start_date={date_str}&end_date={date_str}"
        f"&timezone=Europe/Ljubljana"
    )
    r = requests.get(
        f"https://archive-api.open-meteo.com/v1/archive?{params}",
        timeout=20
    )
    r.raise_for_status()
    return r.json()


def avg_window_actual(hourly, times, start_h, end_h):
    idxs = [i for i, t in enumerate(times) if start_h <= int(t[11:13]) <= end_h]
    if not idxs:
        return None

    def avg(key):
        arr = hourly.get(key, [])
        vals = [arr[i] for i in idxs if i < len(arr) and arr[i] is not None]
        return round(sum(vals) / len(vals), 2) if vals else None

    return {
        "actual_cloud_total": avg("cloudcover"),
        "actual_cloud_low":   avg("cloudcover_low"),
        "actual_cloud_mid":   avg("cloudcover_mid"),
        "actual_cloud_high":  avg("cloudcover_high"),
        "actual_precip":      avg("precipitation"),       # mm, not probability
        "actual_visibility":  avg("visibility"),
        "actual_windspeed":   avg("windspeed_10m"),
        "actual_dewpoint":    avg("dewpoint_2m"),
        "actual_temperature": avg("temperature_2m"),
    }


def photo_score_from_actuals(d):
    """Same scoring logic but applied to actual observed data."""
    if not d:
        return None
    score = 0
    high   = d["actual_cloud_high"] or 0
    mid    = d["actual_cloud_mid"]  or 0
    low    = d["actual_cloud_low"]  or 0
    precip = d["actual_precip"]     or 0  # actual mm — convert to rough probability
    # Treat >2mm as heavy, >0.5mm as moderate
    precip_proxy = min(100, precip * 30)
    vis = d["actual_visibility"]

    good_cloud = high * 0.6 + mid * 0.4
    if   20 <= good_cloud <= 70: score += 50
    elif 70 <  good_cloud <= 85: score += 30
    elif 10 <= good_cloud < 20:  score += 25
    elif good_cloud <= 10:       score += 10
    else:                        score += 15

    if   low <= 10: score += 30
    elif low <= 20: score += 20
    elif low <= 35: score += 8

    if   precip_proxy <= 10: score += 20
    elif precip_proxy <= 25: score += 10
    elif precip_proxy <= 45: score += 3

    if low > 70:         score -= 25
    elif low > 50:       score -= 12
    if precip_proxy > 70:  score -= 20
    elif precip_proxy > 45: score -= 10
    if vis is not None:
        if vis < 500:    score -= 20
        elif vis < 1500: score -= 5

    return max(0, min(100, round(score)))


def save_actuals(location, forecast_date, window, actuals, actual_photo_score):
    """Update all snapshot rows matching this location/date/window with actuals."""
    update = {
        **actuals,
        "actual_photo_score": actual_photo_score,
        "actual_fetched": True,
    }
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/forecast_snapshots"
        f"?location=eq.{requests.utils.quote(location)}"
        f"&forecast_date=eq.{forecast_date}"
        f"&time_window=eq.{window}",
        headers=HEADERS,
        json=update,
        timeout=20
    )
    if r.status_code not in (200, 201, 204):
        print(f"    Update error {r.status_code}: {r.text[:200]}")


def main():
    print("Fetching pending actuals...")
    pending = get_pending_actuals()
    print(f"Found {len(pending)} location/date combos to process")

    processed = set()

    for row in pending:
        loc = row["location"]
        lat = row["lat"]
        lon = row["lon"]
        fdate = row["forecast_date"]
        key = (loc, fdate)

        if key in processed:
            continue
        processed.add(key)

        print(f"\n{loc} — {fdate}")
        try:
            data = fetch_actual(lat, lon, fdate)
            times = data["hourly"]["time"]

            for window_name, (start_h, end_h) in WINDOWS.items():
                actuals = avg_window_actual(data["hourly"], times, start_h, end_h)
                if not actuals:
                    continue
                actual_ps = photo_score_from_actuals(actuals)
                save_actuals(loc, fdate, window_name, actuals, actual_ps)
                print(f"  {window_name}: cloud_low={actuals['actual_cloud_low']}% "
                      f"vis={actuals['actual_visibility']}m "
                      f"photo_score={actual_ps}")

        except Exception as e:
            print(f"  ERROR: {e}")

    print("\nActuals fetch complete.")


if __name__ == "__main__":
    main()
