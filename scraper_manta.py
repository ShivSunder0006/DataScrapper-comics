import requests
import pandas as pd
import time
import concurrent.futures

BASE_API = "https://manta.net"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Referer': 'https://manta.net/en',
    'Accept-Language': 'en-US,en;q=0.9',
}


def get_trending_series():
    """Fetch trending/popular series from search API."""
    url = f"{BASE_API}/manta/v1/search/series/trending?lang=en"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            return {item["id"]: item for item in data}
    except Exception as e:
        print(f"Error fetching trending: {e}")
    return {}


def get_ranked_series():
    """Fetch ranked series from the home/rank endpoint (contains seriesMap with details)."""
    url = f"{BASE_API}/front/v1/home/rank?lang=en&nb=false"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            series_map = data.get("seriesMap", {})
            items_map = data.get("itemsMap", {})

            # Extract ranking info
            rankings = {}
            for category, info in items_map.items():
                for item in info.get("items", []):
                    sid = item["id"]
                    if sid not in rankings:
                        rankings[sid] = {
                            "rank": item.get("rank"),
                            "rank_category": category,
                            "rank_change": item.get("rankChange", 0),
                            "view_count": item.get("cnt", 0),
                        }

            return series_map, rankings
    except Exception as e:
        print(f"Error fetching ranked series: {e}")
    return {}, {}


def get_daily_series(day="mon"):
    """Fetch daily release series for a specific day."""
    url = f"{BASE_API}/front/v1/home/daily?lang=en&nb=false&subTab={day}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            series_map = data.get("seriesMap", {})
            return series_map
    except Exception as e:
        print(f"Error fetching daily ({day}): {e}")
    return {}


def get_completed_series():
    """Fetch completed series."""
    url = f"{BASE_API}/front/v1/home/202505?lang=en&nb=false&tab=completed"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            series_map = data.get("seriesMap", {})
            return series_map
    except Exception as e:
        print(f"Error fetching completed: {e}")
    return {}


def get_series_details(series_id):
    """Fetch detailed info for a single series."""
    url = f"{BASE_API}/front/v1/series/{series_id}?lang=en"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("data", {})
    except Exception as e:
        print(f"Error fetching series {series_id}: {e}")
    return None


def extract_series_info(series_id, series_data, ranking_info=None):
    """Extract structured info from a series data object."""
    data = series_data.get("data", {})
    derived = series_data.get("derived", {})

    # Title
    title_obj = data.get("title", {})
    title = title_obj.get("en", title_obj.get("ko", "Unknown"))

    # Tags/Genres
    tags = data.get("tags", [])
    genres = [t["name"].get("en", "") for t in tags if t.get("name", {}).get("en")]
    genres_str = ", ".join(genres)

    # Age rating
    age_rating_obj = data.get("ageRating", {})
    age_rating = age_rating_obj.get("rate", "N/A")
    if age_rating != "N/A":
        age_rating = f"{age_rating}+"

    # Status
    is_completed = data.get("isCompleted", False)
    release_state = derived.get("releaseState", 0)
    status = "Completed" if is_completed else ("Ongoing" if release_state == 20 else "Other")

    # Release schedule
    schedule = derived.get("releaseSchedule1", {}).get("text", "N/A")

    # Episode info
    first_ep = series_data.get("firstEpisode", {})
    latest_ep = series_data.get("latestEpisode", {})
    episode_count = series_data.get("episodeCount", 0)
    if not episode_count and latest_ep:
        episode_count = latest_ep.get("ord", 0)

    first_ep_title = first_ep.get("data", {}).get("title", "N/A") if first_ep else "N/A"
    latest_ep_title = latest_ep.get("data", {}).get("title", "N/A") if latest_ep else "N/A"
    latest_ep_date = latest_ep.get("openAt", "N/A") if latest_ep else "N/A"
    if latest_ep_date != "N/A":
        latest_ep_date = latest_ep_date.split("T")[0]

    # Cover image
    image_data = series_data.get("image", {})
    cover_url = ""
    for key in ["1280x1840_720", "1280x1840_480", "1440x1440_720", "1440x1440_480"]:
        if key in image_data:
            cover_url = image_data[key].get("downloadUrl", "")
            break

    # Access type
    lock_type = derived.get("lockType", 0)
    lock_map = {0: "Free", 30: "Gem (Buy)", 40: "Free Pass / Unlimited"}
    access_type = lock_map.get(lock_type, f"Type {lock_type}")

    # Billboard subtitle (short genre description)
    billboard = derived.get("billboardSubText", "")

    # Open date
    open_at = series_data.get("openAt", "N/A")
    if open_at != "N/A":
        open_at = open_at.split("T")[0]

    # Series URL
    slug = title.lower().replace(" ", "-").replace("'", "").replace(",", "")
    series_url = f"https://manta.net/en/series/{slug}?seriesId={series_id}"

    result = {
        "ID": series_id,
        "Title": title,
        "Genres/Tags": genres_str,
        "Billboard Subtitle": billboard,
        "Age Rating": age_rating,
        "Status": status,
        "Release Schedule": schedule,
        "Total Episodes": episode_count,
        "First Episode": first_ep_title,
        "Latest Episode": latest_ep_title,
        "Latest Episode Date": latest_ep_date,
        "Published Date": open_at,
        "Access Type": access_type,
        "Cover Image URL": cover_url,
        "URL": series_url,
    }

    # Add ranking info if available
    if ranking_info:
        result["Rank"] = ranking_info.get("rank", "N/A")
        result["Rank Category"] = ranking_info.get("rank_category", "N/A")
        result["Rank Change"] = ranking_info.get("rank_change", 0)
        result["View Count"] = ranking_info.get("view_count", 0)

    return result


def main():
    print("=" * 60)
    print("  Manta.net Series Scraper")
    print("=" * 60)

    all_series = {}  # id -> series_data
    rankings = {}

    # Phase 1: Collect series from all endpoints
    print("\n[1/4] Fetching ranked series...")
    series_map, rankings = get_ranked_series()
    for sid, sdata in series_map.items():
        all_series[int(sid)] = sdata
    print(f"  Found {len(series_map)} ranked series")

    print("\n[2/4] Fetching trending series...")
    trending = get_trending_series()
    for sid, sdata in trending.items():
        if sid not in all_series:
            all_series[sid] = sdata
    print(f"  Found {len(trending)} trending series")

    print("\n[3/4] Fetching daily series (all days)...")
    days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    for day in days:
        daily_map = get_daily_series(day)
        for sid, sdata in daily_map.items():
            if int(sid) not in all_series:
                all_series[int(sid)] = sdata
        print(f"  {day.upper()}: {len(daily_map)} series")
        time.sleep(0.5)

    print("\n[4/4] Fetching completed series...")
    completed_map = get_completed_series()
    for sid, sdata in completed_map.items():
        if int(sid) not in all_series:
            all_series[int(sid)] = sdata
    print(f"  Found {len(completed_map)} completed series")

    print(f"\n{'=' * 60}")
    print(f"  Total unique series collected: {len(all_series)}")
    print(f"{'=' * 60}")

    # Phase 2: Try to fetch additional details for series that need it
    print("\nFetching additional details for series...")
    series_needing_details = []
    for sid, sdata in all_series.items():
        # If data is minimal (e.g. from trending which already has full data), skip
        if "episodeCount" not in sdata and "latestEpisode" not in sdata:
            series_needing_details.append(sid)

    if series_needing_details:
        print(f"  Need details for {len(series_needing_details)} series...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_map = {
                executor.submit(get_series_details, sid): sid
                for sid in series_needing_details
            }
            for future in concurrent.futures.as_completed(future_map):
                sid = future_map[future]
                try:
                    detail = future.result()
                    if detail:
                        all_series[sid] = detail
                except Exception as e:
                    print(f"  Error getting details for {sid}: {e}")
    else:
        print("  All series already have sufficient data.")

    # Phase 3: Extract structured data
    print("\nExtracting structured data...")
    extracted = []
    for sid, sdata in all_series.items():
        rank_info = rankings.get(str(sid)) or rankings.get(sid)
        info = extract_series_info(sid, sdata, rank_info)
        extracted.append(info)

    # Sort by rank (ranked first), then by title
    extracted.sort(key=lambda x: (
        x.get("Rank", 999) if isinstance(x.get("Rank"), int) else 999,
        x.get("Title", "")
    ))

    # Phase 4: Export to Excel
    print(f"\nExporting {len(extracted)} series to Excel...")
    df = pd.DataFrame(extracted)

    # Reorder columns
    col_order = [
        "ID", "Title", "Genres/Tags", "Billboard Subtitle", "Age Rating",
        "Status", "Release Schedule", "Total Episodes", "First Episode",
        "Latest Episode", "Latest Episode Date", "Published Date",
        "Access Type", "Rank", "Rank Category", "Rank Change",
        "View Count", "Cover Image URL", "URL"
    ]
    # Only include columns that exist
    col_order = [c for c in col_order if c in df.columns]
    df = df[col_order]

    excel_filename = "manta_series_data.xlsx"
    df.to_excel(excel_filename, index=False, engine="openpyxl")
    print(f"\n{'=' * 60}")
    print(f"  SUCCESS! Data exported to: {excel_filename}")
    print(f"  Total series: {len(extracted)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
