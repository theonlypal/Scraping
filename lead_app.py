import streamlit as st
import pandas as pd
import requests
import sqlite3
import time
from datetime import date, datetime, timedelta
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from dateutil import parser as dateparser
from slugify import slugify

# Database setup
DB_FILE = "lead_calls.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cur = conn.cursor()
cur.execute(
    "CREATE TABLE IF NOT EXISTS leads (osm_id TEXT PRIMARY KEY, outcome TEXT DEFAULT 'Uncalled')"
)
conn.commit()

st.set_page_config(page_title="Hot Leads Dashboard", layout="wide")
st.title("📞 Hot Leads Dashboard")

with st.sidebar:
    st.header("Search Parameters")
    zip_code = st.text_input("U.S. ZIP Code", max_chars=5)
    radius = st.selectbox("Search Radius (miles)", [10, 15, 25], index=0)
    new_within = st.slider("New within N days", min_value=1, max_value=30, value=14)
    uploaded_creds = st.file_uploader("Optional Google Service Account JSON", type="json")

if zip_code and not zip_code.isdigit() or len(zip_code) != 5:
    st.error("Please enter a valid 5-digit U.S. ZIP code")
    st.stop()

@st.cache_data(ttl=86400)
def geocode_zip(zip_code: str):
    geolocator = Nominatim(user_agent="lead_app")
    retries = 3
    for i in range(retries):
        try:
            loc = geolocator.geocode({"postalcode": zip_code, "country": "USA"})
            if loc:
                return loc.latitude, loc.longitude
            return None
        except (GeocoderTimedOut, GeocoderUnavailable):
            time.sleep(1)
    return None

coords = None
if zip_code:
    coords = geocode_zip(zip_code)
    if not coords:
        st.error("Could not geocode ZIP code. Try again later.")
        st.stop()

if coords:
    lat, lon = coords
    st.map(pd.DataFrame({"lat": [lat], "lon": [lon]}))

@st.cache_data(ttl=86400)
def fetch_overpass(lat: float, lon: float, radius_miles: int, days: int):
    radius_m = radius_miles * 1609
    date_threshold = (date.today() - timedelta(days=days)).isoformat()
    query = f"""
[out:json][timeout:25];
(
  node(around:{radius_m},{lat},{lon})[opening_date][!website][phone](if:t["opening_date"]>="{date_threshold}");
  node(around:{radius_m},{lat},{lon})[start_date][!website][phone](if:t["start_date"]>="{date_threshold}");
  node(around:{radius_m},{lat},{lon})[opening_date][!website]["contact:phone"](if:t["opening_date"]>="{date_threshold}");
  node(around:{radius_m},{lat},{lon})[start_date][!website]["contact:phone"](if:t["start_date"]>="{date_threshold}");
  way(around:{radius_m},{lat},{lon})[opening_date][!website][phone](if:t["opening_date"]>="{date_threshold}");
  way(around:{radius_m},{lat},{lon})[start_date][!website][phone](if:t["start_date"]>="{date_threshold}");
  way(around:{radius_m},{lat},{lon})[opening_date][!website]["contact:phone"](if:t["opening_date"]>="{date_threshold}");
  way(around:{radius_m},{lat},{lon})[start_date][!website]["contact:phone"](if:t["start_date"]>="{date_threshold}");
);
out center;"""
    url = "https://overpass-api.de/api/interpreter"
    retries = 3
    for _ in range(retries):
        try:
            resp = requests.post(url, data=query, timeout=60)
            if resp.status_code == 429:
                time.sleep(1)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                time.sleep(1)
                continue
            elif resp.status_code == 400:
                st.error("No data—try a wider radius or shorter date window")
                return None
            else:
                time.sleep(1)
        except requests.exceptions.RequestException:
            time.sleep(1)
    st.error("Rate limit exceeded—please wait and try again.")
    return None

def parse_overpass(data):
    elements = data.get("elements", []) if data else []
    leads = []
    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name")
        if not name:
            continue
        phone = tags.get("phone") or tags.get("contact:phone")
        if not phone:
            continue
        email = tags.get("email") or tags.get("contact:email")
        social = tags.get("facebook") or tags.get("instagram") or tags.get("twitter")
        opening = tags.get("opening_date") or tags.get("start_date")
        try:
            open_dt = dateparser.parse(opening).date()
        except Exception:
            continue
        days_since = (date.today() - open_dt).days
        score = max(0, (30 - days_since))
        if phone:
            score += 10
        if email or social:
            score += 5
        address_parts = [
            tags.get("addr:housenumber", ""),
            tags.get("addr:street", ""),
            tags.get("addr:city", ""),
            tags.get("addr:state", ""),
            tags.get("addr:postcode", ""),
        ]
        address = ", ".join([p for p in address_parts if p])
        osm_id = f"{el['type']}/{el['id']}"
        slug = slugify(f"{name}-{zip_code}")
        demo_link = f"https://yourdomain.com/demo/{slug}"
        leads.append({
            "OSM_ID": osm_id,
            "Name": name,
            "Address": address,
            "Phone": phone,
            "Email/Social": email or social or "",
            "Opening Date": opening,
            "Days Since Opening": days_since,
            "Score": score,
            "Demo Link": demo_link,
        })
    return leads

if coords:
    data = fetch_overpass(lat, lon, radius, new_within)
    if not data:
        st.stop()
    leads = parse_overpass(data)
    if not leads:
        st.error("No data—try a wider radius or shorter date window")
        st.stop()

    df = pd.DataFrame(leads)
    df.sort_values("Score", ascending=False, inplace=True)

    # Remove already called leads
    cur.execute("SELECT osm_id, outcome FROM leads")
    existing = {row[0]: row[1] for row in cur.fetchall()}
    df = df[~df["OSM_ID"].isin(existing.keys())].head(50)
    df["Call Outcome"] = "Uncalled"
    df.reset_index(drop=True, inplace=True)

    st.subheader("Top Leads")
    edited_df = st.data_editor(
        df,
        column_config={
            "Phone": st.column_config.LinkColumn(label="Phone"),
            "Call Outcome": st.column_config.SelectboxColumn(
                "Call Outcome", options=["Uncalled", "Connected", "Voicemail", "No Answer"]
            ),
        },
        disabled=["OSM_ID"],
        hide_index=True,
        use_container_width=True,
        key="leads_editor",
    )

    # Update outcomes in DB
    if "prev_df" not in st.session_state:
        st.session_state["prev_df"] = edited_df
    changed = edited_df[edited_df["Call Outcome"] != st.session_state["prev_df"]["Call Outcome"]]
    for _, row in changed.iterrows():
        cur.execute(
            "INSERT OR REPLACE INTO leads (osm_id, outcome) VALUES (?, ?)",
            (row["OSM_ID"], row["Call Outcome"]),
        )
    if not changed.empty:
        conn.commit()
    st.session_state["prev_df"] = edited_df

    csv_data = edited_df.to_csv(index=False).encode("utf-8")
    st.download_button("Export CSV", csv_data, "leads.csv", "text/csv")

    # Google Sheets export
    def export_gsheets():
        creds = None
        if uploaded_creds is not None:
            creds = uploaded_creds.getvalue()
        elif "gcp_service_account" in st.secrets:
            creds = st.secrets["gcp_service_account"]
        if creds is None:
            st.error("Provide Google service account JSON to enable export.")
            return
        import json
        import gspread
        from gspread_dataframe import set_with_dataframe

        creds_dict = json.loads(creds)
        gc = gspread.service_account_from_dict(creds_dict)
        sh = gc.create(f"Leads_{zip_code}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}")
        sh.share(None, perm_type="anyone", role="writer")
        worksheet = sh.sheet1
        set_with_dataframe(worksheet, edited_df)
        st.success(f"Data exported to {sh.url}")
        st.write(f"[Open Sheet]({sh.url})")

    if st.button("Export to Google Sheets"):
        export_gsheets()

    sms_text = "Hi, check out our demo at {link}".format(link=edited_df.iloc[0]["Demo Link"] if not edited_df.empty else "")
    st.code(sms_text, language=None)
    st.components.v1.html(
        f'<button onclick="navigator.clipboard.writeText(\"{sms_text}\")">Copy SMS Template</button>',
        height=35,
    )

    if st.button("Fetch Latest Leads"):
        geocode_zip.clear()
        fetch_overpass.clear()
        st.experimental_rerun()
