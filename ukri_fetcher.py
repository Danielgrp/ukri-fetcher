import os
import requests
import psycopg2

# Read credentials from environment variables for security
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "sslmode": "require"
}

def fetch_ukri_projects(offset=0, limit=100):
    url = f"https://gtr.ukri.org/gtr/api/projects?fetchSize={limit}&start={offset}"
    headers = {'Accept': 'application/vnd.rcuk.gtr.json-v7'}
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    return response.json()

def save_to_db(projects):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for proj in projects.get('project', []):
        try:
            cur.execute("""
                INSERT INTO ukri_projects (project_id, title, status, abstract, funder)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (project_id) DO NOTHING;
            """, (
                proj.get('id'),
                proj.get('title'),
                proj.get('status'),
                proj.get('abstractText'),
                proj.get('funders', [{}])[0].get('name') if proj.get('funders') else None
            ))
        except Exception as e:
            print(f"Insert error for project {proj.get('id')}: {e}")
    conn.commit()
    cur.close()
    conn.close()

def main():
    print("🚀 Starting UKRI data fetch")
    offset = 0
    limit = 100
    max_projects = 1000
    while offset < max_projects:
        print(f"Fetching projects {offset} to {offset + limit - 1}")
        try:
            data = fetch_ukri_projects(offset, limit)
            save_to_db(data)
        except Exception as e:
            print(f"❌ Error at offset {offset}: {e}")
        offset += limit
    print("✅ Fetch complete.")

if __name__ == "__main__":
    main()
