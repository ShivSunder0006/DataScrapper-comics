import requests
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures
import time

BASE_URL = "https://nyraxmanga.com/manga/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

def get_manga_links_from_page(page_num):
    url = f"{BASE_URL}?page={page_num}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return []
        soup = BeautifulSoup(response.content, 'html.parser')
        manga_links = soup.select('.bsx > a')
        return [a.get('href') for a in manga_links if a.get('href')]
    except Exception as e:
        print(f"Error fetching page {page_num}: {e}")
        return []

def scrape_manga_details(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')

        # Title
        title_el = soup.select_one('h1.entry-title')
        title = title_el.text.strip() if title_el else "Unknown"

        # Cover Image
        cover_el = soup.select_one('.thumb img')
        cover_img = "Unknown"
        if cover_el:
            cover_img = cover_el.get('src') or cover_el.get('data-src') or "Unknown"

        # Genres
        genres = [g.text.strip() for g in soup.select('.mgen a')]
        genres_str = ", ".join(genres) if genres else "Unknown"

        # Status and Type
        status = "Unknown"
        manga_type = "Unknown"
        imptdt = soup.select('.imptdt')
        for item in imptdt:
            text = item.text.strip().lower()
            if 'status' in text:
                status = item.text.replace('Status', '').replace(':', '').strip()
            elif 'type' in text:
                manga_type = item.text.replace('Type', '').replace(':', '').strip()

        # Total Chapters
        chapters = soup.select('#chapterlist .eph-num a')
        total_chapters = len(chapters)

        return {
            "Title": title,
            "Cover Image URL": cover_img,
            "Genres": genres_str,
            "Total Chapters": total_chapters,
            "Status": status,
            "Type": manga_type,
            "URL": url
        }
    except Exception as e:
        print(f"Error extracting details from {url}: {e}")
        return None

def main():
    print("Starting NYRAX Manga Scraper...")
    all_manga_links = []
    page = 1
    
    # Phase 1: Collect ALL manga URLs from pagination
    while True:
        print(f"Fetching manga links from page {page}...")
        links = get_manga_links_from_page(page)
        if not links:
            print(f"No more manga found or encountered an error at page {page}. Stopping pagination.")
            break
        
        # Additional check to break early if links repeat
        if all_manga_links and links[0] in all_manga_links:
             print(f"Reached repeated content at page {page}. Stopping pagination.")
             break

        all_manga_links.extend(links)
        page += 1
        time.sleep(1) # Delay between pages to be polite
        
    # Remove duplicates
    all_manga_links = list(set(all_manga_links))
    print(f"\nTotal unique manga links found: {len(all_manga_links)}")
    
    # Phase 2: Scrape details using ThreadPoolExecutor
    extracted_data = []
    print("\nStarting details extraction...")
    
    # Using a small number of workers to not overwhelm the server
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks
        future_to_url = {executor.submit(scrape_manga_details, url): url for url in all_manga_links}
        
        for idx, future in enumerate(concurrent.futures.as_completed(future_to_url), start=1):
            url = future_to_url[future]
            try:
                data = future.result()
                if data:
                    extracted_data.append(data)
                
                # Progress logging
                if idx % 50 == 0 or idx == len(all_manga_links):
                    print(f"Processed {idx}/{len(all_manga_links)} manga...")
            except Exception as exc:
                print(f"{url} generated an exception: {exc}")

    print("\n--- Scraping Complete ---")
    print(f"Successfully scraped details for {len(extracted_data)} manga.")
    
    # Phase 3: Export to Excel
    if extracted_data:
        df = pd.DataFrame(extracted_data)
        excel_filename = "nyraxmanga_comics.xlsx"
        df.to_excel(excel_filename, index=False)
        print(f"Data successfully exported to {excel_filename}")
    else:
        print("No data extracted. Excel file not created.")

if __name__ == "__main__":
    main()
