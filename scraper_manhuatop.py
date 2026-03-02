import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup
import re
import concurrent.futures
import time
import os

BASE_URL = "https://manhuatop.org/"
# Manhuatop pagination is typically something like https://manhuatop.org/manhua/page/X/
PAGES_TO_SCRAPE = 30 # Let's scrape first 30 pages to get a good chunk of data

def get_manga_links(page_num):
    url = f"https://manhuatop.org/manhua/page/{page_num}/"
    scraper = cloudscraper.create_scraper()
    
    try:
        response = scraper.get(url, timeout=15)
        if response.status_code != 200:
            print(f"Failed to fetch page {page_num}: Status {response.status_code}")
            return []
            
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=re.compile(r'https://manhuatop\.org/manhua/.+'))
        unique_links = list(set([a.get('href') for a in links if a.get('href').endswith('/')]))
        return unique_links
    except Exception as e:
        print(f"Error fetching page {page_num}: {e}")
        return []

def get_manga_details(url):
    scraper = cloudscraper.create_scraper()
    try:
        response = scraper.get(url, timeout=15)
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Title
        title_el = soup.select_one('.post-title h1')
        title = title_el.text.strip() if title_el else "Unknown"
        
        # Cover
        cover_el = soup.select_one('.summary_image img')
        cover = ""
        if cover_el:
            cover = cover_el.get('data-src') or cover_el.get('src') or ""
            
        # Rating
        rating_el = soup.select_one('#averagerating')
        rating = rating_el.text.strip() if rating_el else "Unknown"
        
        # Status / Type
        status = "Unknown"
        manga_type = "Unknown"
        for item in soup.select('.post-content_item'):
            heading = item.select_one('.summary-heading')
            content = item.select_one('.summary-content')
            if heading and content:
                h_text = heading.text.strip().lower()
                if 'status' in h_text:
                    status = content.text.strip()
                elif 'type' in h_text:
                    manga_type = content.text.strip()
                    
        # Genres
        genres = [g.text.strip() for g in soup.select('.genres-content a')]
        genre_str = ", ".join(genres)
        
        # Chapters
        total_chapters = 0
        url_ajax = f"{url.rstrip('/')}/ajax/chapters/"
        resp_ajax = scraper.post(url_ajax, timeout=10)
        if resp_ajax.status_code == 200:
            ajax_soup = BeautifulSoup(resp_ajax.text, 'html.parser')
            chapters = ajax_soup.select('li.wp-manga-chapter a')
            total_chapters = len(chapters)
            
        return {
            'Title': title,
            'Cover Image': cover,
            'Rating': rating,
            'Type': manga_type,
            'Status': status,
            'Genres': genre_str,
            'Total Chapters': total_chapters,
            'URL': url
        }
    except Exception as e:
        print(f"Error fetching details for {url}: {e}")
        return None

def main():
    print(f"Starting to scrape ManhuaTop (first {PAGES_TO_SCRAPE} pages)...")
    all_links = set()
    
    # 1. Fetch links concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_page = {executor.submit(get_manga_links, page): page for page in range(1, PAGES_TO_SCRAPE + 1)}
        for future in concurrent.futures.as_completed(future_to_page):
            page = future_to_page[future]
            try:
                links = future.result()
                all_links.update(links)
                print(f"Page {page} done, found {len(links)} links.")
            except Exception as exc:
                print(f"Page {page} generated an exception: {exc}")

    print(f"Total unique manga links found: {len(all_links)}")
    
    # 2. Fetch details concurrently
    scraped_data = []
    completed = 0
    total = len(all_links)
    
    print("Extracting details for each manga...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(get_manga_details, url): url for url in all_links}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                if data:
                    scraped_data.append(data)
                completed += 1
                if completed % 10 == 0 or completed == total:
                    print(f"Progress: {completed}/{total} comics scraped.")
            except Exception as exc:
                print(f"URL {url} generated an exception: {exc}")
                
    # 3. Save to Excel
    print("Saving to Excel...")
    df = pd.DataFrame(scraped_data)
    excel_filename = "manhuatop_data.xlsx"
    df.to_excel(excel_filename, index=False)
    print(f"Successfully saved data to {excel_filename}")
    
if __name__ == "__main__":
    main()
