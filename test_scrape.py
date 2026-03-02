import undetected_chromedriver as uc
import time

def get_html(url):
    options = uc.ChromeOptions()
    options.headless = True
    # Add a few stability arguments
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    try:
        # Don't use subprocess=True because it causes WinError 6
        driver = uc.Chrome(options=options)
        driver.get(url)
        # Wait a bit for cloudflare challenge to pass
        time.sleep(10)
        
        content = driver.page_source
        
        if "Just a moment..." in content or "Cloudflare" in content:
            print("Hit Cloudflare challenge page again.")
            print(content[:500])
        else:
            print("Likely bypassed!")
            
        with open('manhuafast.html', 'w', encoding='utf-8') as f:
            f.write(content)
        print("Successfully saved manhuafast.html")
        driver.quit()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    get_html("https://manhuafast.net/")
