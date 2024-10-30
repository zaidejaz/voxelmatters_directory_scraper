import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import sys

def get_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    return uc.Chrome(options=options)

def get_soup(driver, url, max_retries=5, delay=5):
    for attempt in range(max_retries):
        try:
            driver.get(url)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            return BeautifulSoup(driver.page_source, 'html.parser')
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

def clean_text(text):
    return ' '.join(text.split())

def extract_company_details(driver, url):
    soup = get_soup(driver, url)
    if not soup:
        return None
    
    details = {}
    
    # Extract basic details
    table = soup.find('table', class_='company-address')
    if table:
        for row in table.find_all('tr', class_='detail'):
            label = clean_text(row.find('td', class_='detail-label').text)
            value_cell = row.find('td', class_='detail')
            
            if label == 'Website':
                link = value_cell.find('a')
                value = link['href'].strip() if link else ' '
            elif label == 'E-mail':
                link = value_cell.find('a')
                value = link['href'].replace('mailto:', '').strip() if link else ' '
            else:
                value = clean_text(value_cell.text)
            
            details[label] = value

    # Extract profile description
    profile_desc_div = soup.find('div', class_='profile-description')
    if profile_desc_div:
        content_div = profile_desc_div.find('div', class_='block-content')
        if content_div:
            details['Profile Description'] = clean_text(content_div.text)
    else:
        details['Profile Description'] = ' '

    # Extract short description
    short_desc_div = soup.find('div', class_='description')
    if short_desc_div:
        details['Short Description'] = clean_text(short_desc_div.text)
    else:
        details['Short Description'] = ' '

    # Extract categories
    breadcrumbs = soup.find('div', class_='breadcrumbs')
    if breadcrumbs:
        category_links = breadcrumbs.find_all('a', class_='categories-company-new')[1:-1]  # Exclude 'Home' and company name
        categories = [clean_text(link.text) for link in category_links]
        details['Categories'] = ', '.join(categories)
    else:
        details['Categories'] = ' '

    # Extract LinkedIn
    social_div = soup.find('div', class_='social-company-page')
    if social_div:
        linkedin = social_div.find('a', class_='linkedin')
        if linkedin:
            details['LinkedIn'] = linkedin['href'].strip()

    return details

def scrape_directory(base_url, output_file):
    driver = get_driver()
    page = 1
    all_companies = []
    existing_data = pd.DataFrame()

    try:
        existing_data = pd.read_excel(output_file)
        print(f"Loaded {len(existing_data)} existing records.")
    except FileNotFoundError:
        print("No existing file found. Starting fresh.")

    try:
        while True:
            url = f"{base_url}?pageds={page}" if page > 1 else base_url
            soup = get_soup(driver, url)
            
            if not soup:
                print(f"Failed to fetch page {page}. Ending scraping.")
                break
            
            listings = soup.find_all('div', class_='listing-title')
            
            if not listings:
                print(f"No listings found on page {page}. Ending scraping.")
                break
            
            for listing in listings:
                link = listing.find('a')
                if link:
                    company_url = link['href'].strip()
                    company_name = clean_text(link.text)
                    
                    if not existing_data.empty and company_name in existing_data['Name'].values:
                        print(f"Skipping {company_name} - already scraped.")
                        continue
                    
                    print(f"Scraping {company_name}...")
                    details = extract_company_details(driver, company_url)
                    if details:
                        details['Name'] = company_name
                        all_companies.append(details)
                        
                        # Save after each record
                        df = pd.DataFrame(all_companies)
                        if not existing_data.empty:
                            df = pd.concat([existing_data, df], ignore_index=True)
                        df.to_excel(output_file, index=False)
                        print(f"Saved {len(df)} records to {output_file}")
                    else:
                        print(f"Failed to scrape details for {company_name}")
                    
                    # Random delay to be polite to the server
                    time.sleep(random.uniform(3, 7))
            
            page += 1
            print(f"Moving to page {page}")
            # Additional delay between pages
            time.sleep(random.uniform(5, 10))

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Restarting the scraper...")
        driver.quit()
        time.sleep(10)  # Wait for 10 seconds before restarting
        scrape_directory(base_url, output_file)

    finally:
        driver.quit()

    print("Scraping completed.")

# Usage
base_url = "https://www.voxelmatters.directory/company-category/printing/"
output_file = "voxel_matters_companies.xlsx"

while True:
    try:
        scrape_directory(base_url, output_file)
        break  # If scraping completes successfully, exit the loop
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print("Restarting the entire process in 10 seconds...")
        time.sleep(10)