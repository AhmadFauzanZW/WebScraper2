import time
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import requests
from collections import Counter

def setup_driver():
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    service = Service('C:/Python/Project/chromedriver-win64/chromedriver.exe')  # Update this path
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def bing_search(driver, query):
    driver.get("https://www.bing.com/")
    time.sleep(2)  # Wait for the page to load

    search_box = driver.find_element(By.NAME, "q")
    search_box.send_keys(query)
    search_box.send_keys(Keys.ENTER)
    time.sleep(5)  # Wait for the search results to load

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    links = []
    for g in soup.find_all('li', class_='b_algo'):
        anchors = g.find_all('a')
        if anchors:
            link = anchors[0]['href']
            links.append(link)
    return links

def extract_phone_number(soup):
    text = soup.get_text()
    # Regular expression to find phone numbers
    phone_pattern = re.compile(r'\+?\d[\d -]{8,}\d')
    matches = phone_pattern.findall(text)
    formatted_phones = []
    for match in matches:
        # Format phone number to +41 XX XXX XX XX
        match = match.replace(" ", "").replace("-", "")
        if len(match) == 12 and match.startswith('+41'):
            formatted_phone = f"+{match[:2]} {match[2:4]} {match[4:7]} {match[7:9]} {match[9:]}"
            formatted_phones.append(formatted_phone)
    return formatted_phones

def extract_website(soup, query):
    # Extract website link from the soup
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('http'):
            domain = href.split('//')[1].split('/')[0]
            if domain.startswith('www.'):
                if any(word.lower() in domain.lower() for word in query.split()):
                    return domain
            else:
                if any(word.lower() in domain.lower() for word in query.split()):
                    return f"www.{domain}"
    return None

def scrape_website(url, query):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses
        soup = BeautifulSoup(response.text, 'html.parser')
        phone_numbers = extract_phone_number(soup)
        website = extract_website(soup, query)
        return phone_numbers, website
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return [], None

def process_excel(input_file, output_file):
    df = pd.read_excel(input_file)
    output_df = df.copy()

    for index, row in df.iterrows():
        first_name = row['Vorname']
        last_name = row['Nachname']
        location = row['Adresse']
        query = f"{first_name} {last_name} {location}"

        driver = setup_driver()
        links = bing_search(driver, query)
        driver.quit()

        print(f"Query: {query}")
        print(f"Found {len(links)} links:")
        for link in links:
            print(link)

        all_phone_numbers = []
        website = None
        for link in links:
            print(f"\nScraping: {link}")
            phone_numbers, extracted_website = scrape_website(link, query)
            if phone_numbers:
                print(f"Phone Numbers Found: {phone_numbers}")
                all_phone_numbers.extend(phone_numbers)
            if extracted_website:
                website = extracted_website
                print(f"Website Found: {website}")

        # Find the most common phone number
        if all_phone_numbers:
            phone_counter = Counter(all_phone_numbers)
            most_common_phone = phone_counter.most_common(1)[0][0]
            print(f"Most Common Phone Number: {most_common_phone}")
            output_df.at[index, 'Telefon'] = most_common_phone
        else:
            print("\nNo phone numbers found after searching all links.")

        if website:
            print(f"Final Website: {website}")
            output_df.at[index, 'Webseite'] = website
        else:
            print("\nNo website found after searching all links.")

        # Save the updated DataFrame to the output Excel file after each query
        output_df.to_excel(output_file, index=False)
        print(f"Updated output file after processing {query}")

if __name__ == "__main__":
    input_file = 'datas.xlsx'  # Path to the input Excel file
    output_file = 'output.xlsx'  # Path to the output Excel file
    process_excel(input_file, output_file)