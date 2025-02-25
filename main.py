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

def comparis_search(driver, query):
    driver.get(f"https://www.comparis.ch/suche/?q={query}")
    time.sleep(5)  # Wait for the search results to load

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    links = []
    for g in soup.find_all('a', class_='cmp-product-teaser__title-link'):
        link = g['href']
        if link.startswith('http'):
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

def extract_whatsapp_link(soup):
    # Extract WhatsApp link from the soup
    for link in soup.find_all('a', href=True):
        href = link['href']
        if "wa.me" in href:
            return href
    return None

def scrape_website(url, query):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses
        soup = BeautifulSoup(response.text, 'html.parser')
        phone_numbers = extract_phone_number(soup)
        website = extract_website(soup, query)
        whatsapp_link = extract_whatsapp_link(soup)
        return phone_numbers, website, whatsapp_link
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return [], None, None

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
        websites = []
        whatsapp_links = []

        for link in links:
            print(f"\nScraping: {link}")
            phone_numbers, website, whatsapp_link = scrape_website(link, query)
            if phone_numbers:
                print(f"Phone Numbers Found: {phone_numbers}")
                all_phone_numbers.extend(phone_numbers)
            if website:
                print(f"Website Found: {website}")
                websites.append(website)
            if whatsapp_link:
                print(f"WhatsApp Link Found: {whatsapp_link}")
                whatsapp_links.append(whatsapp_link)

        # Find the most common phone number
        if all_phone_numbers:
            phone_counter = Counter(all_phone_numbers)
            most_common_phone = phone_counter.most_common(1)[0]
            if most_common_phone[1] > 1:
                most_common_phone_number = most_common_phone[0]
            else:
                most_common_phone_number = ', '.join(all_phone_numbers)
            print(f"Selected Phone Number: {most_common_phone_number}")
            output_df.at[index, 'Telefon'] = most_common_phone_number
        else:
            print("\nNo phone numbers found after searching all links.")

        # Select the best website link
        if websites:
            # Prefer website links that contain the person's name
            name_in_domain = [w for w in websites if any(word.lower() in w.lower() for word in query.split())]
            if name_in_domain:
                selected_website = name_in_domain[0]
            else:
                selected_website = websites[0]
            print(f"Selected Website: {selected_website}")
            output_df.at[index, 'Webseite'] = selected_website
        elif whatsapp_links:
            # Use WhatsApp link if no website is found
            selected_website = whatsapp_links[0]
            print(f"Selected Website (WhatsApp): {selected_website}")
            output_df.at[index, 'Webseite'] = selected_website
        else:
            # Fallback to comparis.ch search
            driver = setup_driver()
            comparis_links = comparis_search(driver, query)
            driver.quit()

            print(f"Comparis Search Found {len(comparis_links)} links:")
            for link in comparis_links:
                print(link)

            for link in comparis_links:
                print(f"\nScraping: {link}")
                phone_numbers, website, whatsapp_link = scrape_website(link, query)
                if phone_numbers:
                    print(f"Phone Numbers Found: {phone_numbers}")
                    all_phone_numbers.extend(phone_numbers)
                if website:
                    print(f"Website Found: {website}")
                    websites.append(website)
                if whatsapp_link:
                    print(f"WhatsApp Link Found: {whatsapp_link}")
                    whatsapp_links.append(whatsapp_link)

            # Re-evaluate phone numbers and websites after comparis search
            if all_phone_numbers:
                phone_counter = Counter(all_phone_numbers)
                most_common_phone = phone_counter.most_common(1)[0]
                if most_common_phone[1] > 1:
                    most_common_phone_number = most_common_phone[0]
                else:
                    most_common_phone_number = ', '.join(all_phone_numbers)
                print(f"Selected Phone Number: {most_common_phone_number}")
                output_df.at[index, 'Telefon'] = most_common_phone_number
            else:
                print("\nNo phone numbers found after searching all links.")

            if websites:
                # Prefer website links that contain the person's name
                name_in_domain = [w for w in websites if any(word.lower() in w.lower() for word in query.split())]
                if name_in_domain:
                    selected_website = name_in_domain[0]
                else:
                    selected_website = websites[0]
                print(f"Selected Website: {selected_website}")
                output_df.at[index, 'Webseite'] = selected_website
            elif whatsapp_links:
                # Use WhatsApp link if no website is found
                selected_website = whatsapp_links[0]
                print(f"Selected Website (WhatsApp): {selected_website}")
                output_df.at[index, 'Webseite'] = selected_website
            else:
                print("\nNo website found after searching all links.")

        # Save the updated DataFrame to the output Excel file after each query
        output_df.to_excel(output_file, index=False)
        print(f"Updated output file after processing {query}")

if __name__ == "__main__":
    input_file = 'datas.xlsx'  # Path to the input Excel file
    output_file = 'output.xlsx'  # Path to the output Excel file
    process_excel(input_file, output_file)