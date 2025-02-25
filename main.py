import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class InfoScraper:
    def __init__(self, excel_path):
        self.excel_path = excel_path
        self.df = None
        self.driver = None

    def setup_driver(self):
        """Set up headless Chrome driver"""
        logger.info("Setting up Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def read_excel(self):
        """Read data from Excel file"""
        logger.info(f"Reading data from {self.excel_path}")
        try:
            self.df = pd.read_excel(self.excel_path)
            logger.info(f"Successfully read {len(self.df)} entries from Excel")

            # Check required columns
            required_cols = ['Vorname', 'Nachname', 'Institution', 'Adresse']
            missing_cols = [col for col in required_cols if col not in self.df.columns]

            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                raise ValueError(f"Excel file is missing required columns: {missing_cols}")

            # Create website and phone columns if they don't exist
            if 'Webseite' not in self.df.columns:
                self.df['Webseite'] = None
            if 'Telefon' not in self.df.columns:
                self.df['Telefon'] = None

            return True

        except Exception as e:
            logger.error(f"Error reading Excel file: {str(e)}")
            return False

    def search_google(self, query):
        """Search Google for information"""
        try:
            self.driver.get("https://www.google.com")
            # Accept cookies if prompted (common in EU)
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept all')]"))
                ).click()
            except:
                pass  # No cookie prompt

            # Find search box and enter query
            search_box = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, "q"))
            )
            search_box.clear()
            search_box.send_keys(query)
            search_box.send_keys(Keys.RETURN)

            # Wait for results
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "search"))
            )

            # Get the page source after search results are loaded
            return self.driver.page_source

        except Exception as e:
            logger.error(f"Error searching Google for '{query}': {str(e)}")
            return None

    def extract_website(self, html):
        """Extract website URL from search results"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Look for first organic result
            search_results = soup.select('.g .yuRUbf a')

            if search_results:
                website = search_results[0]['href']
                return website
            return None

        except Exception as e:
            logger.error(f"Error extracting website: {str(e)}")
            return None

    def extract_phone(self, html):
        """Extract phone number from search results"""
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # First try to find phone in Google's featured snippet or knowledge panel
            phone_patterns = [
                r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # US/CA: (123) 456-7890
                r'\+\d{1,3}\s?\d{2,3}\s?\d{3,4}\s?\d{3,4}',  # International: +12 345 6789
                r'\d{3,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}'  # General format: 123-456-7890
            ]

            # Combine all text content
            text_content = soup.get_text()

            for pattern in phone_patterns:
                matches = re.findall(pattern, text_content)
                if matches:
                    return matches[0]

            return None

        except Exception as e:
            logger.error(f"Error extracting phone: {str(e)}")
            return None

    def visit_website_for_phone(self, url):
        """Visit the website to look for phone number"""
        if not url:
            return None

        try:
            self.driver.get(url)
            # Wait for page to load
            time.sleep(3)

            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            # Phone patterns
            phone_patterns = [
                r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # US/CA: (123) 456-7890
                r'\+\d{1,3}\s?\d{2,3}\s?\d{3,4}\s?\d{3,4}',  # International: +12 345 6789
                r'\d{3,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}'  # General format: 123-456-7890
            ]

            # Look for contact page links
            contact_links = self.driver.find_elements(By.XPATH,
                                                      "//a[contains(translate(text(), 'CONTACT', 'contact'), 'contact') or contains(@href, 'contact')]")

            # If contact page exists, visit it
            if contact_links:
                contact_links[0].click()
                time.sleep(2)
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')

            # Try to find phone in the page
            text_content = soup.get_text()

            for pattern in phone_patterns:
                matches = re.findall(pattern, text_content)
                if matches:
                    return matches[0]

            return None

        except Exception as e:
            logger.error(f"Error visiting website for phone: {str(e)}")
            return None

    def process_data(self):
        """Process each row in the Excel file"""
        if self.df is None:
            logger.error("No data loaded from Excel")
            return False

        total_rows = len(self.df)
        for index, row in self.df.iterrows():
            try:
                # Skip if both website and phone are already filled
                if pd.notna(row['Website']) and pd.notna(row['Phone']):
                    logger.info(
                        f"Skipping entry {index + 1}/{total_rows} (already complete): {row['First name']} {row['Last name']}")
                    continue

                first_name = str(row['First name'])
                last_name = str(row['Last name'])
                institution = str(row['Institution'])
                location = str(row['Location'])

                logger.info(f"Processing entry {index + 1}/{total_rows}: {first_name} {last_name} at {institution}")

                # Create search queries
                name_query = f"{first_name} {last_name} {institution} {location}"
                institution_query = f"{institution} {location} contact"

                # Search for person first
                logger.info(f"Searching for: {name_query}")
                html = self.search_google(name_query)
                website = self.extract_website(html)
                phone = self.extract_phone(html)

                # If no results, search for institution
                if not website or not phone:
                    logger.info(f"Searching for institution: {institution_query}")
                    html = self.search_google(institution_query)

                    if not website:
                        website = self.extract_website(html)

                    if not phone:
                        phone = self.extract_phone(html)

                # If we have a website but no phone, visit the website
                if website and not phone:
                    logger.info(f"Visiting website for phone: {website}")
                    phone = self.visit_website_for_phone(website)

                # Update dataframe with findings
                if website and pd.isna(self.df.at[index, 'Website']):
                    self.df.at[index, 'Website'] = website
                    logger.info(f"Found website: {website}")

                if phone and pd.isna(self.df.at[index, 'Phone']):
                    self.df.at[index, 'Phone'] = phone
                    logger.info(f"Found phone: {phone}")

                # Save after each entry to prevent data loss
                self.save_excel()

                # Add random delay to avoid being blocked
                time.sleep(2)

            except Exception as e:
                logger.error(f"Error processing entry {index + 1}: {str(e)}")

        return True

    def save_excel(self):
        """Save data back to Excel file"""
        try:
            self.df.to_excel(self.excel_path, index=False)
            logger.info(f"Successfully saved data to {self.excel_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving Excel file: {str(e)}")
            return False

    def run(self):
        """Run the complete scraping process"""
        try:
            # Read Excel data
            if not self.read_excel():
                return False

            # Setup browser
            self.setup_driver()

            # Process data
            success = self.process_data()

            # Close browser
            if self.driver:
                self.driver.quit()

            return success

        except Exception as e:
            logger.error(f"Error running scraper: {str(e)}")
            if self.driver:
                self.driver.quit()
            return False


if __name__ == "__main__":
    # Example usage
    excel_file = "datas.xlsx"
    scraper = InfoScraper(excel_file)
    scraper.run()