import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re
import logging
import urllib.parse
import requests
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MedicalProviderScraper:
    def __init__(self, excel_path):
        self.excel_path = excel_path
        self.df = None
        self.driver = None

    def setup_driver(self):
        """Set up headless Chrome driver - for fallback if needed"""
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

            # Check required columns based on the provided screenshot
            # First check if German column names exist
            if 'Vorname' in self.df.columns and 'Nachname' in self.df.columns:
                self.name_cols = {'first': 'Vorname', 'last': 'Nachname'}
                self.institution_col = 'Institution'
                self.address_col = 'Adresse'
                self.website_col = 'Webseite'
                self.phone_col = 'Telefon'
                self.specialty_cols = ['Spalte5', 'Zusatz', 'SpalteF']
            # Check if other columns from the second screenshot exist
            elif 'Fachgebiet 1' in self.df.columns:
                self.name_cols = None  # No explicit name columns
                self.institution_col = None  # No explicit institution column
                self.specialty_cols = ['Fachgebiet 1', 'Fachgebiet 2', 'Fachgebiet 3']
                self.website_col = 'Website'
                self.phone_col = 'Telefon'
                self.id_col = 'Onekey Individual ID'
            else:
                # Try to work with what we have
                logger.warning("Excel structure doesn't match expected format.")
                # Look for likely name columns
                name_candidates = [col for col in self.df.columns if
                                   any(x in col.lower() for x in ['name', 'first', 'last', 'vor', 'nach'])]
                self.name_cols = {'first': name_candidates[0] if name_candidates else None,
                                  'last': name_candidates[1] if len(name_candidates) > 1 else None}

                # Look for institution column
                inst_candidates = [col for col in self.df.columns if
                                   any(x in col.lower() for x in ['inst', 'pract', 'org', 'praxis'])]
                self.institution_col = inst_candidates[0] if inst_candidates else None

                # Look for address column
                addr_candidates = [col for col in self.df.columns if
                                   any(x in col.lower() for x in ['addr', 'adresse', 'location', 'ort'])]
                self.address_col = addr_candidates[0] if addr_candidates else None

                # Look for website and phone
                web_candidates = [col for col in self.df.columns if
                                  any(x in col.lower() for x in ['web', 'site', 'url'])]
                self.website_col = web_candidates[0] if web_candidates else 'Website'

                phone_candidates = [col for col in self.df.columns if
                                    any(x in col.lower() for x in ['phone', 'tel', 'telefon'])]
                self.phone_col = phone_candidates[0] if phone_candidates else 'Telefon'

                self.specialty_cols = [col for col in self.df.columns if
                                       any(x in col.lower() for x in ['specialty', 'fach', 'spezial'])]

            # Create website and phone columns if they don't exist
            if self.website_col not in self.df.columns:
                self.df[self.website_col] = None
            if self.phone_col not in self.df.columns:
                self.df[self.phone_col] = None

            return True

        except Exception as e:
            logger.error(f"Error reading Excel file: {str(e)}")
            return False

    def format_phone_number(self, phone):
        """Format phone number to +41 XX XXX XX XX format for Swiss numbers"""
        if not phone:
            return None

        # Remove all non-numeric characters
        digits = re.sub(r'\D', '', phone)

        # Check if it's likely a Swiss number
        if len(digits) >= 9:
            # If number starts with country code, ensure it's +41
            if digits.startswith('41'):
                digits = digits[2:]  # Remove the 41
            elif digits.startswith('0041'):
                digits = digits[4:]  # Remove the 0041
            elif digits.startswith('00'):
                digits = digits[2:]  # Remove the 00
            elif digits.startswith('0'):
                digits = digits[1:]  # Remove leading 0

            # Format as +41 XX XXX XX XX
            if len(digits) >= 9:
                # Format varies slightly depending on number length
                if len(digits) == 9:
                    formatted = f"+41 {digits[0:2]} {digits[2:5]} {digits[5:7]} {digits[7:9]}"
                else:
                    formatted = f"+41 {digits[0:2]} {digits[2:5]} {digits[5:7]} {digits[7:]}"
                return formatted

        # For non-Swiss or unparseable numbers, return cleaned version
        return phone.strip()

    def format_website(self, url):
        """Format website to www.XXXXX.XXX format"""
        if not url:
            return None

        # Remove protocol (http/https)
        url = re.sub(r'^https?://', '', url)

        # Ensure www. prefix
        if not url.startswith('www.'):
            url = 'www.' + url

        return url

    def search_comparis(self, query, location=None):
        """Search comparis.ch for provider information

        Args:
            query (str): The search query, typically doctor name or specialty
            location (str, optional): Location to search in, e.g. 'Zürich'

        Returns:
            dict: JSON response or None if failed
        """
        try:
            # Set request headers to mimic browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'de-CH,de;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': 'https://www.comparis.ch/gesundheit/aerzte-spitaeler/arztsuche/suche',
                'Origin': 'https://www.comparis.ch'
            }

            # Format query and location for URL
            formatted_query = urllib.parse.quote(query)
            loc_param = f"&location={urllib.parse.quote(location)}" if location else ""

            # Make the API request
            url = f"https://www.comparis.ch/gesundheit/aerzte-spitaeler/arztsuche/api/search?query={formatted_query}{loc_param}&page=1"
            logger.info(f"Comparis API URL: {url}")

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"Comparis search successful. Found {len(data.get('results', []))} results")
                    return data
                except json.JSONDecodeError:
                    logger.error("Failed to decode JSON from comparis.ch response")
                    return None
            else:
                logger.error(f"Failed to search comparis.ch: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error searching comparis.ch: {str(e)}")
            return None

    def search_medicosearch(self, query, location=None):
        """Search medicosearch.ch for provider information

        Args:
            query (str): The search query, typically doctor name or specialty
            location (str, optional): Location to search in, e.g. 'Zürich'

        Returns:
            dict: JSON response or None if failed
        """
        try:
            # Set request headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'de-CH,de;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': 'https://www.medicosearch.ch/',
                'Origin': 'https://www.medicosearch.ch'
            }

            # Format query and location for URL
            formatted_query = urllib.parse.quote(query)
            loc_param = f"&location={urllib.parse.quote(location)}" if location else ""

            # Make the API request - note: this URL may need to be updated based on actual endpoints
            url = f"https://www.medicosearch.ch/api/v1/search?q={formatted_query}{loc_param}&page=1&type=doctors"
            logger.info(f"Medicosearch API URL: {url}")

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"Medicosearch search successful. Found {len(data.get('results', []))} results")
                    return data
                except json.JSONDecodeError:
                    logger.error("Failed to decode JSON from medicosearch.ch response")
                    return None
            else:
                logger.error(f"Failed to search medicosearch.ch: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error searching medicosearch.ch: {str(e)}")
            return None

    def search_docdoc(self, query, location=None):
        """Search docdoc.ch for provider information

        Args:
            query (str): The search query, typically doctor name or specialty
            location (str, optional): Location to search in, e.g. 'Zürich'

        Returns:
            dict: JSON response or None if failed
        """
        try:
            # Set request headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'de-CH,de;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': 'https://www.docdoc.ch/arztsuche',
                'Origin': 'https://www.docdoc.ch'
            }

            # Format query and location for URL
            formatted_query = urllib.parse.quote(query)
            loc_param = f"&location={urllib.parse.quote(location)}" if location else ""

            # Make the API request
            url = f"https://www.docdoc.ch/api/v1/doctors/search?q={formatted_query}{loc_param}&page=1"
            logger.info(f"Docdoc API URL: {url}")

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"Docdoc search successful. Found {len(data.get('doctors', []))} results")
                    return data
                except json.JSONDecodeError:
                    logger.error("Failed to decode JSON from docdoc.ch response")
                    return None
            else:
                logger.error(f"Failed to search docdoc.ch: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error searching docdoc.ch: {str(e)}")
            return None

    def search_doctorfmh(self, query, location=None):
        """Search doctorfmh.ch for provider information

        Args:
            query (str): The search query, typically doctor name or specialty
            location (str, optional): Location to search in, e.g. 'Zürich'

        Returns:
            dict: JSON response or None if failed
        """
        try:
            # Set request headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'de-CH,de;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': 'https://www.doctorfmh.ch/',
                'Origin': 'https://www.doctorfmh.ch'
            }

            # Format query and location for URL
            formatted_query = urllib.parse.quote(query)
            loc_param = f"&location={urllib.parse.quote(location)}" if location else ""

            # Make the API request
            url = f"https://www.doctorfmh.ch/api/search?name={formatted_query}{loc_param}&page=1"
            logger.info(f"DoctorFMH API URL: {url}")

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"DoctorFMH search successful. Found {len(data.get('results', []))} results")
                    return data
                except json.JSONDecodeError:
                    logger.error("Failed to decode JSON from doctorfmh.ch response")
                    return None
            else:
                logger.error(f"Failed to search doctorfmh.ch: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error searching doctorfmh.ch: {str(e)}")
            return None

    def generate_search_query(self, row):
        """Generate search query based on available data"""
        query_parts = []
        location = None

        # Handle first screenshot format with names and institution
        if self.name_cols and self.name_cols['first'] in row and self.name_cols['last'] in row:
            if pd.notna(row[self.name_cols['first']]) and pd.notna(row[self.name_cols['last']]):
                query_parts.append(f"{row[self.name_cols['first']]} {row[self.name_cols['last']]}")

        # Add institution if available
        if self.institution_col and self.institution_col in row and pd.notna(row[self.institution_col]):
            query_parts.append(str(row[self.institution_col]))

        # Extract location from address if available
        if self.address_col and self.address_col in row and pd.notna(row[self.address_col]):
            address = str(row[self.address_col])
            query_parts.append(address)

            # Try to extract city from address
            # Swiss postal code pattern is usually 4 digits
            postal_match = re.search(r'\b(\d{4})\s+([^\d,]+)', address)
            if postal_match:
                location = postal_match.group(2).strip()

        # Handle second screenshot format with specialties
        if not query_parts and self.specialty_cols:
            for col in self.specialty_cols:
                if col in row and pd.notna(row[col]):
                    query_parts.append(str(row[col]))

            # If we have an ID, add it
            if self.id_col and self.id_col in row and pd.notna(row[self.id_col]):
                query_parts.append(str(row[self.id_col]))

        # If we still don't have enough to search, use whatever is available
        if not query_parts:
            for col in row.index:
                if pd.notna(row[col]) and isinstance(row[col], str) and len(row[col]) > 3:
                    query_parts.append(str(row[col]))
                    if len(query_parts) >= 3:
                        break

        # Clean up query
        query = " ".join(query_parts).strip()

        # Store location for location-based searches
        self.last_location = location

        return query

    def extract_data_from_api(self, data, source="unknown"):
        """Extract website and phone from API response data

        Args:
            data (dict): API response data
            source (str): Source of the data (comparis, medicosearch, etc.)

        Returns:
            tuple: (website, phone)
        """
        website = None
        phone = None

        try:
            if source == "comparis":
                # Extract from comparis.ch response
                if data and 'results' in data and data['results']:
                    provider = data['results'][0]  # Use first result

                    if 'website' in provider and provider['website']:
                        website = self.format_website(provider['website'])

                    if 'phoneNumber' in provider and provider['phoneNumber']:
                        phone = self.format_phone_number(provider['phoneNumber'])
                    elif 'phone' in provider and provider['phone']:
                        phone = self.format_phone_number(provider['phone'])

            elif source == "medicosearch":
                # Extract from medicosearch.ch response
                if data and 'results' in data and data['results']:
                    provider = data['results'][0]  # Use first result

                    if 'website' in provider and provider['website']:
                        website = self.format_website(provider['website'])

                    if 'phone' in provider and provider['phone']:
                        phone = self.format_phone_number(provider['phone'])

            elif source == "docdoc":
                # Extract from docdoc.ch response
                if data and 'doctors' in data and data['doctors']:
                    provider = data['doctors'][0]  # Use first result

                    if 'website' in provider and provider['website']:
                        website = self.format_website(provider['website'])

                    if 'phone' in provider and provider['phone']:
                        phone = self.format_phone_number(provider['phone'])

            elif source == "doctorfmh":
                # Extract from doctorfmh.ch response
                if data and 'results' in data and data['results']:
                    provider = data['results'][0]  # Use first result

                    if 'website' in provider and provider['website']:
                        website = self.format_website(provider['website'])

                    if 'phone' in provider and provider['phone']:
                        phone = self.format_phone_number(provider['phone'])
                    elif 'phoneNumber' in provider and provider['phoneNumber']:
                        phone = self.format_phone_number(provider['phoneNumber'])

            return website, phone

        except Exception as e:
            logger.error(f"Error extracting data from {source} API: {str(e)}")
            return None, None

    def process_data(self):
        """Process each row in the Excel file using Swiss medical provider APIs"""
        if self.df is None:
            logger.error("No data loaded from Excel")
            return False

        total_rows = len(self.df)
        for index, row in self.df.iterrows():
            try:
                # Skip if both website and phone are already filled
                if pd.notna(row[self.website_col]) and pd.notna(row[self.phone_col]):
                    logger.info(f"Skipping entry {index + 1}/{total_rows} (already complete)")
                    continue

                # Generate search query
                search_query = self.generate_search_query(row)
                if not search_query:
                    logger.warning(f"Could not generate search query for entry {index + 1}")
                    continue

                logger.info(f"Processing entry {index + 1}/{total_rows}: {search_query}")

                # Variables to track if we found data
                website_found = False
                phone_found = False

                # Try comparis.ch first
                if not website_found or not phone_found:
                    logger.info(f"Searching comparis.ch for: {search_query}")
                    comparis_data = self.search_comparis(search_query, self.last_location)

                    if comparis_data:
                        website, phone = self.extract_data_from_api(comparis_data, "comparis")

                        if website and pd.isna(row[self.website_col]):
                            self.df.at[index, self.website_col] = website
                            logger.info(f"Found website from comparis.ch: {website}")
                            website_found = True

                        if phone and pd.isna(row[self.phone_col]):
                            self.df.at[index, self.phone_col] = phone
                            logger.info(f"Found phone from comparis.ch: {phone}")
                            phone_found = True

                # Try medicosearch.ch if needed
                if not website_found or not phone_found:
                    logger.info(f"Searching medicosearch.ch for: {search_query}")
                    medicosearch_data = self.search_medicosearch(search_query, self.last_location)

                    if medicosearch_data:
                        website, phone = self.extract_data_from_api(medicosearch_data, "medicosearch")

                        if not website_found and website and pd.isna(row[self.website_col]):
                            self.df.at[index, self.website_col] = website
                            logger.info(f"Found website from medicosearch.ch: {website}")
                            website_found = True

                        if not phone_found and phone and pd.isna(row[self.phone_col]):
                            self.df.at[index, self.phone_col] = phone
                            logger.info(f"Found phone from medicosearch.ch: {phone}")
                            phone_found = True

                # Try docdoc.ch if needed
                if not website_found or not phone_found:
                    logger.info(f"Searching docdoc.ch for: {search_query}")
                    docdoc_data = self.search_docdoc(search_query, self.last_location)

                    if docdoc_data:
                        website, phone = self.extract_data_from_api(docdoc_data, "docdoc")

                        if not website_found and website and pd.isna(row[self.website_col]):
                            self.df.at[index, self.website_col] = website
                            logger.info(f"Found website from docdoc.ch: {website}")
                            website_found = True

                        if not phone_found and phone and pd.isna(row[self.phone_col]):
                            self.df.at[index, self.phone_col] = phone
                            logger.info(f"Found phone from docdoc.ch: {phone}")
                            phone_found = True

                # Try doctorfmh.ch if needed
                if not website_found or not phone_found:
                    logger.info(f"Searching doctorfmh.ch for: {search_query}")
                    doctorfmh_data = self.search_doctorfmh(search_query, self.last_location)

                    if doctorfmh_data:
                        website, phone = self.extract_data_from_api(doctorfmh_data, "doctorfmh")

                        if not website_found and website and pd.isna(row[self.website_col]):
                            self.df.at[index, self.website_col] = website
                            logger.info(f"Found website from doctorfmh.ch: {website}")
                            website_found = True

                        if not phone_found and phone and pd.isna(row[self.phone_col]):
                            self.df.at[index, self.phone_col] = phone
                            logger.info(f"Found phone from doctorfmh.ch: {phone}")
                            phone_found = True

                # Save after every 5 entries to prevent data loss
                if index % 5 == 0:
                    self.save_excel()

                # Add random delay between 1-3 seconds to avoid being blocked
                delay = 1 + (index % 2)
                time.sleep(delay)

            except Exception as e:
                logger.error(f"Error processing entry {index + 1}: {str(e)}")
                # Continue with next entry
                continue

        # Final save
        self.save_excel()
        return True

    def save_excel(self):
        """Save data back to Excel file"""
        try:
            self.df.to_excel(self.excel_path, index=False)
            logger.info(f"Successfully saved data to {self.excel_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving Excel file: {str(e)}")
            # Try saving to a backup file
            try:
                backup_path = self.excel_path.replace('.xlsx', '_backup.xlsx')
                self.df.to_excel(backup_path, index=False)
                logger.info(f"Saved backup to {backup_path}")
            except:
                pass
            return False

    def run(self):
        """Run the complete process"""
        try:
            # Read Excel data
            if not self.read_excel():
                return False

            # Initialize location tracking
            self.last_location = None

            # Process data
            success = self.process_data()

            return success

        except Exception as e:
            logger.error(f"Error running scraper: {str(e)}")
            return False


if __name__ == "__main__":
    # Example usage
    excel_file = "datas.xlsx"
    scraper = MedicalProviderScraper(excel_file)
    scraper.run()