"""
MLB Team Salary Web Scraper
Scrapes team payroll data from stevetheump.com for years 1998-2025
Saves raw data to CSV files organized by year
"""

import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import re


class SalaryScraper:
    """Scraper for MLB team payroll data from stevetheump.com"""
    
    def __init__(self, headless=True):
        """
        Initialize the scraper with Selenium WebDriver
        
        Args:
            headless: Run browser in headless mode (no visible window)
        """
        self.driver = None
        self.headless = headless
        self.url = "https://www.stevetheump.com/Payrolls.htm"
        
    def setup_driver(self):
        """Setup Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.set_page_load_timeout(60)
        
        print("✓ WebDriver initialized successfully")
        
    def close_driver(self):
        """Close the WebDriver safely"""
        if self.driver:
            try:
                self.driver.quit()
                print("✓ WebDriver closed")
            except Exception as e:
                print(f"⚠ WebDriver close warning: {e}")
            finally:
                self.driver = None
    
    def wait_for_page_load(self, timeout=30):
        """Wait for page to fully load"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)
            return True
        except TimeoutException:
            print("⚠ Page load timeout")
            return False
    
    def create_directory_structure(self, base_path, years):
        """
        Create directory structure for all years
        
        Args:
            base_path: Base path for data storage
            years: List of years to create folders for
        """
        for year in years:
            path = os.path.join(base_path, "Data", str(year))
            os.makedirs(path, exist_ok=True)
        print(f"✓ Directory structure created for years {min(years)}-{max(years)}")
    
    def extract_all_tables(self):
        """
        Extract all tables from the page
        
        Returns:
            List of (year, DataFrame) tuples
        """
        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Find all tables
        tables = soup.find_all('table')
        
        print(f"Found {len(tables)} tables on page")
        
        return tables, soup
    
    def parse_year_from_header(self, element):
        """
        Try to find the year from nearby header elements
        
        Args:
            element: BeautifulSoup element to search around
            
        Returns:
            Year as integer or None
        """
        # Look for year patterns in text
        text = str(element)
        year_match = re.search(r'(19\d{2}|20[0-2]\d)', text)
        if year_match:
            return int(year_match.group(1))
        return None
    
    def extract_payroll_tables(self, soup):
        """
        Extract payroll tables organized by year
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Dictionary mapping year to list of table rows
        """
        year_data = {}
        
        # Find all headers that indicate a year section
        headers = soup.find_all(['h2', 'h3', 'b', 'strong'])
        
        current_year = None
        
        for header in headers:
            header_text = header.get_text()
            # Look for year in header
            year_match = re.search(r'(19\d{2}|20[0-2]\d)', header_text)
            if year_match and ('payroll' in header_text.lower() or 'mlb' in header_text.lower() or 'opening' in header_text.lower()):
                current_year = int(year_match.group(1))
                if current_year not in year_data:
                    year_data[current_year] = []
        
        # Now find tables and associate them with years
        all_elements = soup.find_all(['h2', 'h3', 'table', 'b', 'strong'])
        current_year = None
        
        for elem in all_elements:
            if elem.name in ['h2', 'h3', 'b', 'strong']:
                text = elem.get_text()
                year_match = re.search(r'(19\d{2}|20[0-2]\d)', text)
                if year_match and ('payroll' in text.lower() or 'mlb' in text.lower() or 'team' in text.lower()):
                    current_year = int(year_match.group(1))
            elif elem.name == 'table' and current_year:
                if current_year not in year_data:
                    year_data[current_year] = []
                year_data[current_year].append(elem)
        
        return year_data
    
    def table_to_dataframe(self, table, year):
        """
        Convert an HTML table to a DataFrame
        
        Args:
            table: BeautifulSoup table element
            year: Year for context
            
        Returns:
            pandas DataFrame or None
        """
        try:
            # Try using pandas read_html
            html_str = str(table)
            dfs = pd.read_html(html_str)
            if dfs:
                return dfs[0]
        except Exception as e:
            pass
        
        # Manual parsing fallback
        try:
            rows = table.find_all('tr')
            data = []
            
            for row in rows:
                cols = row.find_all(['td', 'th'])
                row_data = [col.get_text(strip=True) for col in cols]
                if row_data and any(row_data):  # Skip empty rows
                    data.append(row_data)
            
            if data:
                # Try to determine if first row is header
                df = pd.DataFrame(data[1:], columns=data[0]) if len(data) > 1 else pd.DataFrame(data)
                return df
        except Exception as e:
            pass
        
        return None
    
    def scrape_payrolls(self, base_path, start_year=1998, end_year=2025):
        """
        Scrape payroll data for all years
        
        Args:
            base_path: Base path for saving files
            start_year: First year to scrape
            end_year: Last year to scrape
            
        Returns:
            Dictionary with results summary
        """
        years = list(range(start_year, end_year + 1))
        
        print(f"\n{'='*60}")
        print("MLB Team Salary Scraper")
        print(f"{'='*60}")
        print(f"URL: {self.url}")
        print(f"Years: {start_year} - {end_year}")
        print(f"{'='*60}\n")
        
        # Create directories
        self.create_directory_structure(base_path, years)
        
        # Load page
        print("Loading page...")
        self.driver.get(self.url)
        
        if not self.wait_for_page_load():
            return {"success": False, "error": "Page load failed"}
        
        print("✓ Page loaded successfully\n")
        
        # Get page source and parse
        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Extract tables by year
        print("Extracting payroll tables...")
        year_tables = self.extract_payroll_tables(soup)
        
        print(f"✓ Found data for {len(year_tables)} years\n")
        
        results = {"success": True, "files_created": [], "years_processed": []}
        
        # Process each year
        for year in sorted(year_tables.keys()):
            if year < start_year or year > end_year:
                continue
                
            print(f"\n--- {year} ---")
            tables = year_tables[year]
            
            if not tables:
                print(f"  ⚠ No tables found for {year}")
                continue
            
            # Convert first table (usually the main payroll table)
            df = None
            for table in tables:
                df = self.table_to_dataframe(table, year)
                if df is not None and len(df) >= 20:  # Should have ~30 teams
                    break
            
            if df is not None and not df.empty:
                # Save to CSV
                year_path = os.path.join(base_path, "Data", str(year))
                filepath = os.path.join(year_path, f"Salaries_{year}.csv")
                df.to_csv(filepath, index=False)
                print(f"  ✓ Saved: Salaries_{year}.csv ({len(df)} rows, {len(df.columns)} columns)")
                results["files_created"].append(filepath)
                results["years_processed"].append(year)
            else:
                print(f"  ⚠ Could not extract valid data for {year}")
        
        return results


def main():
    """Main function to run the salary scraper"""
    # Configuration
    START_YEAR = 1998
    END_YEAR = 2025
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
    HEADLESS = True
    
    print("\n" + "="*60)
    print("MLB Team Salary Scraper")
    print("="*60)
    print(f"Years to scrape: {START_YEAR} - {END_YEAR}")
    print(f"Output directory: {os.path.join(BASE_PATH, 'Data')}")
    print(f"Headless mode: {HEADLESS}")
    print("="*60 + "\n")
    
    # Initialize scraper
    scraper = SalaryScraper(headless=HEADLESS)
    
    try:
        # Setup WebDriver
        scraper.setup_driver()
        
        # Scrape data
        results = scraper.scrape_payrolls(BASE_PATH, START_YEAR, END_YEAR)
        
        # Summary
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        
        if results.get("success"):
            print(f"\n✓ Years processed: {len(results.get('years_processed', []))}")
            print(f"✓ Files created: {len(results.get('files_created', []))}")
            
            for filepath in results.get('files_created', []):
                print(f"  - {os.path.basename(filepath)}")
        else:
            print(f"\n✗ Failed: {results.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Clean up
        scraper.close_driver()
    
    print("\n" + "="*60)
    print("Scraping complete!")
    print("="*60)
    print("\nNext step: Run 'python salary_cleaning.py' to clean and standardize the data")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
