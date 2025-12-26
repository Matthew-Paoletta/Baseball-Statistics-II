"""
Baseball Statistics Web Scraper
Uses Selenium to scrape team statistics from Baseball Reference
Stores data in organized CSV files by year and category
"""

import os
import time
import pandas as pd
import signal
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


# Timeout exception for long-running operations
class OperationTimeoutError(Exception):
    pass


class BaseballScraper:
    """Scraper for Baseball Reference team statistics"""
    
    def __init__(self, headless=True):
        """
        Initialize the scraper with Selenium WebDriver
        
        Args:
            headless: Run browser in headless mode (no visible window)
        """
        self.driver = None
        self.headless = headless
        self.base_url = "https://www.baseball-reference.com/leagues/majors/{year}.shtml"
        
    def setup_driver(self):
        """Setup Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        # Options to help avoid detection
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Suppress logging
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set page load timeout
        self.driver.set_page_load_timeout(60)
        
        # Remove webdriver property to avoid detection
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
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
            
    def create_directory_structure(self, base_path, year):
        """
        Create the directory structure for storing CSV files
        
        Args:
            base_path: Base path for data storage
            year: Year for the data
            
        Returns:
            Path to the year folder
        """
        path = os.path.join(base_path, "Data", str(year))
        os.makedirs(path, exist_ok=True)
            
        print(f"✓ Directory structure created for {year}")
        return path
    
    def wait_for_page_load(self, timeout=15):
        """Wait for page to fully load"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            # Additional wait for dynamic content
            time.sleep(3)
            return True
        except TimeoutException:
            print("⚠ Page load timeout")
            return False
    
    def expand_hidden_tables(self):
        """Click to expand any hidden/collapsed tables on the page"""
        try:
            # Baseball Reference sometimes hides tables behind "Show" buttons
            show_buttons = self.driver.find_elements(By.CSS_SELECTOR, "a.sr_preset")
            for button in show_buttons:
                try:
                    button.click()
                    time.sleep(0.5)
                except:
                    pass
        except:
            pass
    
    def get_table_by_id(self, table_id):
        """
        Extract a table by its ID and convert to DataFrame
        
        Args:
            table_id: HTML id of the table
            
        Returns:
            pandas DataFrame or None if not found
        """
        try:
            # First try to find in regular DOM
            table = self.driver.find_element(By.ID, table_id)
            html = table.get_attribute('outerHTML')
            df = pd.read_html(html)[0]
            return df
        except NoSuchElementException:
            # Table might be in a comment (Baseball Reference hides some tables this way)
            try:
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Look for the table in comments
                comments = soup.find_all(string=lambda text: isinstance(text, str) and table_id in text)
                for comment in comments:
                    if f'id="{table_id}"' in str(comment) or f"id='{table_id}'" in str(comment):
                        comment_soup = BeautifulSoup(str(comment), 'html.parser')
                        table = comment_soup.find('table', {'id': table_id})
                        if table:
                            df = pd.read_html(str(table))[0]
                            return df
                            
                # Also check for tables in HTML comments
                import re
                comment_pattern = re.compile(r'<!--(.+?)-->', re.DOTALL)
                matches = comment_pattern.findall(page_source)
                for match in matches:
                    if table_id in match:
                        match_soup = BeautifulSoup(match, 'html.parser')
                        table = match_soup.find('table', {'id': table_id})
                        if table:
                            df = pd.read_html(str(table))[0]
                            return df
                            
            except Exception as e:
                print(f"  ⚠ Error extracting {table_id} from comments: {e}")
                
        except Exception as e:
            print(f"  ⚠ Error getting table {table_id}: {e}")
            
        return None
    
    def get_all_tables(self, timeout_seconds=60):
        """
        Get all available tables from the page with timeout
        
        Args:
            timeout_seconds: Maximum time to spend extracting tables
            
        Returns:
            Dictionary with table names and DataFrames
        """
        start_time = time.time()
        
        try:
            page_source = self.driver.page_source
        except Exception as e:
            print(f"  ⚠ Error getting page source: {e}")
            return {}
        
        # Check timeout
        if time.time() - start_time > timeout_seconds:
            print(f"  ⚠ Timeout after {timeout_seconds}s getting page source")
            return {}
            
        soup = BeautifulSoup(page_source, 'html.parser')
        
        tables = {}
        
        # Find all tables with IDs (quick operation)
        print("    Finding visible tables...")
        for table in soup.find_all('table', {'id': True}):
            if time.time() - start_time > timeout_seconds:
                print(f"  ⚠ Timeout reached, returning {len(tables)} tables found so far")
                return tables
                
            table_id = table.get('id')
            if table_id:
                try:
                    df = pd.read_html(str(table))[0]
                    tables[table_id] = df
                except:
                    pass
                    
        print(f"    Found {len(tables)} visible tables")
        
        # Also search in comments (Baseball Reference hides many tables in comments)
        # This is the slow part - limit it
        print("    Searching HTML comments for hidden tables...")
        import re
        
        try:
            # Use a more efficient regex with a limit
            comment_pattern = re.compile(r'<!--(.*?)-->', re.DOTALL)
            
            # Limit comment search to avoid hanging
            comment_count = 0
            max_comments = 50  # Limit how many comments we process
            
            for match in comment_pattern.finditer(page_source):
                if time.time() - start_time > timeout_seconds:
                    print(f"  ⚠ Timeout during comment search, returning {len(tables)} tables")
                    return tables
                    
                comment_count += 1
                if comment_count > max_comments:
                    print(f"    Processed {max_comments} comments, stopping search")
                    break
                    
                comment_content = match.group(1)
                
                # Quick check if this comment might contain a table
                if '<table' not in comment_content:
                    continue
                    
                try:
                    match_soup = BeautifulSoup(comment_content, 'html.parser')
                    for table in match_soup.find_all('table', {'id': True}):
                        table_id = table.get('id')
                        if table_id and table_id not in tables:
                            try:
                                df = pd.read_html(str(table))[0]
                                tables[table_id] = df
                            except:
                                pass
                except:
                    pass
                    
        except Exception as e:
            print(f"  ⚠ Error searching comments: {e}")
        
        elapsed = time.time() - start_time
        print(f"    Table extraction completed in {elapsed:.1f}s")
                        
        return tables
    
    def clean_dataframe(self, df):
        """
        Clean the DataFrame by removing unnecessary rows and columns
        
        Args:
            df: pandas DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        if df is None:
            return None
            
        # Handle multi-level columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in df.columns]
        
        # Remove rows that are header repeats (common in Baseball Reference tables)
        df = df[df.iloc[:, 0] != df.columns[0]]
        
        # Remove "League Average" or "Lg Avg" rows if present
        first_col = df.columns[0]
        df = df[~df[first_col].astype(str).str.contains('Avg|Average|League', case=False, na=False)]
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df
    
    def scrape_year(self, year, base_path, max_time=180):
        """
        Scrape all statistics for a given year
        
        Args:
            year: Year to scrape (e.g., 2025)
            base_path: Base path for saving files
            max_time: Maximum time in seconds for scraping this year (default 3 minutes)
            
        Returns:
            Dictionary with results summary
        """
        start_time = time.time()
        
        url = self.base_url.format(year=year)
        print(f"\n{'='*60}")
        print(f"Scraping Baseball Reference for {year}")
        print(f"URL: {url}")
        print(f"Max time: {max_time} seconds")
        print(f"{'='*60}\n")
        
        # Create directory structure
        year_path = self.create_directory_structure(base_path, year)
        
        # Navigate to page
        print("Loading page...")
        try:
            self.driver.get(url)
        except Exception as e:
            print(f"✗ Error navigating to page: {e}")
            return {"success": False, "error": f"Navigation failed: {e}"}
        
        if not self.wait_for_page_load():
            print("✗ Failed to load page")
            return {"success": False, "error": "Page load failed"}
        
        # Check time
        elapsed = time.time() - start_time
        if elapsed > max_time:
            print(f"✗ Timeout after {elapsed:.1f}s during page load")
            return {"success": False, "error": "Timeout during page load"}
        
        print(f"✓ Page loaded successfully ({elapsed:.1f}s)\n")
        
        # Expand any hidden tables
        self.expand_hidden_tables()
        time.sleep(2)
        
        # Check time before table extraction
        elapsed = time.time() - start_time
        remaining_time = max_time - elapsed
        if remaining_time < 10:
            print(f"✗ Not enough time remaining ({remaining_time:.1f}s)")
            return {"success": False, "error": "Timeout before table extraction"}
        
        # Get all tables with remaining time as timeout
        print(f"Extracting tables (timeout: {remaining_time:.0f}s)...")
        all_tables = self.get_all_tables(timeout_seconds=min(remaining_time, 60))
        print(f"✓ Found {len(all_tables)} tables\n")
        
        # Define which tables to save with simple names (matching your format)
        # Maps table_id -> output filename
        table_mapping = {
            # Batting - pick the best/most complete batting table
            "teams_standard_batting": f"Batting_{year}.csv",
            # Pitching - pick the best/most complete pitching table
            "teams_standard_pitching": f"Pitching_{year}.csv",
            # Fielding
            "teams_standard_fielding": f"Fielding_{year}.csv",
            # WAA per position
            "team_output": f"WAA_Positions_{year}.csv",
            # Postseason (if available)
            "postseason": f"Postseason_{year}.csv",
        }
        
        # Alternative table IDs to try if primary ones aren't found
        alternatives = {
            f"Batting_{year}.csv": ["teams_batting", "teams_batting_totals"],
            f"Pitching_{year}.csv": ["teams_pitching", "teams_pitching_totals"],
            f"Fielding_{year}.csv": ["teams_fielding", "teams_fielding_totals"],
            f"WAA_Positions_{year}.csv": ["teams_war_batting", "teams_pos_batting"],
        }
        
        results = {"success": True, "files_created": [], "tables_found": list(all_tables.keys())}
        
        # Print available tables for debugging
        print("Available tables found:")
        for table_id in sorted(all_tables.keys()):
            print(f"  - {table_id}")
        print()
        
        # Track which files we've already saved
        saved_files = set()
        
        # Save tables with simple naming
        print("--- Saving Tables ---")
        
        for table_id, filename in table_mapping.items():
            if table_id in all_tables:
                df = self.clean_dataframe(all_tables[table_id])
                if df is not None and not df.empty:
                    filepath = os.path.join(year_path, filename)
                    df.to_csv(filepath, index=False)
                    print(f"  ✓ Saved: {filename} ({len(df)} rows)")
                    results["files_created"].append(filepath)
                    saved_files.add(filename)
                else:
                    print(f"  ⚠ {table_id}: Empty or invalid data")
            else:
                # Try alternatives
                if filename in alternatives:
                    for alt_id in alternatives[filename]:
                        if alt_id in all_tables:
                            df = self.clean_dataframe(all_tables[alt_id])
                            if df is not None and not df.empty:
                                filepath = os.path.join(year_path, filename)
                                df.to_csv(filepath, index=False)
                                print(f"  ✓ Saved: {filename} ({len(df)} rows) [from {alt_id}]")
                                results["files_created"].append(filepath)
                                saved_files.add(filename)
                                break
                    else:
                        print(f"  ⚠ {filename}: No matching table found")
                else:
                    print(f"  ⚠ {table_id}: Not found")
        
        return results
    
    def scrape_multiple_years(self, years, base_path, max_time_per_year=180):
        """
        Scrape statistics for multiple years
        
        Args:
            years: List of years to scrape
            base_path: Base path for saving files
            max_time_per_year: Maximum seconds to spend on each year
            
        Returns:
            Dictionary with results for each year
        """
        all_results = {}
        failed_years = []
        
        print(f"\nTotal years to scrape: {len(years)}")
        print(f"Max time per year: {max_time_per_year} seconds")
        
        for i, year in enumerate(years):
            year_start = time.time()
            print(f"\n[{i+1}/{len(years)}] Processing year {year}...")
            
            try:
                # Restart browser for each year to avoid timeout issues
                if i > 0:
                    print(f"Restarting browser for next year...")
                    try:
                        self.close_driver()
                    except:
                        pass
                    time.sleep(3)
                    self.setup_driver()
                
                results = self.scrape_year(year, base_path, max_time=max_time_per_year)
                all_results[year] = results
                
                if not results.get("success", False):
                    failed_years.append(year)
                
                year_elapsed = time.time() - year_start
                print(f"Year {year} completed in {year_elapsed:.1f}s")
                
                # Be respectful with rate limiting - longer wait between years
                if year != years[-1]:
                    print(f"\nWaiting 10 seconds before next request...")
                    time.sleep(10)
                    
            except KeyboardInterrupt:
                print(f"\n⚠ Keyboard interrupt detected. Stopping...")
                all_results[year] = {"success": False, "error": "Interrupted by user"}
                break
                    
            except Exception as e:
                print(f"✗ Error scraping {year}: {e}")
                all_results[year] = {"success": False, "error": str(e)}
                failed_years.append(year)
                
                # Try to recover for next year
                try:
                    self.close_driver()
                    time.sleep(5)
                    self.setup_driver()
                except Exception as recovery_error:
                    print(f"  ⚠ Could not recover browser: {recovery_error}")
        
        if failed_years:
            print(f"\n⚠ Failed years: {failed_years}")
                
        return all_results


def main():
    """Main function to run the scraper"""
    # Configuration
    YEARS_TO_SCRAPE = [2022]  # 2000-2022
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
    HEADLESS = True  # Set to False to see the browser in action
    MAX_TIME_PER_YEAR = 180  # 3 minutes max per year
    
    print("\n" + "="*60)
    print("Baseball Statistics Web Scraper")
    print("="*60)
    print(f"Years to scrape: {YEARS_TO_SCRAPE}")
    print(f"Output directory: {os.path.join(BASE_PATH, 'Data')}")
    print(f"Headless mode: {HEADLESS}")
    print(f"Max time per year: {MAX_TIME_PER_YEAR} seconds")
    print("="*60 + "\n")
    
    # Initialize scraper
    scraper = BaseballScraper(headless=HEADLESS)
    
    try:
        # Setup WebDriver
        scraper.setup_driver()
        
        # Scrape data
        results = scraper.scrape_multiple_years(YEARS_TO_SCRAPE, BASE_PATH, max_time_per_year=MAX_TIME_PER_YEAR)
        
        # Summary
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        
        for year, result in results.items():
            if result.get("success"):
                print(f"\n{year}:")
                print(f"  ✓ Files created: {len(result.get('files_created', []))}")
                for filepath in result.get('files_created', []):
                    print(f"    - {os.path.basename(filepath)}")
            else:
                print(f"\n{year}:")
                print(f"  ✗ Failed: {result.get('error', 'Unknown error')}")
                
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Clean up
        scraper.close_driver()
        
    print("\n" + "="*60)
    print("Scraping complete!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
