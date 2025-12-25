"""
MLB Team Salary Data Cleaning
Cleans and standardizes salary data scraped from stevetheump.com
- Identifies the correct payroll column (highest value per row)
- Removes average/median salary columns
- Standardizes team names to match 2025 conventions
- Cleans currency formatting (removes $, commas, M suffix)
"""

import os
import pandas as pd
import re
from pathlib import Path


# =============================================================================
# TEAM NAME STANDARDIZATION MAPPINGS
# Maps historical and variant team names to current 2025 standard names
# =============================================================================

TEAM_NAME_MAPPINGS = {
    # Oakland Athletics variations (relocated in 2024)
    "Oakland Athletics": "Athletics",
    "Oakland A's": "Athletics",
    "Oakland": "Athletics",
    "A's": "Athletics",
    "Athletics": "Athletics",
    
    # Cleveland (renamed 2022)
    "Cleveland Indians": "Cleveland Guardians",
    "Cleveland": "Cleveland Guardians",
    "Indians": "Cleveland Guardians",
    "Guardians": "Cleveland Guardians",
    
    # Miami Marlins (renamed 2012)
    "Florida Marlins": "Miami Marlins",
    "Florida": "Miami Marlins",
    "Marlins": "Miami Marlins",
    
    # Washington Nationals (relocated 2005)
    "Montreal Expos": "Washington Nationals",
    "Montreal": "Washington Nationals",
    "Expos": "Washington Nationals",
    "Washington": "Washington Nationals",
    "Nationals": "Washington Nationals",
    
    # Tampa Bay Rays (renamed 2008)
    "Tampa Bay Devil Rays": "Tampa Bay Rays",
    "Devil Rays": "Tampa Bay Rays",
    "Tampa Bay": "Tampa Bay Rays",
    "Rays": "Tampa Bay Rays",
    
    # Los Angeles Angels variations
    "Anaheim Angels": "Los Angeles Angels",
    "California Angels": "Los Angeles Angels",
    "Los Angeles Angels of Anaheim": "Los Angeles Angels",
    "Anaheim": "Los Angeles Angels",
    "LA Angels": "Los Angeles Angels",
    "Angels": "Los Angeles Angels",
    
    # Common abbreviations and short names
    "NY Yankees": "New York Yankees",
    "N.Y. Yankees": "New York Yankees",
    "Yankees": "New York Yankees",
    
    "NY Mets": "New York Mets",
    "N.Y. Mets": "New York Yankees",
    "Mets": "New York Mets",
    
    "LA Dodgers": "Los Angeles Dodgers",
    "Los Angeles": "Los Angeles Dodgers",  # Be careful - context dependent
    "Dodgers": "Los Angeles Dodgers",
    
    "SF Giants": "San Francisco Giants",
    "San Francisco": "San Francisco Giants",
    "Giants": "San Francisco Giants",
    
    "SD Padres": "San Diego Padres",
    "San Diego": "San Diego Padres",
    "Padres": "San Diego Padres",
    
    "Boston": "Boston Red Sox",
    "Red Sox": "Boston Red Sox",
    
    "Chicago Cubs": "Chicago Cubs",
    "Cubs": "Chicago Cubs",
    
    "Chicago White Sox": "Chicago White Sox",
    "Ch. White Sox": "Chicago White Sox",
    "White Sox": "Chicago White Sox",
    
    "Philadelphia": "Philadelphia Phillies",
    "Phillies": "Philadelphia Phillies",
    
    "Houston": "Houston Astros",
    "Astros": "Houston Astros",
    
    "Atlanta": "Atlanta Braves",
    "Braves": "Atlanta Braves",
    
    "St. Louis": "St. Louis Cardinals",
    "St Louis Cardinals": "St. Louis Cardinals",
    "Cardinals": "St. Louis Cardinals",
    
    "Texas": "Texas Rangers",
    "Rangers": "Texas Rangers",
    
    "Seattle": "Seattle Mariners",
    "Mariners": "Seattle Mariners",
    
    "Detroit": "Detroit Tigers",
    "Tigers": "Detroit Tigers",
    
    "Baltimore": "Baltimore Orioles",
    "Orioles": "Baltimore Orioles",
    
    "Minnesota": "Minnesota Twins",
    "Twins": "Minnesota Twins",
    
    "Kansas City": "Kansas City Royals",
    "KC Royals": "Kansas City Royals",
    "Royals": "Kansas City Royals",
    
    "Colorado": "Colorado Rockies",
    "Rockies": "Colorado Rockies",
    
    "Arizona": "Arizona Diamondbacks",
    "Diamondbacks": "Arizona Diamondbacks",
    "D-backs": "Arizona Diamondbacks",
    
    "Cincinnati": "Cincinnati Reds",
    "Reds": "Cincinnati Reds",
    
    "Pittsburgh": "Pittsburgh Pirates",
    "Pirates": "Pittsburgh Pirates",
    
    "Milwaukee": "Milwaukee Brewers",
    "Brewers": "Milwaukee Brewers",
    
    "Toronto": "Toronto Blue Jays",
    "Blue Jays": "Toronto Blue Jays",
}

# Full team names for validation
VALID_TEAM_NAMES = [
    "Arizona Diamondbacks",
    "Athletics",
    "Atlanta Braves",
    "Baltimore Orioles",
    "Boston Red Sox",
    "Chicago Cubs",
    "Chicago White Sox",
    "Cincinnati Reds",
    "Cleveland Guardians",
    "Colorado Rockies",
    "Detroit Tigers",
    "Houston Astros",
    "Kansas City Royals",
    "Los Angeles Angels",
    "Los Angeles Dodgers",
    "Miami Marlins",
    "Milwaukee Brewers",
    "Minnesota Twins",
    "New York Mets",
    "New York Yankees",
    "Philadelphia Phillies",
    "Pittsburgh Pirates",
    "San Diego Padres",
    "San Francisco Giants",
    "Seattle Mariners",
    "St. Louis Cardinals",
    "Tampa Bay Rays",
    "Texas Rangers",
    "Toronto Blue Jays",
    "Washington Nationals",
]

# =============================================================================
# HARDCODED DATA FOR 1998 AND 1999
# The scraper gets incorrect data for these years, so we hardcode the correct values
# =============================================================================

HARDCODED_1998 = [
    {"Tm": "Baltimore Orioles", "Payroll": 71860921},
    {"Tm": "New York Yankees", "Payroll": 65663698},
    {"Tm": "Los Angeles Dodgers", "Payroll": 62806667},
    {"Tm": "Atlanta Braves", "Payroll": 61708000},
    {"Tm": "Texas Rangers", "Payroll": 60519595},
    {"Tm": "Cleveland Guardians", "Payroll": 59543165},
    {"Tm": "Boston Red Sox", "Payroll": 59497000},
    {"Tm": "New York Mets", "Payroll": 58660665},
    {"Tm": "San Diego Padres", "Payroll": 53066166},
    {"Tm": "Chicago Cubs", "Payroll": 49816000},
    {"Tm": "San Francisco Giants", "Payroll": 48514715},
    {"Tm": "Los Angeles Angels", "Payroll": 48389000},
    {"Tm": "Houston Astros", "Payroll": 48304000},
    {"Tm": "Colorado Rockies", "Payroll": 47714648},
    {"Tm": "St. Louis Cardinals", "Payroll": 44090854},
    {"Tm": "Seattle Mariners", "Payroll": 43698136},
    {"Tm": "Kansas City Royals", "Payroll": 35610000},
    {"Tm": "Chicago White Sox", "Payroll": 35180000},
    {"Tm": "Toronto Blue Jays", "Payroll": 34158500},
    {"Tm": "Milwaukee Brewers", "Payroll": 31897903},
    {"Tm": "Arizona Diamondbacks", "Payroll": 31614500},
    {"Tm": "Philadelphia Phillies", "Payroll": 28622500},
    {"Tm": "Tampa Bay Rays", "Payroll": 27370000},
    {"Tm": "Minnesota Twins", "Payroll": 24527500},
    {"Tm": "Athletics", "Payroll": 22463500},
    {"Tm": "Cincinnati Reds", "Payroll": 20707333},
    {"Tm": "Detroit Tigers", "Payroll": 19237500},
    {"Tm": "Miami Marlins", "Payroll": 15141000},
    {"Tm": "Pittsburgh Pirates", "Payroll": 13695000},
    {"Tm": "Washington Nationals", "Payroll": 8317000},
]

HARDCODED_1999 = [
    {"Tm": "New York Yankees", "Payroll": 88180712},
    {"Tm": "Texas Rangers", "Payroll": 81576598},
    {"Tm": "Atlanta Braves", "Payroll": 74890000},
    {"Tm": "Cleveland Guardians", "Payroll": 73278458},
    {"Tm": "Baltimore Orioles", "Payroll": 72198363},
    {"Tm": "Boston Red Sox", "Payroll": 71725000},
    {"Tm": "New York Mets", "Payroll": 71506427},
    {"Tm": "Los Angeles Dodgers", "Payroll": 71115786},
    {"Tm": "Arizona Diamondbacks", "Payroll": 70496000},
    {"Tm": "Chicago Cubs", "Payroll": 55443500},
    {"Tm": "Colorado Rockies", "Payroll": 54442505},
    {"Tm": "Houston Astros", "Payroll": 54339000},
    {"Tm": "Los Angeles Angels", "Payroll": 49868167},
    {"Tm": "Toronto Blue Jays", "Payroll": 48455333},
    {"Tm": "St. Louis Cardinals", "Payroll": 46173195},
    {"Tm": "San Francisco Giants", "Payroll": 45959557},
    {"Tm": "San Diego Padres", "Payroll": 45832180},
    {"Tm": "Seattle Mariners", "Payroll": 44396336},
    {"Tm": "Milwaukee Brewers", "Payroll": 42927395},
    {"Tm": "Cincinnati Reds", "Payroll": 42142761},
    {"Tm": "Tampa Bay Rays", "Payroll": 38027500},
    {"Tm": "Detroit Tigers", "Payroll": 34959667},
    {"Tm": "Philadelphia Phillies", "Payroll": 30568167},
    {"Tm": "Chicago White Sox", "Payroll": 24535000},
    {"Tm": "Athletics", "Payroll": 24175333},
    {"Tm": "Pittsburgh Pirates", "Payroll": 24167667},
    {"Tm": "Kansas City Royals", "Payroll": 16557000},
    {"Tm": "Washington Nationals", "Payroll": 16413000},
    {"Tm": "Minnesota Twins", "Payroll": 16345000},
    {"Tm": "Miami Marlins", "Payroll": 15150000},
]


class SalaryDataCleaner:
    """Cleans and standardizes MLB salary data"""
    
    def __init__(self, data_path):
        """
        Initialize the cleaner
        
        Args:
            data_path: Path to the Data folder
        """
        self.data_path = Path(data_path)
        self.changes_made = []
        
    def clean_currency(self, value):
        """
        Clean currency values - remove $, commas, handle M suffix
        
        Args:
            value: Raw currency string
            
        Returns:
            Integer payroll value or None
        """
        if pd.isna(value):
            return None
            
        value_str = str(value).strip()
        
        # Handle "N/A", "-", empty strings
        if value_str in ['', '-', 'N/A', 'nan', 'None']:
            return None
        
        # Remove $ sign and spaces
        value_str = value_str.replace('$', '').replace(' ', '')
        
        # Handle "M" suffix (millions)
        if value_str.upper().endswith('M'):
            try:
                # Remove M and convert to full number
                num = float(value_str[:-1].replace(',', ''))
                return int(num * 1_000_000)
            except ValueError:
                pass
        
        # Remove commas and try to convert
        value_str = value_str.replace(',', '')
        
        try:
            # Handle potential decimal values
            return int(float(value_str))
        except ValueError:
            return None
    
    def standardize_team_name(self, name):
        """
        Standardize team name to 2025 convention
        
        Args:
            name: Raw team name
            
        Returns:
            Standardized team name
        """
        if pd.isna(name):
            return None
            
        name = str(name).strip()
        
        # Remove common prefixes like "1.", "2.", rank numbers
        name = re.sub(r'^[\d]+\.?\s*', '', name)
        name = name.strip()
        
        # Check direct mapping
        if name in TEAM_NAME_MAPPINGS:
            return TEAM_NAME_MAPPINGS[name]
        
        # Check if already valid
        if name in VALID_TEAM_NAMES:
            return name
        
        # Try partial matching
        name_lower = name.lower()
        for key, value in TEAM_NAME_MAPPINGS.items():
            if key.lower() in name_lower or name_lower in key.lower():
                return value
        
        # Return original if no match (will be flagged)
        return name
    
    def identify_team_column(self, df):
        """
        Identify which column contains team names
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Column name or None
        """
        team_keywords = ['team', 'tm', 'name', 'club']
        
        # First check column names
        for col in df.columns:
            col_lower = str(col).lower()
            if any(kw in col_lower for kw in team_keywords):
                return col
        
        # Check first column (often unnamed but contains teams)
        first_col = df.columns[0]
        sample_values = df[first_col].dropna().head(10).astype(str)
        
        team_matches = 0
        for val in sample_values:
            clean_val = re.sub(r'^[\d]+\.?\s*', '', str(val)).strip()
            if clean_val in TEAM_NAME_MAPPINGS or clean_val in VALID_TEAM_NAMES:
                team_matches += 1
        
        if team_matches >= 3:
            return first_col
        
        # Check all columns for team name content
        for col in df.columns:
            sample_values = df[col].dropna().head(10).astype(str)
            team_matches = 0
            for val in sample_values:
                clean_val = re.sub(r'^[\d]+\.?\s*', '', str(val)).strip()
                if clean_val in TEAM_NAME_MAPPINGS or clean_val in VALID_TEAM_NAMES:
                    team_matches += 1
            if team_matches >= 3:
                return col
        
        return df.columns[0]  # Default to first column
    
    def identify_payroll_column(self, df, team_col):
        """
        Identify the column with total team payroll (highest values)
        Ignores average salary columns
        
        Args:
            df: DataFrame to analyze
            team_col: Column containing team names
            
        Returns:
            Column name for payroll
        """
        # Keywords to AVOID (average/median salary columns)
        avoid_keywords = ['average', 'avg', 'median', 'mean', 'per', 'minimum', 'min']
        
        # Keywords that indicate payroll columns
        payroll_keywords = ['payroll', 'total', 'salary', 'opening']
        
        candidate_columns = []
        
        for col in df.columns:
            if col == team_col:
                continue
                
            col_lower = str(col).lower()
            
            # Skip columns with avoid keywords
            if any(kw in col_lower for kw in avoid_keywords):
                continue
            
            # Try to convert column to numeric and check if values are reasonable payrolls
            try:
                # Clean and convert values
                cleaned_values = df[col].apply(self.clean_currency)
                valid_values = cleaned_values.dropna()
                
                if len(valid_values) < 10:
                    continue
                
                max_val = valid_values.max()
                median_val = valid_values.median()
                
                # Payroll should be in millions (> 10M typically)
                # Average player salary is usually < 10M
                if max_val > 10_000_000 and median_val > 5_000_000:
                    # Prioritize columns with payroll keywords
                    priority = 2 if any(kw in col_lower for kw in payroll_keywords) else 1
                    candidate_columns.append((col, max_val, priority))
                    
            except Exception:
                continue
        
        if not candidate_columns:
            # Fallback: just find column with highest max value
            for col in df.columns:
                if col == team_col:
                    continue
                try:
                    cleaned_values = df[col].apply(self.clean_currency)
                    valid_values = cleaned_values.dropna()
                    if len(valid_values) >= 10:
                        max_val = valid_values.max()
                        if max_val and max_val > 0:
                            candidate_columns.append((col, max_val, 0))
                except Exception:
                    continue
        
        if candidate_columns:
            # Sort by priority (desc), then by max value (desc)
            candidate_columns.sort(key=lambda x: (x[2], x[1]), reverse=True)
            return candidate_columns[0][0]
        
        return None
    
    def clean_dataframe(self, df, year):
        """
        Clean a salary DataFrame
        
        Args:
            df: Raw DataFrame
            year: Year for context
            
        Returns:
            Cleaned DataFrame with columns [Tm, Payroll]
        """
        if df is None or df.empty:
            return None
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove rows that look like headers repeated or league averages
        df = df[~df.iloc[:, 0].astype(str).str.contains('league|average|avg|total|rank', case=False, na=False)]
        
        # Identify team column
        team_col = self.identify_team_column(df)
        print(f"    Team column: {team_col}")
        
        # Identify payroll column
        payroll_col = self.identify_payroll_column(df, team_col)
        print(f"    Payroll column: {payroll_col}")
        
        if payroll_col is None:
            print(f"    ⚠ Could not identify payroll column")
            return None
        
        # Create clean DataFrame
        clean_data = []
        
        for _, row in df.iterrows():
            team_raw = row[team_col]
            payroll_raw = row[payroll_col]
            
            # Standardize team name
            team = self.standardize_team_name(team_raw)
            
            # Clean payroll value
            payroll = self.clean_currency(payroll_raw)
            
            # Skip invalid rows
            if team and payroll and payroll > 0:
                # Validate team name
                if team in VALID_TEAM_NAMES:
                    clean_data.append({
                        'Tm': team,
                        'Payroll': payroll
                    })
                else:
                    # Try harder to match
                    matched = False
                    for valid_name in VALID_TEAM_NAMES:
                        if team.lower() in valid_name.lower() or valid_name.lower() in team.lower():
                            clean_data.append({
                                'Tm': valid_name,
                                'Payroll': payroll
                            })
                            matched = True
                            break
                    
                    if not matched and len(team) > 3:  # Skip obvious non-teams
                        print(f"    ⚠ Unrecognized team: '{team}' (raw: '{team_raw}')")
        
        if clean_data:
            result_df = pd.DataFrame(clean_data)
            # Sort by payroll descending
            result_df = result_df.sort_values('Payroll', ascending=False).reset_index(drop=True)
            return result_df
        
        return None
    
    def process_file(self, filepath, year):
        """
        Process a single salary CSV file
        
        Args:
            filepath: Path to the CSV file
            year: Year of the data
            
        Returns:
            Tuple of (success, row_count)
        """
        try:
            # Use hardcoded data for 1998 and 1999
            if year == 1998:
                print(f"  Processing {filepath.name}...")
                print(f"    Using hardcoded data for 1998")
                clean_df = pd.DataFrame(HARDCODED_1998)
                clean_df.to_csv(filepath, index=False)
                print(f"    ✓ Saved: {len(clean_df)} teams (hardcoded)")
                return True, len(clean_df)
            
            if year == 1999:
                print(f"  Processing {filepath.name}...")
                print(f"    Using hardcoded data for 1999")
                clean_df = pd.DataFrame(HARDCODED_1999)
                clean_df.to_csv(filepath, index=False)
                print(f"    ✓ Saved: {len(clean_df)} teams (hardcoded)")
                return True, len(clean_df)
            
            df = pd.read_csv(filepath)
            
            print(f"  Processing {filepath.name}...")
            print(f"    Original shape: {df.shape}")
            
            # Clean the dataframe
            clean_df = self.clean_dataframe(df, year)
            
            if clean_df is not None and not clean_df.empty:
                # Save cleaned file
                clean_df.to_csv(filepath, index=False)
                print(f"    ✓ Cleaned: {len(clean_df)} teams")
                return True, len(clean_df)
            else:
                print(f"    ⚠ No valid data after cleaning")
                return False, 0
                
        except Exception as e:
            print(f"    ✗ Error: {e}")
            return False, 0
    
    def process_all_years(self, start_year=1998, end_year=2025):
        """
        Process all salary files
        
        Args:
            start_year: First year to process
            end_year: Last year to process
            
        Returns:
            Summary of processing
        """
        print("\n" + "="*60)
        print("MLB Salary Data Cleaning")
        print("="*60)
        print(f"Data path: {self.data_path}")
        print(f"Years: {start_year} - {end_year}")
        print("="*60 + "\n")
        
        results = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "total_teams": 0
        }
        
        for year in range(start_year, end_year + 1):
            year_path = self.data_path / str(year)
            salary_file = year_path / f"Salaries_{year}.csv"
            
            if salary_file.exists():
                print(f"\n--- {year} ---")
                success, count = self.process_file(salary_file, year)
                results["processed"] += 1
                
                if success:
                    results["success"] += 1
                    results["total_teams"] += count
                else:
                    results["failed"] += 1
            else:
                print(f"\n--- {year} ---")
                print(f"  ⚠ File not found: Salaries_{year}.csv")
        
        # Summary
        print("\n" + "="*60)
        print("CLEANING SUMMARY")
        print("="*60)
        print(f"Files processed: {results['processed']}")
        print(f"Successful: {results['success']}")
        print(f"Failed: {results['failed']}")
        print(f"Total team records: {results['total_teams']}")
        print("="*60 + "\n")
        
        return results


def main():
    """Main function to run the salary data cleaning"""
    # Get script directory
    script_dir = Path(__file__).parent
    data_path = script_dir / "Data"
    
    # Initialize cleaner
    cleaner = SalaryDataCleaner(data_path)
    
    # Process all years
    results = cleaner.process_all_years(1998, 2025)
    
    print("\nData cleaning complete!")
    print("\nKey standardizations applied:")
    print("  • Team names standardized to 2025 conventions")
    print("  • 'Oakland Athletics' → 'Athletics'")
    print("  • 'Florida Marlins' → 'Miami Marlins'")
    print("  • 'Montreal Expos' → 'Washington Nationals'")
    print("  • 'Cleveland Indians' → 'Cleveland Guardians'")
    print("  • 'Tampa Bay Devil Rays' → 'Tampa Bay Rays'")
    print("  • Currency cleaned (removed $, commas, converted M to full numbers)")
    print("  • Only kept total payroll column (ignored averages)")


if __name__ == "__main__":
    main()
