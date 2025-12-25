"""
Baseball Statistics Data Cleaning
Standardizes team names and abbreviations across all years to match 2025 conventions
"""

import os
import pandas as pd
import re
from pathlib import Path


# =============================================================================
# 2025 STANDARD TEAM NAMES AND ABBREVIATIONS
# All historical data will be converted to match these standards
# =============================================================================

# Standard team names (2025 conventions)
STANDARD_TEAM_NAMES = {
    "Arizona Diamondbacks": "Arizona Diamondbacks",
    "Athletics": "Athletics",  # 2025 name (no longer "Oakland Athletics")
    "Atlanta Braves": "Atlanta Braves",
    "Baltimore Orioles": "Baltimore Orioles",
    "Boston Red Sox": "Boston Red Sox",
    "Chicago Cubs": "Chicago Cubs",
    "Chicago White Sox": "Chicago White Sox",
    "Cincinnati Reds": "Cincinnati Reds",
    "Cleveland Guardians": "Cleveland Guardians",
    "Colorado Rockies": "Colorado Rockies",
    "Detroit Tigers": "Detroit Tigers",
    "Houston Astros": "Houston Astros",
    "Kansas City Royals": "Kansas City Royals",
    "Los Angeles Angels": "Los Angeles Angels",
    "Los Angeles Dodgers": "Los Angeles Dodgers",
    "Miami Marlins": "Miami Marlins",
    "Milwaukee Brewers": "Milwaukee Brewers",
    "Minnesota Twins": "Minnesota Twins",
    "New York Mets": "New York Mets",
    "New York Yankees": "New York Yankees",
    "Philadelphia Phillies": "Philadelphia Phillies",
    "Pittsburgh Pirates": "Pittsburgh Pirates",
    "San Diego Padres": "San Diego Padres",
    "Seattle Mariners": "Seattle Mariners",
    "San Francisco Giants": "San Francisco Giants",
    "St. Louis Cardinals": "St. Louis Cardinals",
    "Tampa Bay Rays": "Tampa Bay Rays",
    "Texas Rangers": "Texas Rangers",
    "Toronto Blue Jays": "Toronto Blue Jays",
    "Washington Nationals": "Washington Nationals",
}

# Standard abbreviations (2025 conventions)
STANDARD_ABBREVIATIONS = {
    "ARI": "ARI",   # Arizona Diamondbacks
    "ATH": "ATH",   # Athletics (2025 standard - was OAK)
    "ATL": "ATL",   # Atlanta Braves
    "BAL": "BAL",   # Baltimore Orioles
    "BOS": "BOS",   # Boston Red Sox
    "CHC": "CHC",   # Chicago Cubs
    "CHW": "CHW",   # Chicago White Sox (sometimes CWS)
    "CIN": "CIN",   # Cincinnati Reds
    "CLE": "CLE",   # Cleveland Guardians
    "COL": "COL",   # Colorado Rockies
    "DET": "DET",   # Detroit Tigers
    "HOU": "HOU",   # Houston Astros
    "KCR": "KCR",   # Kansas City Royals (sometimes KC or KAN)
    "LAA": "LAA",   # Los Angeles Angels
    "LAD": "LAD",   # Los Angeles Dodgers
    "MIA": "MIA",   # Miami Marlins
    "MIL": "MIL",   # Milwaukee Brewers
    "MIN": "MIN",   # Minnesota Twins
    "NYM": "NYM",   # New York Mets
    "NYY": "NYY",   # New York Yankees
    "PHI": "PHI",   # Philadelphia Phillies
    "PIT": "PIT",   # Pittsburgh Pirates
    "SDP": "SDP",   # San Diego Padres (sometimes SD)
    "SEA": "SEA",   # Seattle Mariners
    "SFG": "SFG",   # San Francisco Giants (sometimes SF)
    "STL": "STL",   # St. Louis Cardinals
    "TBR": "TBR",   # Tampa Bay Rays (sometimes TB)
    "TEX": "TEX",   # Texas Rangers
    "TOR": "TOR",   # Toronto Blue Jays
    "WSN": "WSN",   # Washington Nationals (sometimes WAS or WSH)
}

# =============================================================================
# NAME MAPPINGS: Historical names -> Standard 2025 names
# Add new mappings here as you encounter them in older data
# =============================================================================

NAME_MAPPINGS = {
    # Oakland Athletics -> Athletics (2024 relocation)
    "Oakland Athletics": "Athletics",
    "Oakland A's": "Athletics",
    "Oakland": "Athletics",
    
    # Historical Cleveland name changes
    "Cleveland Indians": "Cleveland Guardians",
    
    # Historical Washington names
    "Washington Nationals": "Washington Nationals",
    
    # Common variations
    "LA Angels": "Los Angeles Angels",
    "LA Dodgers": "Los Angeles Dodgers",
    "NY Mets": "New York Mets",
    "NY Yankees": "New York Yankees",
    "SF Giants": "San Francisco Giants",
    "SD Padres": "San Diego Padres",
    "TB Rays": "Tampa Bay Rays",
    "KC Royals": "Kansas City Royals",
    
    # Angels historical names
    "Los Angeles Angels of Anaheim": "Los Angeles Angels",
    "Anaheim Angels": "Los Angeles Angels",
    "California Angels": "Los Angeles Angels",
    
    # Marlins historical name
    "Florida Marlins": "Miami Marlins",
    
    # Rays historical name
    "Tampa Bay Devil Rays": "Tampa Bay Rays",
    
    # Nationals historical name (for very old data)
    "Montreal Expos": "Washington Nationals",
}

# =============================================================================
# ABBREVIATION MAPPINGS: Old abbreviations -> Standard 2025 abbreviations
# Add new mappings here as you encounter them in older data
# =============================================================================

ABBREVIATION_MAPPINGS = {
    # Oakland -> Athletics
    "OAK": "ATH",
    
    # Common variations
    "CWS": "CHW",   # Chicago White Sox
    "KC": "KCR",    # Kansas City Royals
    "KAN": "KCR",   # Kansas City Royals
    "SD": "SDP",    # San Diego Padres
    # Note: SF and TB are also stat columns (Sacrifice Flies, Total Bases)
    # so we don't auto-convert those abbreviations
    "WAS": "WSN",   # Washington Nationals
    "WSH": "WSN",   # Washington Nationals
    "ANA": "LAA",   # Anaheim Angels -> LA Angels
    "CAL": "LAA",   # California Angels -> LA Angels
    "FLA": "MIA",   # Florida Marlins -> Miami Marlins
    "MON": "WSN",   # Montreal Expos -> Washington Nationals
    "TBD": "TBR",   # Tampa Bay Devil Rays
    "CLV": "CLE",   # Cleveland (old)
}


class BaseballDataCleaner:
    """Cleans and standardizes baseball statistics data"""
    
    def __init__(self, data_path):
        """
        Initialize the cleaner
        
        Args:
            data_path: Path to the Data folder containing year subfolders
        """
        self.data_path = Path(data_path)
        self.files_processed = []
        self.changes_made = []
        
    def standardize_team_name(self, name):
        """
        Convert a team name to the standard 2025 format
        
        Args:
            name: Original team name
            
        Returns:
            Standardized team name
        """
        if pd.isna(name):
            return name
            
        name = str(name).strip()
        
        # Check if it's already standard
        if name in STANDARD_TEAM_NAMES:
            return name
            
        # Check mappings
        if name in NAME_MAPPINGS:
            return NAME_MAPPINGS[name]
            
        return name
    
    def standardize_abbreviation(self, abbr):
        """
        Convert an abbreviation to the standard 2025 format
        
        Args:
            abbr: Original abbreviation
            
        Returns:
            Standardized abbreviation
        """
        if pd.isna(abbr):
            return abbr
            
        abbr = str(abbr).strip().upper()
        
        # Check if it's already standard
        if abbr in STANDARD_ABBREVIATIONS:
            return abbr
            
        # Check mappings
        if abbr in ABBREVIATION_MAPPINGS:
            return ABBREVIATION_MAPPINGS[abbr]
            
        return abbr
    
    def standardize_cell_value(self, value):
        """
        Standardize a cell value that may contain team names or abbreviations
        This handles cells like "Philadelphia Phillies18.5" or "PHI14.8"
        
        Args:
            value: Cell value to standardize
            
        Returns:
            Standardized cell value
        """
        if pd.isna(value):
            return value
            
        value_str = str(value)
        original_value = value_str
        
        # Check for team names with numbers (like "Philadelphia Phillies18.5")
        for old_name, new_name in NAME_MAPPINGS.items():
            if old_name in value_str:
                value_str = value_str.replace(old_name, new_name)
        
        # Check for abbreviations with numbers (like "OAK4.0")
        # Use regex to find abbreviation patterns
        for old_abbr, new_abbr in ABBREVIATION_MAPPINGS.items():
            # Match abbreviation followed by optional number/punctuation
            pattern = r'\b' + old_abbr + r'(?=[\d\.\-]|$)'
            value_str = re.sub(pattern, new_abbr, value_str, flags=re.IGNORECASE)
        
        return value_str
    
    def clean_dataframe(self, df, filename):
        """
        Clean and standardize a DataFrame
        
        Args:
            df: pandas DataFrame to clean
            filename: Name of the file (for logging)
            
        Returns:
            Cleaned DataFrame and list of changes made
        """
        changes = []
        df_clean = df.copy()
        
        # Identify columns that likely contain team names
        team_name_columns = ['Tm', 'Team', 'team', 'Name']
        
        for col in df_clean.columns:
            if col in team_name_columns:
                # This column contains team names
                for idx, value in df_clean[col].items():
                    new_value = self.standardize_team_name(value)
                    if str(value) != str(new_value):
                        changes.append({
                            'file': filename,
                            'column': col,
                            'row': idx,
                            'old_value': value,
                            'new_value': new_value,
                            'type': 'team_name'
                        })
                        df_clean.at[idx, col] = new_value
            else:
                # Check all cells for embedded team names/abbreviations
                for idx, value in df_clean[col].items():
                    new_value = self.standardize_cell_value(value)
                    if str(value) != str(new_value):
                        changes.append({
                            'file': filename,
                            'column': col,
                            'row': idx,
                            'old_value': value,
                            'new_value': new_value,
                            'type': 'embedded'
                        })
                        df_clean.at[idx, col] = new_value
        
        # Also check column headers for abbreviations
        new_columns = []
        for col in df_clean.columns:
            new_col = self.standardize_cell_value(col)
            if col != new_col:
                changes.append({
                    'file': filename,
                    'column': 'header',
                    'row': 'N/A',
                    'old_value': col,
                    'new_value': new_col,
                    'type': 'header'
                })
            new_columns.append(new_col)
        df_clean.columns = new_columns
        
        return df_clean, changes
    
    def process_file(self, filepath):
        """
        Process a single CSV file
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            Tuple of (success, changes_count)
        """
        try:
            df = pd.read_csv(filepath)
            filename = filepath.name
            
            df_clean, changes = self.clean_dataframe(df, filename)
            
            if changes:
                # Save the cleaned file
                df_clean.to_csv(filepath, index=False)
                self.changes_made.extend(changes)
                print(f"  ✓ {filename}: {len(changes)} changes made")
                return True, len(changes)
            else:
                print(f"  ○ {filename}: No changes needed")
                return True, 0
                
        except Exception as e:
            print(f"  ✗ {filepath.name}: Error - {e}")
            return False, 0
    
    def process_year(self, year):
        """
        Process all files for a specific year
        
        Args:
            year: Year to process
            
        Returns:
            Dictionary with results
        """
        year_path = self.data_path / str(year)
        
        if not year_path.exists():
            print(f"⚠ Year folder not found: {year}")
            return {"success": False, "error": "Folder not found"}
        
        print(f"\n--- Processing {year} ---")
        
        csv_files = list(year_path.glob("*.csv"))
        results = {"files_processed": 0, "changes_made": 0, "errors": 0}
        
        for filepath in csv_files:
            success, changes = self.process_file(filepath)
            if success:
                results["files_processed"] += 1
                results["changes_made"] += changes
            else:
                results["errors"] += 1
        
        return results
    
    def process_all_years(self):
        """
        Process all year folders in the data directory
        
        Returns:
            Summary of all changes made
        """
        print("\n" + "="*60)
        print("Baseball Data Cleaning - Standardizing Team Names")
        print("="*60)
        print(f"Data path: {self.data_path}")
        print("Standard: 2025 team names and abbreviations")
        print("="*60)
        
        # Find all year folders
        year_folders = sorted([
            d.name for d in self.data_path.iterdir() 
            if d.is_dir() and d.name.isdigit()
        ])
        
        print(f"\nFound {len(year_folders)} year folders: {', '.join(year_folders)}")
        
        all_results = {}
        total_changes = 0
        
        for year in year_folders:
            results = self.process_year(year)
            all_results[year] = results
            total_changes += results.get("changes_made", 0)
        
        # Print summary
        print("\n" + "="*60)
        print("CLEANING SUMMARY")
        print("="*60)
        
        for year, results in all_results.items():
            print(f"\n{year}:")
            print(f"  Files processed: {results.get('files_processed', 0)}")
            print(f"  Changes made: {results.get('changes_made', 0)}")
            if results.get('errors', 0) > 0:
                print(f"  Errors: {results.get('errors', 0)}")
        
        print(f"\n{'='*60}")
        print(f"TOTAL CHANGES: {total_changes}")
        print(f"{'='*60}\n")
        
        # Print detailed changes if any
        if self.changes_made:
            print("\nDetailed Changes:")
            print("-" * 80)
            for change in self.changes_made:
                print(f"  {change['file']} | {change['column']} | "
                      f"'{change['old_value']}' -> '{change['new_value']}'")
        
        return all_results
    
    def generate_report(self, output_path=None):
        """
        Generate a detailed report of all changes made
        
        Args:
            output_path: Optional path to save report as CSV
        """
        if not self.changes_made:
            print("No changes were made.")
            return None
            
        df_report = pd.DataFrame(self.changes_made)
        
        if output_path:
            df_report.to_csv(output_path, index=False)
            print(f"\nReport saved to: {output_path}")
            
        return df_report


def main():
    """Main function to run the data cleaning"""
    # Get the script's directory
    script_dir = Path(__file__).parent
    data_path = script_dir / "Data"
    
    # Initialize cleaner
    cleaner = BaseballDataCleaner(data_path)
    
    # Process all years
    results = cleaner.process_all_years()
    
    # Optionally generate a report
    # cleaner.generate_report(script_dir / "cleaning_report.csv")
    
    print("\nData cleaning complete!")
    print("\nKey standardizations applied:")
    print("  • 'Oakland Athletics' -> 'Athletics'")
    print("  • 'OAK' -> 'ATH'")
    print("\nTo add more mappings, edit NAME_MAPPINGS and ABBREVIATION_MAPPINGS")
    print("in this file.")


if __name__ == "__main__":
    main()
