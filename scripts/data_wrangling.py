####################################
# Import Libraries
####################################
# Data manipulation
import pandas as pd
import numpy as np

# System and utilities
import re
from pathlib import Path

####################################
# Define Directories and Paths
####################################
# Project root is one level up from this script, so it works from any working directory
base_dir = Path(__file__).resolve().parent.parent
wrangled_data_dir = base_dir / "data" / "wrangled"
wrangled_data_dir.mkdir(parents=True, exist_ok=True)

# Define paths for input and output CSV files
scraped_fights_path = base_dir / "data" / "scraped" / "scraped_fight_details.csv"
wrangled_fights_path = wrangled_data_dir / "wrangled_fight_details.csv"

####################################
# Define Helper Functions for Wrangling
####################################

def clean_text(value):
    """Removes leading/trailing whitespace and handles empty strings."""
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else None
    return value

def parse_percentage(value):
    """Converts percentage strings ('X%') to float (X/100), handles '---'."""
    if isinstance(value, str):
        value = value.strip()
        if value.endswith('%'):
            try:
                return float(value.replace('%', '').strip()) / 100.0
            except ValueError:
                return np.nan
    return np.nan

def parse_x_of_y(value):
    """Parses 'X of Y' strings into two numbers (X, Y), handles '---'."""
    if isinstance(value, str):
        value = value.strip()
        if value == '---':
            return np.nan, np.nan
        parts = value.split(' of ')
        if len(parts) == 2:
            try:
                landed = int(parts[0].strip())
                attempted = int(parts[1].strip())
                return landed, attempted
            except ValueError:
                return np.nan, np.nan
    return np.nan, np.nan

def parse_time_to_seconds(value):
    """Converts 'M:SS' time strings to total seconds, handles '---'."""
    if isinstance(value, str):
        value = value.strip()
        if value == '---':
            return np.nan
        parts = value.split(':')
        if len(parts) == 2:
            try:
                minutes = int(parts[0].strip())
                seconds = int(parts[1].strip())
                return (minutes * 60) + seconds
            except ValueError:
                return np.nan
    return np.nan

def parse_round_lengths(value):
    """Gets the round lengths in minutes from time formats like '3 Rnd (5-5-5)' or '1 Rnd + OT (12-3)'."""
    if isinstance(value, str):
        match = re.search(r'\(([\d-]+)\)', value)
        if match:
            return [int(m) for m in match.group(1).split('-')]
    return None

def total_fight_seconds(row):
    """Adds up the full rounds before the last one plus the time in the last round."""
    lengths = parse_round_lengths(row['time_format'])
    if lengths is None or pd.isna(row['last_round']) or pd.isna(row['last_round_time_seconds']):
        return np.nan
    full_rounds = int(row['last_round']) - 1
    return sum(lengths[:full_rounds]) * 60 + row['last_round_time_seconds']

####################################
# Main Data Wrangling Function
####################################
def wrangle_fight_data():
    """
    Loads the scraped fight data, cleans and transforms it, and saves the wrangled data.
    """

    # Load the scraped data
    try:
        df = pd.read_csv(scraped_fights_path, low_memory=False)
        print(f"Successfully loaded {len(df)} rows.")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    # 1. Keep only rows without errors
    if 'error' in df.columns:
        df = df[df['error'].isna()].copy()
    df = df.drop(columns=['error', 'error_type'], errors='ignore')

    # 2. Drop duplicates based on fight_link
    df = df.drop_duplicates(subset=['fight_link'], keep='first')

    # 3. Clean basic text fields
    text_cols = [
        'location', 'name_1', 'name_2', 'stage_name_1', 'stage_name_2',
        'win_loss_1', 'win_loss_2', 'division', 'method', 'referee', 'stoppage_details'
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)

    # 4. Parse Date
    df['date'] = df['date'].apply(clean_text)
    df['date'] = pd.to_datetime(df['date'], format='%B %d, %Y', errors='coerce')

    # 5. Parse Time Format (like '3 Rnd (5-5-5)'), keep the raw format for the fight time below
    df['time_format'] = df['time_format'].apply(clean_text)
    df['time_format_rounds'] = df['time_format'].str.extract(r'(\d+)\s*Rnd', expand=False).astype(float)

    # 6. Parse Simple Numeric Columns (KD, Sub Att, Rev)
    simple_numeric_cols = ['kd_1', 'kd_2', 'sub_att_1', 'sub_att_2', 'rev_1', 'rev_2', 'last_round']

    # Add round-specific simple numeric columns
    for i in range(1, 6):
        simple_numeric_cols.extend([
            f'r{i}_kd_1', f'r{i}_kd_2', f'r{i}_sub_att_1', f'r{i}_sub_att_2',
            f'r{i}_rev_1', f'r{i}_rev_2'
        ])

    for col in simple_numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].apply(clean_text), errors='coerce')

    # 7. Parse Percentage Columns
    perc_cols = ['sig_str_perc_1', 'sig_str_perc_2', 'td_pct_1', 'td_pct_2']

    # Add round-specific percentage columns
    for i in range(1, 6):
        perc_cols.extend([f'r{i}_sig_str_perc_1', f'r{i}_sig_str_perc_2', f'r{i}_td_pct_1', f'r{i}_td_pct_2'])

    for col in perc_cols:
        if col in df.columns:
            df[col] = df[col].apply(parse_percentage)

    # 8. Parse "X of Y" Columns
    x_of_y_cols_map = {
        'sig_str_1': ('sig_str_landed_1', 'sig_str_attempted_1'),
        'sig_str_2': ('sig_str_landed_2', 'sig_str_attempted_2'),
        'total_str_1': ('total_str_landed_1', 'total_str_attempted_1'),
        'total_str_2': ('total_str_landed_2', 'total_str_attempted_2'),
        'td_1': ('td_landed_1', 'td_attempted_1'),
        'td_2': ('td_landed_2', 'td_attempted_2'),
        'sig_str_head_1': ('sig_str_head_landed_1', 'sig_str_head_attempted_1'),
        'sig_str_head_2': ('sig_str_head_landed_2', 'sig_str_head_attempted_2'),
        'sig_str_body_1': ('sig_str_body_landed_1', 'sig_str_body_attempted_1'),
        'sig_str_body_2': ('sig_str_body_landed_2', 'sig_str_body_attempted_2'),
        'sig_str_leg_1': ('sig_str_leg_landed_1', 'sig_str_leg_attempted_1'),
        'sig_str_leg_2': ('sig_str_leg_landed_2', 'sig_str_leg_attempted_2'),
        'sig_str_dist_1': ('sig_str_dist_landed_1', 'sig_str_dist_attempted_1'),
        'sig_str_dist_2': ('sig_str_dist_landed_2', 'sig_str_dist_attempted_2'),
        'sig_str_clinch_1': ('sig_str_clinch_landed_1', 'sig_str_clinch_attempted_1'),
        'sig_str_clinch_2': ('sig_str_clinch_landed_2', 'sig_str_clinch_attempted_2'),
        'sig_str_ground_1': ('sig_str_ground_landed_1', 'sig_str_ground_attempted_1'),
        'sig_str_ground_2': ('sig_str_ground_landed_2', 'sig_str_ground_attempted_2'),
    }
    # Add round-specific "X of Y" columns
    for i in range(1, 6):
        x_of_y_cols_map.update({
            f'r{i}_sig_str_1': (f'r{i}_sig_str_landed_1', f'r{i}_sig_str_attempted_1'),
            f'r{i}_sig_str_2': (f'r{i}_sig_str_landed_2', f'r{i}_sig_str_attempted_2'),
            f'r{i}_total_str_1': (f'r{i}_total_str_landed_1', f'r{i}_total_str_attempted_1'),
            f'r{i}_total_str_2': (f'r{i}_total_str_landed_2', f'r{i}_total_str_attempted_2'),
            f'r{i}_td_1': (f'r{i}_td_landed_1', f'r{i}_td_attempted_1'),
            f'r{i}_td_2': (f'r{i}_td_landed_2', f'r{i}_td_attempted_2'),
            f'r{i}_sig_str_head_1': (f'r{i}_sig_str_head_landed_1', f'r{i}_sig_str_head_attempted_1'),
            f'r{i}_sig_str_head_2': (f'r{i}_sig_str_head_landed_2', f'r{i}_sig_str_head_attempted_2'),
            f'r{i}_sig_str_body_1': (f'r{i}_sig_str_body_landed_1', f'r{i}_sig_str_body_attempted_1'),
            f'r{i}_sig_str_body_2': (f'r{i}_sig_str_body_landed_2', f'r{i}_sig_str_body_attempted_2'),
            f'r{i}_sig_str_leg_1': (f'r{i}_sig_str_leg_landed_1', f'r{i}_sig_str_leg_attempted_1'),
            f'r{i}_sig_str_leg_2': (f'r{i}_sig_str_leg_landed_2', f'r{i}_sig_str_leg_attempted_2'),
            f'r{i}_sig_str_dist_1': (f'r{i}_sig_str_dist_landed_1', f'r{i}_sig_str_dist_attempted_1'),
            f'r{i}_sig_str_dist_2': (f'r{i}_sig_str_dist_landed_2', f'r{i}_sig_str_dist_attempted_2'),
            f'r{i}_sig_str_clinch_1': (f'r{i}_sig_str_clinch_landed_1', f'r{i}_sig_str_clinch_attempted_1'),
            f'r{i}_sig_str_clinch_2': (f'r{i}_sig_str_clinch_landed_2', f'r{i}_sig_str_clinch_attempted_2'),
            f'r{i}_sig_str_ground_1': (f'r{i}_sig_str_ground_landed_1', f'r{i}_sig_str_ground_attempted_1'),
            f'r{i}_sig_str_ground_2': (f'r{i}_sig_str_ground_landed_2', f'r{i}_sig_str_ground_attempted_2'),
        })

    original_x_of_y_cols = []
    for original_col, (landed_col, attempted_col) in x_of_y_cols_map.items():
        if original_col in df.columns:
            original_x_of_y_cols.append(original_col)
            # Apply the parsing function and assign results to new columns
            parsed_data = df[original_col].apply(parse_x_of_y)
            df[landed_col] = parsed_data.apply(lambda x: x[0])
            df[attempted_col] = parsed_data.apply(lambda x: x[1])
            # Convert new columns to appropriate numeric types (Int64 allows NAs)
            df[landed_col] = pd.to_numeric(df[landed_col], errors='coerce').astype('Int64')
            df[attempted_col] = pd.to_numeric(df[attempted_col], errors='coerce').astype('Int64')

    df = df.drop(columns=original_x_of_y_cols, errors='ignore')

    # 9. Parse Time Columns
    time_cols = ['ctrl_1', 'ctrl_2', 'last_round_time']
    # Add round-specific time columns
    for i in range(1, 6):
        time_cols.extend([f'r{i}_ctrl_1', f'r{i}_ctrl_2'])

    for col in time_cols:
        if col in df.columns:
            df[col + '_seconds'] = df[col].apply(parse_time_to_seconds)
            df = df.drop(columns=[col], errors='ignore')

    # Calculate total fight time in seconds (round lengths vary, e.g. '1 Rnd (15)' or '1 Rnd + OT (12-3)')
    df['total_fight_time_seconds'] = df.apply(total_fight_seconds, axis=1)
    df = df.drop(columns=['time_format'], errors='ignore')

    # 10. Report summary of data types and missing values
    print("\n--- Wrangled Data Summary ---")
    print("Data shape:", df.shape)
    print("\nColumn Data Types:\n", df.dtypes)
    missing_values = df.isnull().sum()
    missing_values = missing_values[missing_values > 0]
    if not missing_values.empty:
        print("\nColumns with Missing Values:\n", missing_values)
    else:
        print("\nNo missing values found in the wrangled data.")

    # --- Save Wrangled Data ---
    print(f"\nSaving wrangled data to: {wrangled_fights_path}")
    try:
        df.to_csv(wrangled_fights_path, index=False, date_format='%Y-%m-%d')
        print("Wrangled data successfully saved.")
    except Exception as e:
        print(f"Error saving wrangled data to CSV: {e}")

####################################
# Script Execution
####################################
if __name__ == "__main__":
    wrangle_fight_data()
    print("Data wrangling complete.")
