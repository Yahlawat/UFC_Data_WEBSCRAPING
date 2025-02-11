import pandas as pd

# Load the DataFrames
save_path = "C:\\Users\\ahlaw\\OneDrive - UBC\\Documents\\vscode\\Projects\\UFC_data_webscraping\\Data\\RAW\\"
event_links_df = pd.read_csv(save_path + "event_links.csv")
fights_df = pd.read_csv(save_path + "fight_details.csv")

# Inspect the data
print("Event Links Data:")
print(event_links_df.info())
print("\nFight Details Data:")
print(fights_df.info())

# 1. Clean column names: lowercase, remove spaces
fights_df.columns = fights_df.columns.str.strip().str.lower().str.replace(" ", "_")

# 2. Handle Missing Values: fill or drop based on column context
# For example, fill missing `referee` with "Unknown"
fights_df["referee"] = fights_df["referee"].fillna("Unknown")

# Convert missing numeric columns to 0 if it makes sense (e.g., knockdowns)
numeric_cols = fights_df.select_dtypes(include=["float64", "int64"]).columns
fights_df[numeric_cols] = fights_df[numeric_cols].fillna(0)

# 3. Convert Dates to datetime format
fights_df["date"] = pd.to_datetime(fights_df["date"], errors="coerce")

# 4. Convert other numeric columns to appropriate types (if needed)
# If any stats are strings due to scraping issues, convert to numeric
fights_df[numeric_cols] = fights_df[numeric_cols].apply(pd.to_numeric, errors="coerce")

# 5. Create new feature columns
# Calculate total significant strikes (head, body, leg)
fights_df["total_sig_strikes_1"] = (
    fights_df["sig_head_1"] + fights_df["sig_body_1"] + fights_df["sig_leg_1"]
)
fights_df["total_sig_strikes_2"] = (
    fights_df["sig_head_2"] + fights_df["sig_body_2"] + fights_df["sig_leg_2"]
)


# Calculate control time in seconds if `ctrl_1` and `ctrl_2` are in "m:ss" format
def convert_control_time(time_str):
    if isinstance(time_str, str) and ":" in time_str:
        minutes, seconds = map(int, time_str.split(":"))
        return minutes * 60 + seconds
    return 0


fights_df["ctrl_1_seconds"] = fights_df["ctrl_1"].apply(convert_control_time)
fights_df["ctrl_2_seconds"] = fights_df["ctrl_2"].apply(convert_control_time)

# 6. Drop duplicates if there are any
fights_df = fights_df.drop_duplicates()

# 7. Save the wrangled data
fights_df.to_csv(save_path + "wrangled_fight_details.csv", index=False)
event_links_df.to_csv(save_path + "wrangled_event_links.csv", index=False)

# Display success message
print("Wrangling complete. Data saved to:", save_path)

data_types = {
    "fight_link": "string",
    "date": "datetime64[s]",
    "location": "string",
    "method": "category",
    "round": "int64",
}
