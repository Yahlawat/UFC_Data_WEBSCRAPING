####################################
# Import Libraries
####################################
# Data manipulation
import pandas as pd

# Web scraping
import scrapy
from scrapy.crawler import CrawlerProcess

# System and utilities
import os
from datetime import datetime
import logging

####################################
# Define Directories and Paths
####################################
# Define the base directory for data
base_dir = os.path.dirname(os.path.abspath("."))  # Get project root directory
data_dir = os.path.join(base_dir, "Data")
scraped_data_dir = os.path.join(data_dir, "Scraped_Data")
wrangled_data_dir = os.path.join(data_dir, "wrangled_Data")

# Create directories if they don't exist
os.makedirs(data_dir, exist_ok=True)
os.makedirs(scraped_data_dir, exist_ok=True)
os.makedirs(wrangled_data_dir, exist_ok=True)

# Define paths to save the CSV files
scrapped_event_links_path = os.path.join(scraped_data_dir, "scrapped_event_links.csv")
scrapped_fights_path = os.path.join(scraped_data_dir, "scrapped_fight_details.csv")

wrangled_event_links_path = os.path.join(wrangled_data_dir, "wrangled_event_links.csv")
wrangled_fights_path = os.path.join(wrangled_data_dir, "wrangled_fight_details.csv")

####################################
# Initialize Data Storage
####################################
# Initialize lists to store UFC fights data
pages = 1  # Set the number of pages to scrape (increase for more data)
event_links = []  # List to store event links separately
fights = []  # List to store fight details


####################################
# Define Scrapy Spider
####################################
class UfcSpider(scrapy.Spider):
    name = "ufc_spider"

    def start_requests(self):
        """Start by sending requests to the UFC statistics events pages."""

        # Loop through the pages
        for page in range(1, pages + 1):
            url = f"http://ufcstats.com/statistics/events/completed?page={page}"
            yield scrapy.Request(url=url, callback=self.parse_main)

    def parse_main(self, response):
        """Extract event links from the page and follow them."""

        # Extract event links
        event_links_on_page = response.css(
            "a.b-link.b-link_style_black::attr(href)"
        ).extract()

        for event_link in event_links_on_page:
            event_links.append({"event_link": event_link})
            yield response.follow(url=event_link, callback=self.parse_events)

    def parse_events(self, response):
        """Extract fight links, date, and location from the event page."""

        # Extract fight links
        fight_links = response.css(
            "tr.b-fight-details__table-row[data-link]::attr(data-link)"
        ).getall()

        # Extract date and location from the box list
        box_list_event_details = response.css(
            "ul.b-list__box-list li.b-list__box-list-item"
        )

        # Extract date - find item with "Date:" title and get its text content
        date_item = box_list_event_details.css(':contains("Date:") ::text').extract()
        date = "".join(date_item).replace("Date:", "").strip()

        # Extract location - find item with "Location:" title and get its text content
        location_item = box_list_event_details.css(
            ':contains("Location:") ::text'
        ).extract()
        location = "".join(location_item).replace("Location:", "").strip()

        for fight_link in fight_links:
            yield response.follow(
                url=fight_link,
                callback=self.parse_fights,
                meta={"date": date, "location": location},
            )

    def parse_fights(self, response):
        """Extract fight details such as fighters, results, and statistics."""
        date = response.meta["date"]
        location = response.meta["location"]

        try:
            # Extract fighter details
            fighter_details = self._extract_fighter_details(response)

            # Extract fight information
            fight_details = self._extract_fight_details(response)

            # Extract overall statistics
            overall_stats = self._extract_overall_stats(response)

            # Extract round-by-round overall statistics
            round_overall_stats = self._extract_round_overall_stats(response)

            # Extract significant strike statistics
            sig_strike_stats = self._extract_sig_strike_stats(response)

            # Extract round-by-round significant strike statistics
            round_sig_strike_stats = self._extract_round_sig_strike_stats(response)

            # Combine all fight data into a single dictionary
            fight_data = {
                "fight_link": response.url,
                "date": date,
                "location": location,
                **fighter_details,
                **fight_details,
                **overall_stats,
                **round_overall_stats,
                **sig_strike_stats,
                **round_sig_strike_stats,
            }

            # Add the fight data to our global list
            fights.append(fight_data)

        except Exception:
            pass

    def _extract_fighter_details(self, response):
        """Extract fighter names, stage names, and win/loss status."""
        fighter_blocks = response.css("div.b-fight-details__person")
        fighter_details = {}

        if len(fighter_blocks) >= 2:
            # Fighter 1
            fighter_details["win_loss_1"] = (
                fighter_blocks[0]
                .css("i.b-fight-details__person-status::text")
                .get(default="N/A")
                .strip()
            )
            fighter_details["name_1"] = (
                fighter_blocks[0]
                .css("h3.b-fight-details__person-name a::text")
                .get(default="N/A")
                .strip()
            )
            fighter_details["stage_name_1"] = (
                fighter_blocks[0]
                .css("p.b-fight-details__person-title::text")
                .get(default="N/A")
                .strip()
            )

            # Fighter 2
            fighter_details["win_loss_2"] = (
                fighter_blocks[1]
                .css("i.b-fight-details__person-status::text")
                .get(default="N/A")
                .strip()
            )
            fighter_details["name_2"] = (
                fighter_blocks[1]
                .css("h3.b-fight-details__person-name a::text")
                .get(default="N/A")
                .strip()
            )
            fighter_details["stage_name_2"] = (
                fighter_blocks[1]
                .css("p.b-fight-details__person-title::text")
                .get(default="N/A")
                .strip()
            )
        else:
            # Assign default values if blocks aren't found
            fighter_details.update(
                {
                    "name_1": None,
                    "name_2": None,
                    "stage_name_1": None,
                    "stage_name_2": None,
                    "win_loss_1": None,
                    "win_loss_2": None,
                }
            )

        return fighter_details


def _extract_fight_details(self, response):
    """Extract general fight details."""

    fight_details = {
        "division": None,
        "method": None,
        "round": None,
        "time": None,
        "time_format": None,
        "referee": None,
        "details": None,
    }

    # Extract division
    division = response.css("i.b-fight-details__fight-title::text").get()
    if division:
        fight_details["division"] = division.strip()

    # Define the fight details block
    fight_details_block = response.css("div.b-fight-details__content")

    # Extract method
    method = fight_details_block.css(
        "p:nth-child(1) > i.b-fight-details__text-item_first > i:nth-child(2)::text"
    ).get()
    if method:
        fight_details["method"] = method.strip()

    # Extract round
    round_texts = fight_details_block.css(
        "p:nth-child(1) > i:nth-child(2)::text"
    ).getall()
    if len(round_texts) > 1:
        fight_details["round"] = round_texts[1].strip()

    # Extract time
    time_texts = fight_details_block.css(
        "p:nth-child(1) > i:nth-child(3)::text"
    ).getall()
    if len(time_texts) > 1:
        fight_details["time"] = time_texts[1].strip()

    # Extract time format
    format_texts = fight_details_block.css(
        "p:nth-child(1) > i:nth-child(4)::text"
    ).getall()
    if len(format_texts) > 1:
        fight_details["time_format"] = format_texts[1].strip()

    # Extract referee
    referee = fight_details_block.css(
        "p:nth-child(1) > i:nth-child(5) > span::text"
    ).get()
    if referee:
        fight_details["referee"] = referee.strip()

    # Extract details
    details_texts = fight_details_block.css("p:nth-child(2)::text").getall()
    if len(details_texts) > 1:
        fight_details["details"] = details_texts[1].strip()

    return fight_details

    def _extract_overall_stats(self, response):
        """Extract overall fight statistics."""
        # Select the table specifically for overall stats
        overall_stats_table = response.css(
            "section.b-fight-details__section:nth-of-type(2) table"
        )

        overall_stats = {}
        try:
            if overall_stats_table:
                p_texts = overall_stats_table.css(
                    "td p.b-fight-details__table-text::text"
                ).getall()

                overall_stats = {
                    "kd_1": p_texts[0].strip() if len(p_texts) > 0 else None,
                    "kd_2": p_texts[1].strip() if len(p_texts) > 1 else None,
                    "sig_str_1": p_texts[2].strip() if len(p_texts) > 2 else None,
                    "sig_str_2": p_texts[3].strip() if len(p_texts) > 3 else None,
                    "sig_str_perc_1": p_texts[4].strip() if len(p_texts) > 4 else None,
                    "sig_str_perc_2": p_texts[5].strip() if len(p_texts) > 5 else None,
                    "total_str_1": p_texts[6].strip() if len(p_texts) > 6 else None,
                    "total_str_2": p_texts[7].strip() if len(p_texts) > 7 else None,
                    "td_1": p_texts[8].strip() if len(p_texts) > 8 else None,
                    "td_2": p_texts[9].strip() if len(p_texts) > 9 else None,
                    "td_pct_1": p_texts[10].strip() if len(p_texts) > 10 else None,
                    "td_pct_2": p_texts[11].strip() if len(p_texts) > 11 else None,
                    "sub_att_1": p_texts[12].strip() if len(p_texts) > 12 else None,
                    "sub_att_2": p_texts[13].strip() if len(p_texts) > 13 else None,
                    "rev_1": p_texts[14].strip() if len(p_texts) > 14 else None,
                    "rev_2": p_texts[15].strip() if len(p_texts) > 15 else None,
                    "ctrl_1": p_texts[16].strip() if len(p_texts) > 16 else None,
                    "ctrl_2": p_texts[17].strip() if len(p_texts) > 17 else None,
                }
            else:
                overall_stats = self._get_empty_overall_stats()

        except (IndexError, Exception):
            overall_stats = self._get_empty_overall_stats()

        return overall_stats

    def _extract_round_overall_stats(self, response):
        """Extract round-by-round overall statistics."""
        # Select the table specifically for round-by-round stats
        round_overall_stats_table = response.css(
            "section.b-fight-details__section:nth-of-type(3) table"
        )

        round_overall_stats = {}
        try:
            if round_overall_stats_table:
                # Get all rows from the table
                rows = round_overall_stats_table.css("tr.b-fight-details__table-row")

                # Process each round
                for round_num in range(1, 6):  # UFC fights can have up to 5 rounds
                    if round_num <= len(rows):
                        # Get the row for this round
                        round_row = rows[round_num - 1]

                        # Extract stats for this round
                        p_texts = round_row.css(
                            "td p.b-fight-details__table-text::text"
                        ).getall()

                        # Add round-specific stats with round number prefix
                        round_overall_stats.update(
                            {
                                f"r{round_num}_kd_1": p_texts[0].strip()
                                if len(p_texts) > 0
                                else None,
                                f"r{round_num}_kd_2": p_texts[1].strip()
                                if len(p_texts) > 1
                                else None,
                                f"r{round_num}_sig_str_1": p_texts[2].strip()
                                if len(p_texts) > 2
                                else None,
                                f"r{round_num}_sig_str_2": p_texts[3].strip()
                                if len(p_texts) > 3
                                else None,
                                f"r{round_num}_sig_str_perc_1": p_texts[4].strip()
                                if len(p_texts) > 4
                                else None,
                                f"r{round_num}_sig_str_perc_2": p_texts[5].strip()
                                if len(p_texts) > 5
                                else None,
                                f"r{round_num}_total_str_1": p_texts[6].strip()
                                if len(p_texts) > 6
                                else None,
                                f"r{round_num}_total_str_2": p_texts[7].strip()
                                if len(p_texts) > 7
                                else None,
                                f"r{round_num}_td_1": p_texts[8].strip()
                                if len(p_texts) > 8
                                else None,
                                f"r{round_num}_td_2": p_texts[9].strip()
                                if len(p_texts) > 9
                                else None,
                                f"r{round_num}_td_pct_1": p_texts[10].strip()
                                if len(p_texts) > 10
                                else None,
                                f"r{round_num}_td_pct_2": p_texts[11].strip()
                                if len(p_texts) > 11
                                else None,
                                f"r{round_num}_sub_att_1": p_texts[12].strip()
                                if len(p_texts) > 12
                                else None,
                                f"r{round_num}_sub_att_2": p_texts[13].strip()
                                if len(p_texts) > 13
                                else None,
                                f"r{round_num}_rev_1": p_texts[14].strip()
                                if len(p_texts) > 14
                                else None,
                                f"r{round_num}_rev_2": p_texts[15].strip()
                                if len(p_texts) > 15
                                else None,
                                f"r{round_num}_ctrl_1": p_texts[16].strip()
                                if len(p_texts) > 16
                                else None,
                                f"r{round_num}_ctrl_2": p_texts[17].strip()
                                if len(p_texts) > 17
                                else None,
                            }
                        )
                    else:
                        # Add empty stats for non-existent rounds
                        round_overall_stats.update(
                            self._get_empty_round_overall_stats()
                        )
            else:
                round_overall_stats = self._get_empty_round_overall_stats()
        except (IndexError, Exception):
            round_overall_stats = self._get_empty_round_overall_stats()

        return round_overall_stats

    def _extract_sig_strike_stats(self, response):
        """Extract overall significant strike statistics."""
        # Select the table for overall significant strikes
        sig_str_table = response.css("div.b-fight-details table")

        sig_str_stats = {}
        try:
            if sig_str_table:
                p_texts = sig_str_table.css(
                    "td p.b-fight-details__table-text::text"
                ).getall()
                sig_str_stats = {
                    "sig_str_head_1": p_texts[4].strip() if len(p_texts) > 4 else None,
                    "sig_str_head_2": p_texts[5].strip() if len(p_texts) > 5 else None,
                    "sig_str_body_1": p_texts[6].strip() if len(p_texts) > 6 else None,
                    "sig_str_body_2": p_texts[7].strip() if len(p_texts) > 7 else None,
                    "sig_str_leg_1": p_texts[8].strip() if len(p_texts) > 8 else None,
                    "sig_str_leg_2": p_texts[9].strip() if len(p_texts) > 9 else None,
                    "sig_str_dist_1": p_texts[10].strip()
                    if len(p_texts) > 10
                    else None,
                    "sig_str_dist_2": p_texts[11].strip()
                    if len(p_texts) > 11
                    else None,
                    "sig_str_clinch_1": p_texts[12].strip()
                    if len(p_texts) > 12
                    else None,
                    "sig_str_clinch_2": p_texts[13].strip()
                    if len(p_texts) > 13
                    else None,
                    "sig_str_ground_1": p_texts[14].strip()
                    if len(p_texts) > 14
                    else None,
                    "sig_str_ground_2": p_texts[15].strip()
                    if len(p_texts) > 15
                    else None,
                }
            else:
                sig_str_stats = self._get_empty_sig_strike_stats()

        except (IndexError, Exception):
            sig_str_stats = self._get_empty_sig_strike_stats()

        return sig_str_stats

    def _extract_round_sig_strike_stats(self, response):
        """Extract round-by-round significant strike statistics."""
        # Select the table for round-by-round significant strikes
        round_sig_str_table = response.css(
            "section.b-fight-details__section:nth-of-type(5) table"
        )

        round_sig_str_stats = {}
        try:
            if round_sig_str_table:
                p_texts = round_sig_str_table.css(
                    "td p.b-fight-details__table-text::text"
                ).getall()

                for round_num in range(1, 6):  # Rounds 1-5
                    round_key = f"round_{round_num}"
                    round_stats = {}

                    # Extract stats for this round
                    round_stats.update(
                        {
                            f"{round_key}_sig_str_head_1": p_texts[0].strip()
                            if len(p_texts) > 0
                            else None,
                            f"{round_key}_sig_str_head_2": p_texts[1].strip()
                            if len(p_texts) > 1
                            else None,
                            f"{round_key}_sig_str_body_1": p_texts[2].strip()
                            if len(p_texts) > 2
                            else None,
                            f"{round_key}_sig_str_body_2": p_texts[3].strip()
                            if len(p_texts) > 3
                            else None,
                            f"{round_key}_sig_str_leg_1": p_texts[4].strip()
                            if len(p_texts) > 4
                            else None,
                            f"{round_key}_sig_str_leg_2": p_texts[5].strip()
                            if len(p_texts) > 5
                            else None,
                            f"{round_key}_sig_str_dist_1": p_texts[6].strip()
                            if len(p_texts) > 6
                            else None,
                            f"{round_key}_sig_str_dist_2": p_texts[7].strip()
                            if len(p_texts) > 7
                            else None,
                            f"{round_key}_sig_str_clinch_1": p_texts[8].strip()
                            if len(p_texts) > 8
                            else None,
                            f"{round_key}_sig_str_clinch_2": p_texts[9].strip()
                            if len(p_texts) > 9
                            else None,
                            f"{round_key}_sig_str_ground_1": p_texts[10].strip()
                            if len(p_texts) > 10
                            else None,
                            f"{round_key}_sig_str_ground_2": p_texts[11].strip()
                            if len(p_texts) > 11
                            else None,
                        }
                    )

                    round_sig_str_stats[round_key] = round_stats
            else:
                round_sig_str_stats = self._get_empty_round_sig_strike_stats()
        except (IndexError, Exception):
            round_sig_str_stats = self._get_empty_round_sig_strike_stats()

        return round_sig_str_stats

    def _get_empty_overall_stats(self):
        """Return empty structure for overall stats."""
        return {
            "kd_1": None,
            "kd_2": None,
            "sig_str_perc_1": None,
            "sig_str_perc_2": None,
            "total_str_1": None,
            "total_str_2": None,
            "td_1": None,
            "td_2": None,
            "sub_att_1": None,
            "sub_att_2": None,
            "ctrl_1": None,
            "ctrl_2": None,
        }

    def _get_empty_round_overall_stats(self):
        """Return a dictionary with None values for all round statistics."""
        round_overall_stats = {}
        stat_fields = [
            "kd",
            "sig_str",
            "sig_str_pct",
            "total_str",
            "td",
            "td_pct",
            "sub_att",
            "rev",
            "ctrl",
        ]

        # loop through each round and each stat for each fighter
        for round_num in range(1, 6):  # Rounds 1-5
            for stat in stat_fields:
                for fighter in [1, 2]:
                    key = f"round_{round_num}_{stat}_{fighter}"
                    round_overall_stats[key] = None

        return round_overall_stats

    def _get_empty_sig_strike_stats(self):
        """Return empty structure for significant strike stats."""
        return {
            "sig_str_head_1": None,
            "sig_str_head_2": None,
            "sig_str_body_1": None,
            "sig_str_body_2": None,
            "sig_str_leg_1": None,
            "sig_str_leg_2": None,
            "sig_str_dist_1": None,
            "sig_str_dist_2": None,
            "sig_str_clinch_1": None,
            "sig_str_clinch_2": None,
            "sig_str_ground_1": None,
            "sig_str_ground_2": None,
        }

    def _get_empty_round_sig_strike_stats(self):
        """Return empty structure for round-by-round significant strike stats."""
        round_sig_strike_stats = {}
        stat_fields = [
            "sig_str_head",
            "sig_str_body",
            "sig_str_leg",
            "sig_str_dist",
            "sig_str_clinch",
            "sig_str_ground",
        ]

        # loop through each round and each stat for each fighter
        for round_num in range(1, 6):  # Rounds 1-5
            for stat in stat_fields:
                for fighter in [1, 2]:
                    key = f"round_{round_num}_{stat}_{fighter}"
                    round_sig_strike_stats[key] = None

        return round_sig_strike_stats


####################################
# Scraper Execution
####################################
def run_scraper():
    """
    Run the UFC Scrapy spider and save the scraped data to CSV files.
    """
    # Create and configure the Scrapy crawler process
    process = CrawlerProcess(
        settings={  # Pass settings directly
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "LOG_LEVEL": "ERROR",
        }
    )

    logging.info("Starting crawler process...")

    # Add the spider to the process
    process.crawl(UfcSpider)
    process.start()

    # Save event links data
    if event_links:
        event_links_df = pd.DataFrame(event_links)
        event_links_df.to_csv(scrapped_event_links_path, index=False)
        print(f"Event links successfully saved.")
    else:
        print("No event links scraped or list is empty.")

    # Save fight details data
    if fights:
        fights_df = pd.DataFrame(fights)
        fights_df.to_csv(scrapped_fights_path, index=False)
        print(f"Fight details successfully saved.")
    else:
        print("No fights scraped or list is empty.")

    # Summary
    print(f"\nScraping Summary...")
    print(f"Target Pages: {pages}")
    print(f"Event Links Found: {len(event_links)}")
    print(f"Fights Parsed: {len(fights)}")


# Run the scraper and track runtime
start_time = datetime.now()
run_scraper()
end_time = datetime.now()
runtime = end_time - start_time
print(f"\nTotal Runtime: {runtime}")

df = pd.read_csv(
    r"C:\Users\ahlaw\OneDrive - UBC\Documents\vscode\Projects\UFC_data_webscraping\Data\Scraped_Data\scrapped_fight_details.csv"
)
df_test = df.sort_values(by="date", ascending=True).head(1)
