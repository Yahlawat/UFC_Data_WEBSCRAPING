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
pages = 29  # Set the number of pages to scrape (29 as of April 12, 2025)
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
        event_links_on_page = response.css("a.b-link.b-link_style_black::attr(href)").extract()

        for event_link in event_links_on_page:
            event_links.append({"event_link": event_link})
            yield response.follow(url=event_link, callback=self.parse_events)

    def parse_events(self, response):
        """Extract fight links, date, and location from the event page."""

        fight_links = response.css("tr.b-fight-details__table-row[data-link]::attr(data-link)").getall()
        date = response.css("li.b-list__box-list-item:nth-child(1)::text").extract()[1]
        location = response.css("li.b-list__box-list-item:nth-child(2)::text").extract()[1]

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
            sig_strikes = self._extract_sig_strikes(response)

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
                **sig_strikes,
                **round_sig_strike_stats,
            }

            # Add the fight data to our global list
            fights.append(fight_data)

        except Exception as e:
            # Log the error details
            self.logger.error(f"Error processing fight at {response.url}")
            self.logger.error(f"Error message: {str(e)}")

            # Create minimal fight data with error info
            error_fight_data = {
                "fight_link": response.url,
                "date": date,
                "location": location,
                "error": str(e),
                "error_type": type(e).__name__,
            }
            fights.append(error_fight_data)

    def _extract_fighter_details(self, response):
        """Extract fighter names, stage names, and win/loss status."""

        table = response.css("div.b-fight-details__person")

        name_1, name_2 = table.css("h3 > a::text").extract()[0:2]
        stage_name_1, stage_name_2 = table.css("p.b-fight-details__person-title::text").extract()[0:2]
        win_loss_1, win_loss_2 = table.css("i.b-fight-details__person-status::text").extract()[0:2]

        # Assign values from extracted data
        fighter_details = {
            "name_1": name_1.strip() if name_1 else None,
            "name_2": name_2.strip() if name_2 else None,
            "stage_name_1": stage_name_1.strip() if stage_name_1 else None,
            "stage_name_2": stage_name_2.strip() if stage_name_2 else None,
            "win_loss_1": win_loss_1.strip() if win_loss_1 else None,
            "win_loss_2": win_loss_2.strip() if win_loss_2 else None,
        }

        return fighter_details

    def _extract_fight_details(self, response):
        """Extract general fight details."""

        table = response.css("div.b-fight-details__content")

        division = response.css("i.b-fight-details__fight-title::text").getall()[-1].strip()
        method = table.css("p:nth-child(1) > i.b-fight-details__text-item_first > i:nth-child(2)::text").getall()[-1].strip()
        last_round = table.css("p:nth-child(1) > i:nth-child(2)::text").getall()[-1].strip()
        last_round_time = table.css("p:nth-child(1) > i:nth-child(3)::text").getall()[-1].strip()
        time_format = table.css("p:nth-child(1) > i:nth-child(4)::text").getall()[-1].strip()
        referee = table.css("p:nth-child(1) > i:nth-child(5) > span::text").getall()[-1].strip()
        stoppage_details = table.css("p:nth-child(2)::text").getall()[-1].strip()

        # Assign values from extracted data
        fight_details = {
            "division": division if division else None,
            "method": method if method else None,
            "last_round": last_round if last_round else None,
            "last_round_time": last_round_time if last_round_time else None,
            "time_format": time_format if time_format else None,
            "referee": referee if referee else None,
            "stoppage_details": stoppage_details if stoppage_details else None,
        }

        return fight_details

    def _extract_overall_stats(self, response):
        """Extract overall fight statistics."""

        table = response.css("section.b-fight-details__section:nth-of-type(2) table")
        p_texts = table.css("td p.b-fight-details__table-text::text").extract()

        # Assign values from extracted data
        overall_stats = {
            "kd_1": p_texts[4].strip() if p_texts[4] else None,
            "kd_2": p_texts[5].strip() if p_texts[5] else None,
            "sig_str_1": p_texts[6].strip() if p_texts[6] else None,
            "sig_str_2": p_texts[7].strip() if p_texts[7] else None,
            "sig_str_perc_1": p_texts[8].strip() if p_texts[8] else None,
            "sig_str_perc_2": p_texts[9].strip() if p_texts[9] else None,
            "total_str_1": p_texts[10].strip() if p_texts[10] else None,
            "total_str_2": p_texts[11].strip() if p_texts[11] else None,
            "td_1": p_texts[12].strip() if p_texts[12] else None,
            "td_2": p_texts[13].strip() if p_texts[13] else None,
            "td_pct_1": p_texts[14].strip() if p_texts[14] else None,
            "td_pct_2": p_texts[15].strip() if p_texts[15] else None,
            "sub_att_1": p_texts[16].strip() if p_texts[16] else None,
            "sub_att_2": p_texts[17].strip() if p_texts[17] else None,
            "rev_1": p_texts[18].strip() if p_texts[18] else None,
            "rev_2": p_texts[19].strip() if p_texts[19] else None,
            "ctrl_1": p_texts[20].strip() if p_texts[20] else None,
            "ctrl_2": p_texts[21].strip() if p_texts[21] else None,
        }

        return overall_stats

    def _extract_round_overall_stats(self, response):
        """Extract round-by-round overall statistics."""
        
        round_overall_stats = {}
        
        table = response.css("section.b-fight-details__section:nth-of-type(3) table")
        round_rows = table.css("tbody tr.b-fight-details__table-row")
        
        for round_num in range(1, 6):
            if round_num <= len(round_rows):
                round_row = round_rows[round_num - 1]
                p_texts = round_row.css("p.b-fight-details__table-text::text").getall()

                if True:
                    round_overall_stats.update({
                        f"r{round_num}_kd_1": p_texts[4].strip(),
                        f"r{round_num}_kd_2": p_texts[5].strip(),
                        f"r{round_num}_sig_str_1": p_texts[6].strip(),
                        f"r{round_num}_sig_str_2": p_texts[7].strip(),
                        f"r{round_num}_sig_str_perc_1": p_texts[8].strip(),
                        f"r{round_num}_sig_str_perc_2": p_texts[9].strip(),
                        f"r{round_num}_total_str_1": p_texts[10].strip(),
                        f"r{round_num}_total_str_2": p_texts[11].strip(),
                        f"r{round_num}_td_1": p_texts[12].strip(),
                        f"r{round_num}_td_2": p_texts[13].strip(),
                        f"r{round_num}_td_pct_1": p_texts[14].strip(),
                        f"r{round_num}_td_pct_2": p_texts[15].strip(),
                        f"r{round_num}_sub_att_1": p_texts[16].strip(),
                        f"r{round_num}_sub_att_2": p_texts[17].strip(),
                        f"r{round_num}_rev_1": p_texts[18].strip(),
                        f"r{round_num}_rev_2": p_texts[19].strip(),
                        f"r{round_num}_ctrl_1": p_texts[20].strip(),
                        f"r{round_num}_ctrl_2": p_texts[21].strip(),
                    })
                else:
                    round_overall_stats.update(self._get_empty_round_overall_stats(round_num))
            else:
                round_overall_stats.update(self._get_empty_round_overall_stats(round_num))

        return round_overall_stats

    def _extract_sig_strikes(self, response):
        """Extract significant strike statistics."""

        table = response.css("body > section > div > div > table")
        p_texts = table.css("td p.b-fight-details__table-text::text").getall()

        # Assign values from extracted data
        sig_strikes = {
            "sig_str_1": p_texts[4].strip() if len(p_texts) > 4 else None,
            "sig_str_2": p_texts[5].strip() if len(p_texts) > 5 else None,
            "sig_str_perc_1": p_texts[6].strip() if len(p_texts) > 6 else None,
            "sig_str_perc_2": p_texts[7].strip() if len(p_texts) > 7 else None,
            "sig_str_head_1": p_texts[8].strip() if len(p_texts) > 8 else None,
            "sig_str_head_2": p_texts[9].strip() if len(p_texts) > 9 else None,
            "sig_str_body_1": p_texts[10].strip() if len(p_texts) > 10 else None,
            "sig_str_body_2": p_texts[11].strip() if len(p_texts) > 11 else None,
            "sig_str_leg_1": p_texts[12].strip() if len(p_texts) > 12 else None,
            "sig_str_leg_2": p_texts[13].strip() if len(p_texts) > 13 else None,
            "sig_str_dist_1": p_texts[14].strip() if len(p_texts) > 14 else None,
            "sig_str_dist_2": p_texts[15].strip() if len(p_texts) > 15 else None,
            "sig_str_clinch_1": p_texts[16].strip() if len(p_texts) > 16 else None,
            "sig_str_clinch_2": p_texts[17].strip() if len(p_texts) > 17 else None,
            "sig_str_ground_1": p_texts[18].strip() if len(p_texts) > 18 else None,
            "sig_str_ground_2": p_texts[19].strip() if len(p_texts) > 19 else None,
        }

        return sig_strikes

    def _extract_round_sig_strike_stats(self, response):
        """Extract round-by-round significant strike statistics."""
        
        round_sig_str_stats = {}

        table = response.css("section.b-fight-details__section:nth-of-type(5) table")
        round_rows = table.css("tbody tr.b-fight-details__table-row")

        for round_num in range(1, 6):
            if round_num <= len(round_rows):
                round_row = round_rows[round_num - 1]
                p_texts = round_row.css("p.b-fight-details__table-text::text").getall()

                if True:
                    round_sig_str_stats.update({
                        f"r{round_num}_sig_str_head_1": p_texts[8].strip(),
                        f"r{round_num}_sig_str_head_2": p_texts[9].strip(),
                        f"r{round_num}_sig_str_body_1": p_texts[10].strip(),
                        f"r{round_num}_sig_str_body_2": p_texts[11].strip(),
                        f"r{round_num}_sig_str_leg_1": p_texts[12].strip(),
                        f"r{round_num}_sig_str_leg_2": p_texts[13].strip(),
                        f"r{round_num}_sig_str_dist_1": p_texts[14].strip(),
                        f"r{round_num}_sig_str_dist_2": p_texts[15].strip(),
                        f"r{round_num}_sig_str_clinch_1": p_texts[16].strip(),
                        f"r{round_num}_sig_str_clinch_2": p_texts[17].strip(),
                        f"r{round_num}_sig_str_ground_1": p_texts[18].strip(),
                        f"r{round_num}_sig_str_ground_2": p_texts[19].strip(),

                    })
                else:
                    round_sig_str_stats.update(self._get_empty_round_sig_strike_stats(round_num))
            else:
                round_sig_str_stats.update(self._get_empty_round_sig_strike_stats(round_num))

        return round_sig_str_stats

    def _get_empty_overall_stats(self):
        """Return empty structure for overall stats."""
        
        return {
            "kd_1": None,
            "kd_2": None,
            "sig_str_1": None,
            "sig_str_2": None,
            "sig_str_perc_1": None,
            "sig_str_perc_2": None,
            "total_str_1": None,
            "total_str_2": None,
            "td_1": None,
            "td_2": None,
            "sub_att_1": None,
            "sub_att_2": None,
            "rev_1": None,
            "rev_2": None,
            "ctrl_1": None,
            "ctrl_2": None,
        }

    def _get_empty_round_overall_stats(self, round_num):
        round_overall_stats = {}
        stat_fields = [
            "kd", "sig_str", "sig_str_perc", "total_str", "td", "td_pct", "sub_att", "rev", "ctrl"
        ]
        for stat in stat_fields:
            for fighter in [1, 2]:
                key = f"r{round_num}_{stat}_{fighter}"
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

    def _get_empty_round_sig_strike_stats(self, round_num):
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

        for stat in stat_fields:
            for fighter in [1, 2]:
                key = f"r{round_num}_{stat}_{fighter}"
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

    print("Starting crawler process...")

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
    print(f"Errors: {len(fights_df[fights_df['error'].notna()])}")
    print(fights_df[['fight_link', 'error', 'error_type']][fights_df['error'].notna()])

# Run the scraper and track runtime
start_time = datetime.now()
run_scraper()
end_time = datetime.now()
runtime = end_time - start_time
print(f"\nTotal Runtime: {runtime}")
