####################################
# Import Libraries
####################################
# Data manipulation
import pandas as pd

# Web scraping
import scrapy
from scrapy.crawler import CrawlerProcess

# System and utilities
from datetime import datetime
from pathlib import Path

####################################
# Define Directories and Paths
####################################
# Project root is one level up from this script, so it works from any working directory
base_dir = Path(__file__).resolve().parent.parent
scraped_data_dir = base_dir / "data" / "scraped"
scraped_data_dir.mkdir(parents=True, exist_ok=True)

# Define paths to save the CSV files
scraped_event_links_path = scraped_data_dir / "scraped_event_links.csv"
scraped_fights_path = scraped_data_dir / "scraped_fight_details.csv"

####################################
# Initialize Data Storage
####################################
# page=all lists every completed event on one page, so no page count to keep up to date
events_url = "http://ufcstats.com/statistics/events/completed?page=all"
event_links = []  # List to store event links separately
fights = []  # List to store fight details

# Column order of the stats tables (each column has one value per fighter)
overall_fields = ["kd", "sig_str", "sig_str_perc", "total_str", "td", "td_pct", "sub_att", "rev", "ctrl"]
sig_strike_fields = ["sig_str_head", "sig_str_body", "sig_str_leg", "sig_str_dist", "sig_str_clinch", "sig_str_ground"]


####################################
# Define Scrapy Spider
####################################
class UfcSpider(scrapy.Spider):
    name = "ufc_spider"

    def start_requests(self):
        """Start by sending a request to the completed events page."""

        yield scrapy.Request(url=events_url, callback=self.parse_main)

    def parse_main(self, response):
        """Extract event links from the page and follow them."""

        # Extract event links
        event_links_on_page = response.css("a.b-link.b-link_style_black::attr(href)").getall()

        for event_link in event_links_on_page:
            event_links.append({"event_link": event_link})
            yield response.follow(url=event_link, callback=self.parse_events)

    def parse_events(self, response):
        """Extract fight links, date, and location from the event page."""

        fight_links = response.css("tr.b-fight-details__table-row[data-link]::attr(data-link)").getall()
        date = self._last_text(response.css("li.b-list__box-list-item:nth-child(1)::text").getall())
        location = self._last_text(response.css("li.b-list__box-list-item:nth-child(2)::text").getall())

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

        except Exception as e:
            # Log the error details
            self.logger.error(f"Error processing fight at {response.url}: {e}")

            # Create minimal fight data with error info
            fights.append({
                "fight_link": response.url,
                "date": date,
                "location": location,
                "error": str(e),
                "error_type": type(e).__name__,
            })
            return

        # Some fights (mostly 1990s events) have no stats tables, keep them with empty stats
        stats_available = bool(response.css("section.b-fight-details__section table"))

        # Combine all fight data into a single dictionary
        fight_data = {
            "fight_link": response.url,
            "date": date,
            "location": location,
            **fighter_details,
            **fight_details,
            "stats_available": stats_available,
            **self._extract_overall_stats(response),
            **self._extract_round_overall_stats(response),
            **self._extract_sig_strikes(response),
            **self._extract_round_sig_strike_stats(response),
        }

        # Add the fight data to our global list
        fights.append(fight_data)

    def _last_text(self, texts):
        """Return the last non-empty stripped string in a list, or None."""

        texts = [t.strip() for t in texts if t.strip()]
        return texts[-1] if texts else None

    def _table_values(self, row, fields, start):
        """Map the cells of one stats row to {field_1, field_2} pairs, None if the row is missing."""

        p_texts = row.css("p.b-fight-details__table-text::text").getall() if row else []
        values = {}
        for i, field in enumerate(fields):
            for fighter in [1, 2]:
                idx = start + 2 * i + (fighter - 1)
                values[f"{field}_{fighter}"] = p_texts[idx].strip() if idx < len(p_texts) else None
        return values

    def _extract_fighter_details(self, response):
        """Extract fighter names, stage names, and win/loss status."""

        table = response.css("div.b-fight-details__person")

        name_1, name_2 = table.css("h3 > a::text").getall()[0:2]
        stage_name_1, stage_name_2 = table.css("p.b-fight-details__person-title::text").getall()[0:2]
        win_loss_1, win_loss_2 = table.css("i.b-fight-details__person-status::text").getall()[0:2]

        # Assign values from extracted data
        fighter_details = {
            "name_1": name_1.strip() or None,
            "name_2": name_2.strip() or None,
            "stage_name_1": stage_name_1.strip() or None,
            "stage_name_2": stage_name_2.strip() or None,
            "win_loss_1": win_loss_1.strip() or None,
            "win_loss_2": win_loss_2.strip() or None,
        }

        return fighter_details

    def _extract_fight_details(self, response):
        """Extract general fight details."""

        table = response.css("div.b-fight-details__content")

        # Details holds the stoppage for finishes and every judge's score for decisions
        details = [t.strip() for t in table.css("p:nth-child(2) ::text").getall() if t.strip()]
        details = [t for t in details if t != "Details:"]

        # Assign values from extracted data
        fight_details = {
            "division": self._last_text(response.css("i.b-fight-details__fight-title::text").getall()),
            "method": self._last_text(table.css("p:nth-child(1) > i.b-fight-details__text-item_first > i:nth-child(2)::text").getall()),
            "last_round": self._last_text(table.css("p:nth-child(1) > i:nth-child(2)::text").getall()),
            "last_round_time": self._last_text(table.css("p:nth-child(1) > i:nth-child(3)::text").getall()),
            "time_format": self._last_text(table.css("p:nth-child(1) > i:nth-child(4)::text").getall()),
            "referee": self._last_text(table.css("p:nth-child(1) > i:nth-child(5) > span::text").getall()),
            "stoppage_details": " ".join(details) or None,
        }

        return fight_details

    def _extract_overall_stats(self, response):
        """Extract overall fight statistics."""

        # First 4 cells are the fighter name cells, stats start at index 4
        table = response.css("section.b-fight-details__section:nth-of-type(2) table")
        return self._table_values(table.css("tbody tr"), overall_fields, start=4)

    def _extract_round_overall_stats(self, response):
        """Extract round-by-round overall statistics."""

        round_overall_stats = {}

        table = response.css("section.b-fight-details__section:nth-of-type(3) table")
        round_rows = table.css("tbody tr.b-fight-details__table-row")

        for round_num in range(1, 6):
            round_row = round_rows[round_num - 1] if round_num <= len(round_rows) else None
            values = self._table_values(round_row, overall_fields, start=4)
            round_overall_stats.update({f"r{round_num}_{k}": v for k, v in values.items()})

        return round_overall_stats

    def _extract_sig_strikes(self, response):
        """Extract significant strike statistics by target and position."""

        # Sig. str and Sig. str % repeat the overall table, so skip them (start at index 8)
        table = response.css("body > section > div > div > table")
        return self._table_values(table.css("tbody tr"), sig_strike_fields, start=8)

    def _extract_round_sig_strike_stats(self, response):
        """Extract round-by-round significant strike statistics."""

        round_sig_str_stats = {}

        table = response.css("section.b-fight-details__section:nth-of-type(5) table")
        round_rows = table.css("tbody tr.b-fight-details__table-row")

        for round_num in range(1, 6):
            round_row = round_rows[round_num - 1] if round_num <= len(round_rows) else None
            values = self._table_values(round_row, sig_strike_fields, start=8)
            round_sig_str_stats.update({f"r{round_num}_{k}": v for k, v in values.items()})

        return round_sig_str_stats

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
            # Identify the scraper honestly and keep the load on the site low
            "USER_AGENT": "UFC_Data_WEBSCRAPING (+https://github.com/Yahlawat/UFC_Data_WEBSCRAPING)",
            "ROBOTSTXT_OBEY": True,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
            "DOWNLOAD_DELAY": 1,
            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 1,
            "AUTOTHROTTLE_MAX_DELAY": 10,
            "RETRY_TIMES": 3,
            "LOG_LEVEL": "ERROR",
        }
    )

    print("Starting crawler process...")

    # Add the spider to the process
    process.crawl(UfcSpider)
    process.start()

    # Save event links data
    if event_links:
        pd.DataFrame(event_links).to_csv(scraped_event_links_path, index=False)
        print("Event links successfully saved.")
    else:
        print("No event links scraped or list is empty.")

    # Save fight details data
    if not fights:
        print("No fights scraped or list is empty.")
        return

    fights_df = pd.DataFrame(fights)
    fights_df.to_csv(scraped_fights_path, index=False)
    print("Fight details successfully saved.")

    # Summary
    errors = fights_df[fights_df["error"].notna()] if "error" in fights_df.columns else fights_df.iloc[0:0]
    print("\nScraping Summary...")
    print(f"Event Links Found: {len(event_links)}")
    print(f"Fights Parsed: {len(fights)}")
    print(f"Fights Without Stats: {(fights_df['stats_available'] == False).sum()}")
    print(f"Errors: {len(errors)}")
    if not errors.empty:
        print(errors[["fight_link", "error", "error_type"]])


if __name__ == "__main__":
    # Run the scraper and track runtime
    start_time = datetime.now()
    run_scraper()
    end_time = datetime.now()
    runtime = end_time - start_time
    print(f"\nTotal Runtime: {runtime}")
