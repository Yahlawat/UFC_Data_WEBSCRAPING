##################################
# Import libraries
##################################
import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
import os
from datetime import datetime

##################################
# Define directories structure
##################################
BASE_DIR = os.path.dirname(os.path.abspath("."))
DATA_DIR = os.path.join(BASE_DIR, "Data")
SCRAPED_DATA_DIR = os.path.join(DATA_DIR, "Scraped_Data")
WRANGLED_DATA_DIR = os.path.join(DATA_DIR, "Wrangled_Data")

# Create directories
for directory in [DATA_DIR, SCRAPED_DATA_DIR, WRANGLED_DATA_DIR]:
    os.makedirs(directory, exist_ok=True)

# Define file paths
SCRAPED_EVENT_LINKS_PATH = os.path.join(SCRAPED_DATA_DIR, "scraped_event_links.csv")
SCRAPED_FIGHTS_PATH = os.path.join(SCRAPED_DATA_DIR, "scraped_fight_details.csv")

##################################
# Data storage
##################################
PAGES_TO_SCRAPE = 1  # Increase for more data
event_links = []
fights = []


##################################
# Define the spider
##################################
class UfcSpider(scrapy.Spider):
    name = "ufc_spider"

    def start_requests(self):
        """Start requests to UFC statistics events pages"""
        for page in range(1, PAGES_TO_SCRAPE + 1):
            url = f"http://ufcstats.com/statistics/events/completed?page={page}"
            yield scrapy.Request(url=url, callback=self.parse_main)

    def parse_main(self, response):
        """Extract and follow event links"""
        event_links_on_page = response.css(
            "a.b-link.b-link_style_black::attr(href)"
        ).extract()

        for event_link in event_links_on_page:
            event_links.append({"event_link": event_link})
            yield response.follow(url=event_link, callback=self.parse_events)

    def parse_events(self, response):
        """Extract fight links and metadata from event pages"""
        # Extract fight links
        fight_links = response.css(
            "tr.b-fight-details__table-row[data-link]::attr(data-link)"
        ).getall()

        # Extract event metadata
        box_list = response.css("ul.b-list__box-list li.b-list__box-list-item")
        date = (
            "".join(box_list.css(':contains("Date:") ::text').extract())
            .replace("Date:", "")
            .strip()
        )
        location = (
            "".join(box_list.css(':contains("Location:") ::text').extract())
            .replace("Location:", "")
            .strip()
        )

        # Follow each fight link
        for fight_link in fight_links:
            yield response.follow(
                url=fight_link,
                callback=self.parse_fights,
                meta={"date": date, "location": location},
            )

    def parse_fights(self, response):
        """Extract detailed fight data"""
        try:
            # Combine all fight data
            fight_data = {
                "fight_link": response.url,
                "date": response.meta["date"],
                "location": response.meta["location"],
                **self._extract_fighter_details(response),
                **self._extract_fight_details(response),
                **self._extract_overall_stats(response),
                **self._extract_round_overall_stats(response),
                **self._extract_sig_strike_stats(response),
                **self._extract_round_sig_strike_stats(response),
            }

            # Store the fight data
            fights.append(fight_data)
        except Exception as e:
            self.logger.error(f"Error parsing fight: {response.url} - {str(e)}")

    def _extract_fighter_details(self, response):
        """Extract fighter information"""
        fighter_blocks = response.css("div.b-fight-details__person")
        fighter_details = {}

        if len(fighter_blocks) >= 2:
            fighter_details.update(
                {
                    # Fighter 1
                    "win_loss_1": fighter_blocks[0]
                    .css("i.b-fight-details__person-status::text")
                    .get(default="N/A")
                    .strip(),
                    "name_1": fighter_blocks[0]
                    .css("h3.b-fight-details__person-name a::text")
                    .get(default="N/A")
                    .strip(),
                    "stage_name_1": fighter_blocks[0]
                    .css("p.b-fight-details__person-title::text")
                    .get(default="N/A")
                    .strip(),
                    # Fighter 2
                    "win_loss_2": fighter_blocks[1]
                    .css("i.b-fight-details__person-status::text")
                    .get(default="N/A")
                    .strip(),
                    "name_2": fighter_blocks[1]
                    .css("h3.b-fight-details__person-name a::text")
                    .get(default="N/A")
                    .strip(),
                    "stage_name_2": fighter_blocks[1]
                    .css("p.b-fight-details__person-title::text")
                    .get(default="N/A")
                    .strip(),
                }
            )
        else:
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
        """Extract general fight details"""
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
        fight_details["division"] = (
            response.css("i.b-fight-details__fight-title::text")
            .get(default="N/A")
            .strip()
        )

        # Define the fight details block
        details_block = response.css("div.b-fight-details__content")

        # Extract method, round, time, format and referee
        fight_details["method"] = (
            details_block.css(
                "p:nth-child(1) > i.b-fight-details__text-item_first > i:nth-child(2)::text"
            )
            .get(default="N/A")
            .strip()
        )

        round_texts = details_block.css(
            "p:nth-child(1) > i:nth-child(2)::text"
        ).getall()
        if len(round_texts) > 1:
            fight_details["round"] = round_texts.extract()[1].strip()

        time_texts = details_block.css("p:nth-child(1) > i:nth-child(3)::text").getall()
        if len(time_texts) > 1:
            fight_details["time"] = time_texts.extract()[1].strip()

        format_texts = details_block.css(
            "p:nth-child(1) > i:nth-child(4)::text"
        ).getall()
        if len(format_texts) > 1:
            fight_details["time_format"] = format_texts.extract()[1].strip()

        referee = details_block.css(
            "p:nth-child(1) > i:nth-child(5) > span::text"
        ).get()
        if referee:
            fight_details["referee"] = referee.strip()

        details_texts = details_block.css("p:nth-child(2)::text").getall()
        if len(details_texts) > 1:
            fight_details["details"] = details_texts.extract()[1].strip()

        return fight_details

    def _extract_overall_stats(self, response):
        """Extract overall fight statistics"""
        stats_table = response.css(
            "section.b-fight-details__section:nth-of-type(2) table"
        )

        if not stats_table:
            return self._get_empty_overall_stats()

        try:
            p_texts = stats_table.css("td p.b-fight-details__table-text::text").getall()
            stats = {}

            field_names = [
                "kd",
                "sig_str",
                "sig_str_perc",
                "total_str",
                "td",
                "td_pct",
                "sub_att",
                "rev",
                "ctrl",
            ]

            for i, field in enumerate(field_names):
                stats[f"{field}_1"] = (
                    p_texts[i * 2].strip() if i * 2 < len(p_texts) else None
                )
                stats[f"{field}_2"] = (
                    p_texts[i * 2 + 1].strip() if i * 2 + 1 < len(p_texts) else None
                )

            return stats
        except Exception:
            return self._get_empty_overall_stats()

    def _extract_round_overall_stats(self, response):
        """Extract round-by-round overall statistics"""
        stats_table = response.css(
            "section.b-fight-details__section:nth-of-type(3) table"
        )

        if not stats_table:
            return self._get_empty_round_overall_stats()

        try:
            rows = stats_table.css("tr.b-fight-details__table-row")
            round_stats = {}

            field_names = [
                "kd",
                "sig_str",
                "sig_str_perc",
                "total_str",
                "td",
                "td_pct",
                "sub_att",
                "rev",
                "ctrl",
            ]

            for round_num in range(1, 6):  # UFC fights can have up to 5 rounds
                if round_num <= len(rows):
                    round_row = rows[round_num - 1]
                    p_texts = round_row.css(
                        "td p.b-fight-details__table-text::text"
                    ).getall()

                    for i, field in enumerate(field_names):
                        round_stats[f"r{round_num}_{field}_1"] = (
                            p_texts[i * 2].strip() if i * 2 < len(p_texts) else None
                        )
                        round_stats[f"r{round_num}_{field}_2"] = (
                            p_texts[i * 2 + 1].strip()
                            if i * 2 + 1 < len(p_texts)
                            else None
                        )

            return round_stats
        except Exception:
            return self._get_empty_round_overall_stats()

    def _extract_sig_strike_stats(self, response):
        """Extract significant strike statistics"""
        sig_str_table = response.css("div.b-fight-details table")

        if not sig_str_table:
            return self._get_empty_sig_strike_stats()

        try:
            p_texts = sig_str_table.css(
                "td p.b-fight-details__table-text::text"
            ).getall()

            stats = {}
            field_names = [
                "sig_str_head",
                "sig_str_body",
                "sig_str_leg",
                "sig_str_dist",
                "sig_str_clinch",
                "sig_str_ground",
            ]

            for i, field in enumerate(field_names):
                idx = i + 2  # Offset to skip the first few entries
                stats[f"{field}_1"] = (
                    p_texts[idx * 2].strip() if idx * 2 < len(p_texts) else None
                )
                stats[f"{field}_2"] = (
                    p_texts[idx * 2 + 1].strip() if idx * 2 + 1 < len(p_texts) else None
                )

            return stats
        except Exception:
            return self._get_empty_sig_strike_stats()

    def _extract_round_sig_strike_stats(self, response):
        """Extract round-by-round significant strike statistics"""
        round_sig_str_table = response.css(
            "section.b-fight-details__section:nth-of-type(5) table"
        )

        if not round_sig_str_table:
            return self._get_empty_round_sig_strike_stats()

        try:
            p_texts = round_sig_str_table.css(
                "td p.b-fight-details__table-text::text"
            ).getall()
            round_stats = {}

            field_names = [
                "sig_str_head",
                "sig_str_body",
                "sig_str_leg",
                "sig_str_dist",
                "sig_str_clinch",
                "sig_str_ground",
            ]

            for round_num in range(1, 6):  # UFC fights can have up to 5 rounds
                round_key = f"round_{round_num}"
                round_data = {}

                for i, field in enumerate(field_names):
                    base_idx = (round_num - 1) * len(field_names) * 2 + i * 2
                    round_data[f"{round_key}_{field}_1"] = (
                        p_texts[base_idx].strip() if base_idx < len(p_texts) else None
                    )
                    round_data[f"{round_key}_{field}_2"] = (
                        p_texts[base_idx + 1].strip()
                        if base_idx + 1 < len(p_texts)
                        else None
                    )

                round_stats.update(round_data)

            return round_stats
        except Exception:
            return self._get_empty_round_sig_strike_stats()

    def _get_empty_overall_stats(self):
        """Return empty structure for overall stats"""
        stats = {}
        for field in [
            "kd",
            "sig_str",
            "sig_str_perc",
            "total_str",
            "td",
            "td_pct",
            "sub_att",
            "rev",
            "ctrl",
        ]:
            stats[f"{field}_1"] = None
            stats[f"{field}_2"] = None
        return stats

    def _get_empty_round_overall_stats(self):
        """Return empty structure for round stats"""
        stats = {}
        for round_num in range(1, 6):
            for field in [
                "kd",
                "sig_str",
                "sig_str_perc",
                "total_str",
                "td",
                "td_pct",
                "sub_att",
                "rev",
                "ctrl",
            ]:
                stats[f"r{round_num}_{field}_1"] = None
                stats[f"r{round_num}_{field}_2"] = None
        return stats

    def _get_empty_sig_strike_stats(self):
        """Return empty structure for significant strike stats"""
        stats = {}
        for field in [
            "sig_str_head",
            "sig_str_body",
            "sig_str_leg",
            "sig_str_dist",
            "sig_str_clinch",
            "sig_str_ground",
        ]:
            stats[f"{field}_1"] = None
            stats[f"{field}_2"] = None
        return stats

    def _get_empty_round_sig_strike_stats(self):
        """Return empty structure for round sig strike stats"""
        stats = {}
        for round_num in range(1, 6):
            for field in [
                "sig_str_head",
                "sig_str_body",
                "sig_str_leg",
                "sig_str_dist",
                "sig_str_clinch",
                "sig_str_ground",
            ]:
                stats[f"round_{round_num}_{field}_1"] = None
                stats[f"round_{round_num}_{field}_2"] = None
        return stats


##################################
# Run the scraper
##################################
def run_scraper():
    """Run the UFC spider and save data to CSV files"""
    # Configure the crawler
    process = CrawlerProcess(
        settings={
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "LOG_LEVEL": "ERROR",
        }
    )

    # Start the crawler
    process.crawl(UfcSpider)
    process.start()

    # Save event links data
    if event_links:
        pd.DataFrame(event_links).to_csv(SCRAPED_EVENT_LINKS_PATH, index=False)
        print(f"Event links successfully saved.")
    else:
        print("No event links scraped.")

    # Save fight details data
    if fights:
        pd.DataFrame(fights).to_csv(SCRAPED_FIGHTS_PATH, index=False)
        print(f"Fight details successfully saved.")
    else:
        print("No fights scraped.")

    # Print summary
    print(f"\nScraping Summary:")
    print(f"Target Pages: {PAGES_TO_SCRAPE}")
    print(f"Event Links Found: {len(event_links)}")
    print(f"Fights Parsed: {len(fights)}")


##################################
# Main execution
##################################
if __name__ == "__main__":
    start_time = datetime.now()
    run_scraper()
    end_time = datetime.now()
    print(f"\nTotal Runtime: {end_time - start_time}")
