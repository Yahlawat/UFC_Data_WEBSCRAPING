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
    name = "ufc_spider"  # Name of the spider

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
        """
        Extract fight links, date, and location from the event page.
        Follow each fight link to extract fight details.
        """
        fight_links = response.css(
            "tr.b-fight-details__table-row[data-link]::attr(data-link)"
        ).getall()
        date = response.css("li.b-list__box-list-item:nth-child(1)::text").extract()[1]
        location = response.css(
            "li.b-list__box-list-item:nth-child(2)::text"
        ).extract()[1]

        for f in fight_links:
            yield response.follow(
                url=f,
                callback=self.parse_fights,
                meta={"date": date, "location": location},
            )

    def parse_fights(self, response):
        """
        Extract fight details such as fighters, results, and statistics.
        """
        date = response.meta["date"]  # Extract date from metadata
        location = response.meta["location"]  # Extract location from metadata

        # Extract fighter details
        fighter_details = response.css("div.b-fight-details__person")
        win_loss_1, win_loss_2 = fighter_details.css(
            "i.b-fight-details__person-status::text"
        ).extract()[0:2]
        name_1, name_2 = fighter_details.css("h3 > a::text").extract()[0:2]
        stage_name_1, stage_name_2 = fighter_details.css(
            "p.b-fight-details__person-title::text"
        ).extract()[0:2]

        # Extract fight details
        fight_details = response.css("div.b-fight-details__content")
        method = fight_details.css(
            "p:nth-child(1) > i.b-fight-details__text-item_first > i:nth-child(2)::text"
        ).get()
        round_num = fight_details.css(
            "p:nth-child(1) > i:nth-child(2)::text"
        ).extract()[1]
        end_time = fight_details.css("p:nth-child(1) > i:nth-child(3)::text").extract()[
            1
        ]
        time_format = fight_details.css(
            "p:nth-child(1) > i:nth-child(4)::text"
        ).extract()[1]
        referee = fight_details.css(
            "p:nth-child(1) > i:nth-child(5) > span::text"
        ).get()
        details = fight_details.css("p:nth-child(2)::text").extract()[1].strip()

        # Extract overall fight statistics
        overall_stats_table = response.css(
            "section.b-fight-details__section:nth-of-type(2) table"
        )

        kd_1, kd_2 = overall_stats_table.css("td:nth-child(2) p::text").extract()[0:2]
        sig_str_1, sig_str_2 = overall_stats_table.css(
            "td:nth-child(3) p::text"
        ).extract()[0:2]
        sig_str_perc_1, sig_str_perc_2 = overall_stats_table.css(
            "td:nth-child(4) p::text"
        ).extract()[0:2]
        total_str_1, total_str_2 = overall_stats_table.css(
            "td:nth-child(5) p::text"
        ).extract()[0:2]
        td_1, td_2 = overall_stats_table.css("td:nth-child(7) p::text").extract()[0:2]
        td_pct_1, td_pct_2 = overall_stats_table.css(
            "td:nth-child(9) p::text"
        ).extract()[0:2]
        sub_att_1, sub_att_2 = overall_stats_table.css(
            "td:nth-child(11) p::text"
        ).extract()[0:2]
        rev_1, rev_2 = overall_stats_table.css("td:nth-child(13) p::text").extract()[
            0:2
        ]
        ctrl_1, ctrl_2 = overall_stats_table.css("td:nth-child(15) p::text").extract()[
            0:2
        ]

        # Extract round-by-round overall statistics
        round_overall_stats_table = response.css(
            "section.b-fight-details__section:nth-of-type(3) table"
        )

        # Append all extracted fight details to the fights list
        fights.append(
            {
                "fight_link": response.url,
                "date": date,
                "location": location,
                "method": method,
                "round": round_num,
                "end_time": end_time,
                "time_format": time_format,
                "referee": referee,
                "details": details,
                "name_1": name_1,
                "name_2": name_2,
                "stage_name_1": stage_name_1,
                "stage_name_2": stage_name_2,
                "win_loss_1": win_loss_1,
                "win_loss_2": win_loss_2,
                "kd_1": kd_1,
                "kd_2": kd_2,
                "sig_str_1": sig_str_1,
                "sig_str_2": sig_str_2,
                "sig_str_perc_1": sig_str_perc_1,
                "sig_str_perc_2": sig_str_perc_2,
                "total_str_1": total_str_1,
                "total_str_2": total_str_2,
                "td_1": td_1,
                "td_2": td_2,
                "td_pct_1": td_pct_1,
                "td_pct_2": td_pct_2,
                "sub_att_1": sub_att_1,
                "sub_att_2": sub_att_2,
                "rev_1": rev_1,
                "rev_2": rev_2,
                "ctrl_1": ctrl_1,
                "ctrl_2": ctrl_2,
            }
        )
