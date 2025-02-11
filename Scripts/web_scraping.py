import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess

# Initialize List to Store UFC Fights Data
pages = 1  # Set the number of pages to scrape
event_links = []  # List to store event links separately
fights = []  # List to store fight details


# Define a Scrapy Spider to scrape UFC fight data
class UfcSpider(scrapy.Spider):
    name = "ufc_spider"  # Name of the spider

    def start_requests(self):
        """
        Start by sending requests to the UFC statistics events page for each page.
        """
        for p in range(1, pages + 1):
            url = f"http://ufcstats.com/statistics/events/completed?page={p}"
            yield scrapy.Request(url=url, callback=self.parse_main)

    def parse_main(self, response):
        """
        Extract event links from the page and follow them.
        """
        event_links_on_page = response.css(
            "a.b-link.b-link_style_black::attr(href)"
        ).extract()

        for e in event_links_on_page:
            event_links.append({"event_link": e})  # Store event link
            yield response.follow(url=e, callback=self.parse_events)

    def parse_events(self, response):
        """
        Extract fight links, date, and location from the event page.
        Follow each fight link to extract fight details.
        """
        fight_links = response.css("a.b-flag.b-flag_style_green::attr(href)").extract()
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
        time = fight_details.css("p:nth-child(1) > i:nth-child(3)::text").extract()[1]
        time_format = fight_details.css(
            "p:nth-child(1) > i:nth-child(4)::text"
        ).extract()[1]
        referee = fight_details.css(
            "p:nth-child(1) > i:nth-child(5) > span::text"
        ).get()
        details = fight_details.css("p:nth-child(2)::text").extract()[1].strip()

        # Extract fight statistics
        stats_table = response.css(
            "body > section > div > div > section:nth-child(4) > table > tbody"
        )
        kd_1, kd_2 = stats_table.css("td:nth-child(2) p::text").extract()[0:2]
        sig_str_1, sig_str_2 = stats_table.css("td:nth-child(3) p::text").extract()[0:2]
        total_str_1, total_str_2 = stats_table.css("td:nth-child(5) p::text").extract()[
            0:2
        ]

        # Append all extracted fight details to the fights list
        fights.append(
            {
                "fight_link": response.url,
                "date": date,
                "location": location,
                "method": method,
                "round": round_num,
                "time": time,
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
                "total_str_1": total_str_1,
                "total_str_2": total_str_2,
            }
        )


# Run the Scrapy spider
process = CrawlerProcess()
process.crawl(UfcSpider)
process.start()

# Convert the results to DataFrames
event_links_df = pd.DataFrame(event_links)
fights_df = pd.DataFrame(fights)

# Clean string columns in the fights DataFrame
fights_df = fights_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Specify directory to save files
save_path = "C:\\Users\\ahlaw\\OneDrive - UBC\\Documents\\vscode\\Projects\\UFC_data_webscraping\\Data\\Raw\\"

# Save DataFrames as CSV files
event_links_df.to_csv(save_path + "event_links.csv", index=False)
fights_df.to_csv(save_path + "fight_details.csv", index=False)

# Display success message
print("DataFrames saved successfully in:", save_path)
