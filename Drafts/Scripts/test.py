import requests
import pandas as pd
from scrapy.http import TextResponse

# Step 1: Request and wrap response
url = "http://ufcstats.com/fight-details/0e71e69359db4d1e"
html = requests.get(url).text
response = TextResponse(url=url, body=html, encoding='utf-8')


#------------------------------------------------------------------
table = response.css("div.b-fight-details__content")

division = response.css("i.b-fight-details__fight-title::text").getall()[-1].strip()
method = table.css("p:nth-child(1) > i.b-fight-details__text-item_first > i:nth-child(2)::text").getall()[-1].strip()
last_round = table.css("p:nth-child(1) > i:nth-child(2)::text").getall()[-1].strip()
last_round_time = table.css("p:nth-child(1) > i:nth-child(3)::text").getall()[-1].strip()
time_format = table.css("p:nth-child(1) > i:nth-child(4)::text").getall()[-1].strip()
referee = table.css("p:nth-child(1) > i:nth-child(5) > span::text").getall()[-1].strip()
stoppage_details = table.css("p:nth-child(2)::text").getall()[-1].strip()

print(division, method, last_round, last_round_time, time_format, referee, stoppage_details)

#------------------------------------------------------------------
table = response.css("section.b-fight-details__section:nth-of-type(2) table")
p_texts = table.css("td p.b-fight-details__table-text::text").extract()

# Assign values from extracted data
overall_stats = {
    "kd_1": p_texts[2].strip() if p_texts[2] else None,
    "kd_2": p_texts[3].strip() if p_texts[3] else None,
    "sig_str_1": p_texts[4].strip() if p_texts[4] else None,
    "sig_str_2": p_texts[5].strip() if p_texts[5] else None,
    "sig_str_perc_1": p_texts[6].strip() if p_texts[6] else None,
    "sig_str_perc_2": p_texts[7].strip() if p_texts[7] else None,
    "total_str_1": p_texts[8].strip() if p_texts[8] else None,
    "total_str_2": p_texts[9].strip() if p_texts[9] else None,
    "td_1": p_texts[10].strip() if p_texts[10] else None,
    "td_2": p_texts[11].strip() if p_texts[11] else None,
    "td_pct_1": p_texts[12].strip() if p_texts[12] else None,
    "td_pct_2": p_texts[13].strip() if p_texts[13] else None,
    "sub_att_1": p_texts[14].strip() if p_texts[14] else None,
    "sub_att_2": p_texts[15].strip() if p_texts[15] else None,
    "rev_1": p_texts[16].strip() if p_texts[16] else None,
    "rev_2": p_texts[17].strip() if p_texts[17] else None,
    "ctrl_1": p_texts[18].strip() if p_texts[18] else None,
    "ctrl_2": p_texts[19].strip() if p_texts[19] else None,
}

#---------------------------------------------------------------
sig_strikes_table = response.css("body > section > div > div > table")

p_texts = sig_strikes_table.css("td p.b-fight-details__table-text::text").getall()

# Assign values from extracted data
sig_strikes = {
    "sig_str_1": p_texts[4].strip() if len(p_texts) > 4 else None,
    "sig_str_2": p_texts[5].strip() if len(p_texts) > 5 else None,
    "sig_str_perc_1": p_texts[6].strip() if len(p_texts) > 6 else None,
    "sig_str_perc_2": p_texts[7].strip() if len(p_texts) > 7 else None,
    "head_1": p_texts[8].strip() if len(p_texts) > 8 else None,
    "head_2": p_texts[9].strip() if len(p_texts) > 9 else None,
    "body_1": p_texts[10].strip() if len(p_texts) > 10 else None,
    "body_2": p_texts[11].strip() if len(p_texts) > 11 else None,
    "leg_1": p_texts[12].strip() if len(p_texts) > 12 else None,
    "leg_2": p_texts[13].strip() if len(p_texts) > 13 else None,
    "distance_1": p_texts[14].strip() if len(p_texts) > 14 else None,
    "distance_2": p_texts[15].strip() if len(p_texts) > 15 else None,
    "clinch_1": p_texts[16].strip() if len(p_texts) > 16 else None,
    "clinch_2": p_texts[17].strip() if len(p_texts) > 17 else None,
    "ground_1": p_texts[18].strip() if len(p_texts) > 18 else None,
    "ground_2": p_texts[19].strip() if len(p_texts) > 19 else None,
}
#------------------------------------------------------------------
# Step 2: Select the full round stats table
round_overall_stats = {}

table = response.css("section.b-fight-details__section:nth-of-type(3) table")
round_rows = table.css("tbody tr.b-fight-details__table-row")

# Loop through rounds 1-5
for round_num in range(1, 6):
    # Find the corresponding row for this round if it exists
    if round_num <= len(round_rows):
        round_row = round_rows[round_num - 1]
        p_texts = round_row.css("p.b-fight-details__table-text::text").getall()

        if len(p_texts) > 20:
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

# Step 4: Convert to DataFrame
df = pd.DataFrame([round_overall_stats])
print(df)
#------------------------------------------------------------------

round_sig_str_stats = {}

table = response.css("section.b-fight-details__section:nth-of-type(5) table")
round_rows = table.css("tbody tr.b-fight-details__table-row")

for round_num in range(1, 6):
    if round_num <= len(round_rows):
        round_row = round_rows[round_num - 1]
        p_texts = round_row.css("p.b-fight-details__table-text::text").getall()

        if len(p_texts) > 18:
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
            continue
    else:
        continue

df = pd.DataFrame([round_sig_str_stats])
print(df)
#------------------------------------------------------------------
table = response.css("section.b-fight-details__section:nth-of-type(5) table")
round_rows = table.css("tbody tr.b-fight-details__table-row")

round_sig_str_stats = {}

for round_num in range(1, 6):
    if round_num <= len(round_rows):
        round_row = round_rows[0]
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
            continue    
    else:
        continue

df = pd.DataFrame([round_sig_str_stats])
df
#------------------------------------------------------------------
