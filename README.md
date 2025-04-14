# UFC Fight Data Analysis

This project combines web scraping and data wrangling to analyze UFC fight statistics. It consists of two main Python scripts:

1. **Web Scraping**: Extract UFC fight data from ufcstats.com using Python
2. **Data Wrangling**: Clean and transform the data for analysis

I plan to use this data in the future of other ML related projects.

<img src="./Images/logo.png" width="300" alt="UFC Logo">

## Scraped Data Description
The dataset contains 8,066 UFC fights with comprehensive statistics. The data includes the following structure:

### Fight Details
- Event information (date, location)
- Fight metadata (division, method, referee, stoppage details)
- Time format and round information
- Fight outcome

### Overall Fight Statistics
For each fighter in a bout, the following statistics are collected:
- Basic information (name, stage name, win/loss record)
- Knockdowns (KD)
- Significant strikes (landed, attempted, percentage)
- Total strikes (landed, attempted)
- Takedowns (landed, attempted, percentage)
- Submission attempts
- Reversals
- Control time

### Round-by-Round Fight Statistics
Detailed statistics for each round (up to 5 rounds) including:
- Significant strikes by target (head, body, leg)
- Significant strikes by position (distance, clinch, ground)
- Knockdowns
- Takedowns
- Submission attempts
- Reversals
- Control time

## Installation
To install dependencies, run:
```sh
pip install -r requirements.txt
```

## Project Structure
```
UFC_data_webscraping/
├── Data/              # Data storage directory
├── Images/            # Generated visualizations
├── Scripts/           # Python scripts
│   ├── web_scraping.py    # Web scraping implementation
│   └── data_wrangling.py  # Data processing and analysis
├── README.md          # Project documentation
└── requirements.txt   # Dependencies
```

## Usage
1. Run the web scraping script to collect data:
```sh
python Scripts/web_scraping.py
```

2. Process and analyze the data:
```sh
python Scripts/data_wrangling.py
```

## Dependencies
All required dependencies are listed in `requirements.txt`. 

## License
This project is licensed under the MIT License.

