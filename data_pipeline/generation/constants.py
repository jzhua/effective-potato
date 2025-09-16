"""Shared constants for the data generation pipeline."""

# Standard product categories used throughout the e-commerce data pipeline
CATEGORIES = [
    "Electronics",
    "Clothing", 
    "Sports",
    "Home & Garden",
    "Books",
    "Health & Beauty",
    "Automotive",
    "Toys & Games",
    "Kitchen",
    "Office",
    "Pet Supplies",
    "Jewelry & Accessories",
    "Baby & Kids",
    "Grocery & Food",
    "Garden & Outdoor",
    "Musical Instruments",
    "Craft & Hobby",
    "Tools & Hardware",
    "Travel & Luggage"
]

# Price ranges by category (min_price, max_price)
CATEGORY_PRICE_RANGES = {
    "Electronics": (50, 2500),
    "Clothing": (15, 200), 
    "Sports": (20, 800),
    "Home & Garden": (25, 1000),
    "Books": (8, 50),
    "Health & Beauty": (5, 150),
    "Automotive": (20, 500),
    "Toys & Games": (10, 150),
    "Kitchen": (15, 800),
    "Office": (5, 300),
    "Pet Supplies": (8, 200),
    "Jewelry & Accessories": (25, 2000),
    "Baby & Kids": (15, 500),
    "Grocery & Food": (2, 50),
    "Garden & Outdoor": (10, 1500),
    "Musical Instruments": (50, 3000),
    "Craft & Hobby": (5, 200),
    "Tools & Hardware": (15, 800),
    "Travel & Luggage": (30, 600)
}

# Regions with intentional spelling mistakes for data quality testing
REGIONS = [
    "North America", "north america", "N America", "North Ameica", "NA", "United States", "US", "Canada", "USA",
    "Europe", "europe", "EUROPE", "Eurpoe", "EU", "United Kingdom", "UK", "Germany", "France", "Spain",
    "Asia", "asia", "ASIA", "Aisa", "China", "Japan", "India", "South Korea", "Southeast Asia",
    "South America", "south america", "S America", "Latin America", "Brazil", "Argentina", "Chile",
    "Australia", "australia", "Austrailia", "Oceania", "New Zealand", "AU",
    "Africa", "africa", "AFRICA", "Middle East", "South Africa", "Nigeria", "Egypt",
    "Eastern Europe", "Western Europe", "Central America", "Caribbean", "Scandinavia", "Nordic"
]

# Email domains for customer email generation
EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "company.com", "icloud.com",
    "aol.com", "protonmail.com", "live.com", "msn.com", "comcast.net", "verizon.net",
    "att.net", "sbcglobal.net", "cox.net", "charter.net", "earthlink.net", "juno.com",
    "workmail.com", "business.com", "corp.com", "enterprise.com", "organization.org"
]

# Date formats for varied date representation
DATE_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%Y/%m/%d"
]