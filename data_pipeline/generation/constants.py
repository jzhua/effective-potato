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

# Regions focused on India and Southeast Asia with intentional spelling mistakes for data quality testing
REGIONS = [
    # Major Indian cities/states
    "Mumbai", "Mumbay", "Bombay",
    "Delhi", "Dehli", "New Delhi",
    "Bangalore", "Bengaluru", "Bangalor",
    "Chennai", "Chenai", "Madras",
    "Kolkata", "Kolkatta", "Calcutta",
    
    # Southeast Asian countries
    "Thailand", "Thailnd",
    "Singapore", "Singapre",
    "Malaysia", "Malaysa", "Malasia",
    "Indonesia", "indonsia",
    "Vietnam", "Viet Nam",
    "Philippines", "Philipines", "phillipines",
]

# Email domains for customer email generation
EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "company.com", "icloud.com",
    "aol.com", "protonmail.com", "live.com", "msn.com", "comcast.net", "verizon.net",
    "att.net", "sbcglobal.net", "cox.net", "charter.net", "earthlink.net", "juno.com",
    "workmail.com", "business.com", "corp.com", "enterprise.com", "organization.org"
]

# Category-specific discount ranges (min_discount, max_discount, frequency)
# Frequency represents how often discounts are applied (0.0 = never, 1.0 = always)
CATEGORY_DISCOUNT_RANGES = {
    "Electronics": (0.05, 0.25, 0.4),      # Tech has moderate discounts
    "Clothing": (0.10, 0.70, 0.6),         # Fashion has frequent sales
    "Sports": (0.05, 0.40, 0.3),           # Sports gear moderate discounts
    "Home & Garden": (0.05, 0.50, 0.4),    # Home goods regular sales
    "Books": (0.10, 0.30, 0.2),            # Books rarely discounted much
    "Health & Beauty": (0.05, 0.60, 0.5),  # Beauty products frequent sales
    "Automotive": (0.05, 0.20, 0.2),       # Auto parts rarely discounted
    "Toys & Games": (0.10, 0.50, 0.5),     # Toys seasonal discounts
    "Kitchen": (0.05, 0.45, 0.4),          # Kitchen appliances moderate
    "Office": (0.05, 0.30, 0.3),           # Office supplies moderate
    "Pet Supplies": (0.05, 0.40, 0.3),     # Pet products moderate
    "Jewelry & Accessories": (0.10, 0.60, 0.4),  # Jewelry frequent sales
    "Baby & Kids": (0.05, 0.40, 0.4),      # Baby products moderate
    "Grocery & Food": (0.05, 0.30, 0.2),   # Food rarely heavily discounted
    "Garden & Outdoor": (0.05, 0.50, 0.4), # Seasonal outdoor equipment
    "Musical Instruments": (0.05, 0.30, 0.2),  # Instruments rarely discounted
    "Craft & Hobby": (0.10, 0.50, 0.4),    # Craft supplies moderate
    "Tools & Hardware": (0.05, 0.35, 0.3), # Tools moderate discounts
    "Travel & Luggage": (0.10, 0.60, 0.4)  # Travel gear seasonal sales
}

# Date formats for varied date representation
DATE_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%Y/%m/%d"
]
