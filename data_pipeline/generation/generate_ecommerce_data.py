import csv
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .product_generator import generate_product_variations
from .typo_utils import generate_typo
from .constants import CATEGORIES, REGIONS, EMAIL_DOMAINS, DATE_FORMATS, CATEGORY_PRICE_RANGES


class ProgressLogger:
    """Progress logger with exponential backoff to avoid log spam."""
    
    def __init__(self, logger, total_rows: int):
        self.logger = logger
        self.total_rows = total_rows
        self.start_time = time.time()
        self.last_log_time = self.start_time
        self.log_interval = 0.1  # Start with 100ms intervals
        self.max_interval = 1.0  # Cap at 1 second
        
    def should_log(self, current_time: float, is_complete: bool = False) -> bool:
        """Determine if we should log based on exponential backoff."""
        if is_complete:
            return True
        
        elapsed_since_last = current_time - self.last_log_time
        return elapsed_since_last >= self.log_interval
    
    def log_progress(self, current_row: int, current_time: float) -> None:
        """Log progress and update backoff interval."""
        progress_pct = (current_row / self.total_rows) * 100
        elapsed = current_time - self.start_time
        rows_per_sec = current_row / elapsed if elapsed > 0 else 0
        
        if current_row == self.total_rows:
            self.logger.info(f"Completed: {current_row:,}/{self.total_rows:,} rows (100.0%) - {rows_per_sec:.0f} rows/sec")
        else:
            eta_seconds = (self.total_rows - current_row) / rows_per_sec if rows_per_sec > 0 else 0
            eta_minutes = eta_seconds / 60
            self.logger.info(f"Progress: {current_row:,}/{self.total_rows:,} rows ({progress_pct:.1f}%) - {rows_per_sec:.0f} rows/sec - ETA: {eta_minutes:.1f}m")
        
        # Update timing and increase interval (exponential backoff)
        self.last_log_time = current_time
        self.log_interval = min(self.log_interval * 1.5, self.max_interval)


class EcommerceDataGenerator:
    def __init__(self, clean_data: bool = False):
        # Generate comprehensive product variations using the product generator
        self.product_variations = generate_product_variations()
        self.clean_data = clean_data

        # Use shared constants
        self.categories = CATEGORIES
        self.regions = REGIONS
        self.date_formats = DATE_FORMATS
        self.email_domains = EMAIL_DOMAINS

        # Track order IDs to allow some duplicates
        self.used_order_ids = []
        
        # Cache product keys for fast random selection
        self.product_keys = self.product_variations['_product_keys'][:-1]  # Exclude '_product_keys' itself

    def generate_order_id(self) -> str:
        # 5% chance of duplicate order ID (only when dirty data enabled)
        if not self.clean_data and self.used_order_ids and random.random() < 0.05:
            return random.choice(self.used_order_ids)

        order_id = f"ORD-{random.randint(1000000, 9999999)}"
        self.used_order_ids.append(order_id)
        return order_id

    def generate_product_name(self) -> str:
        base_product = random.choice(self.product_keys)
        product_name = random.choice(self.product_variations[base_product]["variations"])

        # 5% chance of adding spelling mistakes or data errors (only when dirty data enabled)
        if not self.clean_data and random.random() < 0.05:
            product_name = generate_typo(product_name, typo_probability=0.2)

        return product_name, base_product


    def generate_random_wrong_category(self) -> str:
        """Generate a random category (potentially with typos) for dirty data scenarios."""
        if random.random() <= 0.90:
            return random.choice(self.categories)
        return generate_typo(random.choice(self.categories))

    def generate_quantity(self, base_product: str) -> str:
        """Generate realistic quantities using precomputed data."""
        config_key = 'clean_config' if self.clean_data else 'dirty_config'
        config = self.product_variations[base_product][config_key]
        
        quantity = random.choices(config['quantity_options'], weights=config['quantity_weights'])[0]
        return str(quantity)

    def generate_unit_price(self, base_product: str) -> float:
        """Generate realistic prices using precomputed data."""
        config_key = 'clean_config' if self.clean_data else 'dirty_config'
        config = self.product_variations[base_product][config_key]
        
        min_price, max_price = config['price_range']
        
        # Use weighted distribution (more items at lower prices)
        if random.random() < 0.6:  # 60% of items in lower price range
            price = random.uniform(min_price, min_price + (max_price - min_price) * 0.4)
        elif random.random() < 0.3:  # 30% in middle range
            price = random.uniform(min_price + (max_price - min_price) * 0.4, 
                                 min_price + (max_price - min_price) * 0.8)
        else:  # 10% in higher range
            price = random.uniform(min_price + (max_price - min_price) * 0.8, max_price)
        
        return round(price, 2)


    def generate_discount_percent(self, category: str = "", unit_price: float = 0) -> float:
        """Generate realistic discount percentages based on category and price."""
        
        if self.clean_data:
            # Realistic discount distribution for clean data
            rand = random.random()
            
            # No discount (60% of items)
            if rand < 0.6:
                return 0.0
            
            # Small discounts 5-15% (25% of items)
            elif rand < 0.85:
                return round(random.uniform(0.05, 0.15), 3)
            
            # Medium discounts 20-40% (10% of items)
            elif rand < 0.95:
                return round(random.uniform(0.20, 0.40), 3)
            
            # High discounts 50-70% (5% of items - clearance, seasonal)
            else:
                return round(random.uniform(0.50, 0.70), 3)
        
        # Dirty data generation with realistic bias
        rand = random.random()
        
        if rand < 0.75:  # 75% realistic discounts
            # Weight distribution toward common discount patterns
            discount_ranges = [
                (0.0, 0.0, 0.4),      # No discount - 40% weight
                (0.05, 0.10, 0.2),    # 5-10% discount - 20% weight  
                (0.15, 0.25, 0.15),   # 15-25% discount - 15% weight
                (0.30, 0.50, 0.1),    # 30-50% discount - 10% weight
                (0.60, 0.80, 0.05)    # 60-80% discount - 5% weight (clearance)
            ]
            
            # Select range based on weights
            weights = [r[2] for r in discount_ranges]
            selected_range = random.choices(discount_ranges, weights=weights)[0]
            
            if selected_range[0] == selected_range[1]:  # No discount case
                return 0.0
            else:
                return round(random.uniform(selected_range[0], selected_range[1]), 3)
                
        elif rand < 0.95:  # 20% edge cases but still somewhat realistic
            # Unusual but possible discounts
            return round(random.choice([0.85, 0.90, 0.95, 0.99]), 3)
            
        else:  # 5% problematic values
            # Values > 1 or negative (data quality issues)
            return round(random.uniform(-0.1, 2.5), 3)

    def generate_region(self) -> str:
        # TODO: Implement clean region normalization when clean_data=True
        return random.choice(self.regions)

    def generate_sale_date(self) -> str:
        # 10% chance of null/empty date (only when dirty data enabled)
        if not self.clean_data and random.random() < 0.1:
            return random.choice(["", "NULL", "N/A"])

        # Generate random date in the last 2 years
        start_date = datetime.now() - timedelta(days=730)
        random_days = random.randint(0, 730)
        sale_date = start_date + timedelta(days=random_days)

        # Use random date format (consistent format when clean data enabled)
        if self.clean_data:
            date_format = "%Y-%m-%d"
        else:
            date_format = random.choice(self.date_formats)
        return sale_date.strftime(date_format)

    def generate_customer_email(self) -> str:
        # 15% chance of null/empty email (only when dirty data enabled)
        if not self.clean_data and random.random() < 0.15:
            return random.choice(["", "NULL", "N/A"])

        # Generate realistic but sometimes flawed emails
        first_names = [
            "john", "jane", "mike", "sarah", "david", "lisa", "chris", "anna", "robert", "mary",
            "james", "patricia", "michael", "jennifer", "william", "linda", "richard", "elizabeth",
            "joseph", "barbara", "thomas", "susan", "charles", "jessica", "christopher", "karen",
            "daniel", "nancy", "matthew", "helen", "anthony", "betty", "mark", "dorothy",
            "donald", "sandra", "steven", "donna", "paul", "carol", "andrew", "ruth", "joshua",
            "sharon", "kenneth", "michelle", "kevin", "laura", "brian", "sarah", "george", "kimberly"
        ]
        last_names = [
            "smith", "johnson", "brown", "davis", "wilson", "moore", "taylor", "anderson",
            "thomas", "jackson", "white", "harris", "martin", "thompson", "garcia", "martinez",
            "robinson", "clark", "rodriguez", "lewis", "lee", "walker", "hall", "allen",
            "young", "hernandez", "king", "wright", "lopez", "hill", "scott", "green",
            "adams", "baker", "gonzalez", "nelson", "carter", "mitchell", "perez", "roberts",
            "turner", "phillips", "campbell", "parker", "evans", "edwards", "collins", "stewart"
        ]

        first = random.choice(first_names)
        last = random.choice(last_names)
        domain = random.choice(self.email_domains)

        # 5% chance of malformed email (only when dirty data enabled)
        if not self.clean_data and random.random() < 0.05:
            return random.choice([
                f"{first}.{last}@",
                f"{first}@{domain}",
                f"{first}.{last}.{domain}",
                f"{first}..{last}@{domain}"
            ])

        return f"{first}.{last}@{domain}"

    def calculate_revenue(self, unit_price: float, quantity_str: str, discount_percent: float) -> float:
        # Try to parse quantity, default to 0 if can't
        try:
            quantity = int(float(quantity_str))
            if quantity < 0:
                quantity = 0
        except (ValueError, TypeError):
            quantity = 0

        # Calculate revenue (ignore if specified, but we'll calculate anyway for realism)
        base_revenue = unit_price * quantity
        discount_amount = base_revenue * min(max(discount_percent, 0), 1)  # Cap discount
        return round(base_revenue - discount_amount, 2)

    def generate_row(self) -> Dict[str, Any]:
        order_id = self.generate_order_id()
        product_name, base_product = self.generate_product_name()
        
        # Get correct category from product catalog
        correct_category = self.product_variations[base_product]["category"]
        
        # Use correct category or introduce errors for dirty data
        if self.clean_data:
            category = correct_category
        else:
            # For dirty data: 90% correct category, 10% wrong category
            if random.random() < 0.9:
                category = correct_category
                # Add typos to correct category 20% of the time
                if random.random() < 0.2:
                    category = generate_typo(category)
            else:
                # Use completely wrong category 10% of the time
                category = self.generate_random_wrong_category()
        
        quantity = self.generate_quantity(base_product)
        unit_price = self.generate_unit_price(base_product)
        discount_percent = self.generate_discount_percent(category, unit_price)
        region = self.generate_region()
        sale_date = self.generate_sale_date()
        customer_email = self.generate_customer_email()
        revenue = self.calculate_revenue(unit_price, quantity, discount_percent)

        return {
            "order_id": order_id,
            "product_name": product_name,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_percent": discount_percent,
            "region": region,
            "sale_date": sale_date,
            "customer_email": customer_email,
            "revenue": revenue
        }

    def generate_csv(self, filename: str, num_rows: int = 1000):
        logger = logging.getLogger(__name__)
        
        fieldnames = [
            "order_id", "product_name", "category", "quantity", "unit_price",
            "discount_percent", "region", "sale_date", "customer_email", "revenue"
        ]

        logger.info(f"Starting CSV generation: {num_rows} rows to '{filename}'")
        logger.info(f"Data mode: {'clean' if self.clean_data else 'dirty (with errors)'}")
        
        progress_logger = ProgressLogger(logger, num_rows)
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_rows):
                row = self.generate_row()
                writer.writerow(row)
                
                current_time = time.time()
                is_complete = (i + 1) == num_rows
                
                if progress_logger.should_log(current_time, is_complete):
                    progress_logger.log_progress(i + 1, current_time)

        logger.info(f"Successfully generated {num_rows:,} rows of e-commerce data in '{filename}'")


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)
    
    generator = EcommerceDataGenerator()

    # Generate a small test sample first to verify data errors
    logger.info("Generating test sample with data errors...")
    generator.generate_csv("test_sample.csv", 5000)
    
    # Generate clean data sample
    logger.info("Generating clean data sample...")
    clean_generator = EcommerceDataGenerator(clean_data=True)
    clean_generator.generate_csv("sample_clean.csv", 100000000)

    # Show some sample product names with errors
    logger.info("Sample product names with data errors:")
    for i in range(10):
        product = generator.generate_product_name()
        logger.info(f"- {product}")

    logger.info("To generate full dataset, uncomment the line below:")
    logger.info("# generator.generate_csv('ecommerce_data_100m.csv', 100000000)")

    logger.info("Sample of data quality issues you'll find:")
    logger.info("- Duplicate order IDs (~5% chance)")
    logger.info("- Product name variations (iPhone vs iphone vs I-Phone)")
    logger.info("- Product name spelling errors (~15% chance): iPhoen, Samsng, MacBok")
    logger.info("- Data corruption: missing chars, extra spaces, punctuation")
    logger.info("- Inconsistent categories (Electronics vs electronics)")
    logger.info("- Invalid quantities (negative, zero, strings like 'two')")
    logger.info("- Discount percentages > 1 or negative")
    logger.info("- Region spelling mistakes (Eurpoe instead of Europe)")
    logger.info("- Mixed date formats and null dates")
    logger.info("- Missing or malformed email addresses")
