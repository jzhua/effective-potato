import csv
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .product_generator import generate_product_variations
from .typo_utils import generate_typo
from .constants import CATEGORIES, REGIONS, EMAIL_DOMAINS, DATE_FORMATS, CATEGORY_PRICE_RANGES, CATEGORY_DISCOUNT_RANGES


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

        # Track order IDs with incremental counter
        self.order_id_counter = 1
        self.used_order_ids = []
        self.max_stored_order_ids = 1000  # Just need a sample for duplicates
        
        # Cache product keys for fast random selection
        self.product_keys = self.product_variations['_product_keys'][:-1]  # Exclude '_product_keys' itself
        
        # Cache date calculations for performance
        self.base_date = datetime.now() - timedelta(days=730)
        self.null_dates = ["", "NULL", "N/A"]
        
        # Cache other frequently used lists
        self.heavy_discounts = [0.85, 0.90, 0.95, 0.99]
        
        # Cache name lists for email generation
        self.first_names = [
            "john", "jane", "mike", "sarah", "david", "lisa", "chris", "anna", "robert", "mary",
            "james", "patricia", "michael", "jennifer", "william", "linda", "richard", "elizabeth",
            "joseph", "barbara", "thomas", "susan", "charles", "jessica", "christopher", "karen",
            "daniel", "nancy", "matthew", "helen", "anthony", "betty", "mark", "dorothy",
            "donald", "sandra", "steven", "donna", "paul", "carol", "andrew", "ruth", "joshua",
            "sharon", "kenneth", "michelle", "laura", "sarah", "kimberly"
        ]
        self.last_names = [
            "smith", "johnson", "brown", "davis", "wilson", "moore", "taylor", "anderson",
            "jackson", "white", "harris", "martin", "thompson", "garcia", "martinez", "robinson",
            "clark", "rodriguez", "lewis", "lee", "walker", "hall", "allen", "young", "hernandez",
            "king", "wright", "lopez", "hill", "scott", "green", "adams", "baker", "gonzalez",
            "nelson", "carter", "mitchell", "perez", "roberts", "turner", "phillips", "campbell",
            "parker", "evans", "edwards", "collins", "stewart", "sanchez", "morris", "rogers",
            "reed", "cook", "morgan", "bell", "murphy", "bailey", "rivera", "cooper", "richardson",
            "cox", "howard", "ward", "torres", "peterson", "gray", "ramirez", "james", "watson"
        ]

    def generate_order_id(self) -> str:
        # 1% chance of duplicate order ID (only when dirty data enabled)
        if not self.clean_data and self.used_order_ids and random.random() < 0.01:
            return random.choice(self.used_order_ids)

        order_id = f"ORD-{self.order_id_counter:09d}"
       
        # Increment counter for next order ID
        self.order_id_counter += 1
        
        # Only store a sample of order IDs for duplicate generation
        if len(self.used_order_ids) < self.max_stored_order_ids:
            self.used_order_ids.append(order_id)
        elif random.random() < 0.1:  # 10% chance to replace a random existing ID
            self.used_order_ids[random.randint(0, len(self.used_order_ids) - 1)] = order_id
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

    def generate_category(self, base_product: str) -> tuple[str, str]:
        """Generate category based on product, with potential errors for dirty data.
        
        Returns:
            tuple: (displayed_category, correct_category) - displayed may have typos, correct is always valid
        """
        # Get correct category from product catalog
        correct_category = self.product_variations[base_product]["category"]
        
        # Use correct category or introduce errors for dirty data
        if self.clean_data:
            return correct_category, correct_category
        else:
            # For dirty data: 90% correct category, 10% wrong category
            if random.random() < 0.9:
                category = correct_category
                # Add typos to correct category 20% of the time
                if random.random() < 0.2:
                    category = generate_typo(category)
                return category, correct_category
            else:
                # Use completely wrong category 10% of the time
                return self.generate_random_wrong_category(), correct_category

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
        """Generate realistic discount percentages based on category-specific ranges."""
        
        # Get category-specific discount settings
        discount_config = CATEGORY_DISCOUNT_RANGES[category]
        min_discount, max_discount, frequency = discount_config
        
        if self.clean_data:
            # For clean data, only use category-appropriate discounts (no edge cases or errors)
            if random.random() > frequency:
                return 0.0  # No discount based on category frequency
            
            # Apply category-specific discount range
            return round(random.uniform(min_discount, max_discount), 3)
        
        # For dirty data, use category-specific logic with some data quality issues
        rand = random.random()
        
        if rand < 0.94:  # 94% category-appropriate discounts
            if random.random() > frequency:
                return 0.0  # No discount based on category frequency
            
            # Apply category-specific discount range
            return round(random.uniform(min_discount, max_discount), 3)
                
        elif rand < 0.99:  # 5% edge cases but still somewhat realistic
            # Unusual but possible discounts (ignoring category)
            return round(random.choice(self.heavy_discounts), 3)
            
        else:  # 1% problematic values
            # Values > 1 or negative (data quality issues)
            return round(random.uniform(-0.1, 2.5), 3)

    def generate_region(self) -> str:
        # TODO: Implement clean region normalization when clean_data=True
        return random.choice(self.regions)

    def generate_sale_date(self) -> str:
        """Generate sale dates with realistic patterns.
        
        Goals:
        1. Cover the last 730 days (2 years) of data
        2. Skew slightly toward more recent dates (recency bias)
        3. Add seasonal shopping patterns (more sales during holiday periods)
        4. Maintain some randomness to avoid predictable patterns
        """
        # 10% chance of null/empty date (only when dirty data enabled)
        if not self.clean_data and random.random() < 0.1:
            return random.choice(self.null_dates)

        from datetime import timedelta
        current_date = datetime.now()
        
        # 60% chance of more recent dates (last 365 days), 40% chance of older dates (365-730 days ago)
        if random.random() < 0.6:
            days_back = random.randint(0, 365)
        else:
            days_back = random.randint(365, 730)
        
        target_date = current_date - timedelta(days=days_back)
        
        # Apply seasonal bias by adjusting the date within a small window
        seasonal_adjustment = 0
        month = target_date.month
        
        rand = random.random()
        if rand < 0.2:  # 20% - Holiday season bias (Nov-Dec)
            if month not in [11, 12]:
                if month < 11:
                    seasonal_adjustment = random.randint(0, 30)
                else:
                    seasonal_adjustment = random.randint(-30, 0)
        elif rand < 0.35:  # 15% - Back to school bias (Aug-Sep)
            if month not in [8, 9]:
                if month < 8:
                    seasonal_adjustment = random.randint(0, 20)
                elif month > 9:
                    seasonal_adjustment = random.randint(-20, 0)
        elif rand < 0.45:  # 10% - Spring shopping bias (Mar-Apr)
            if month not in [3, 4]:
                if month < 3:
                    seasonal_adjustment = random.randint(0, 15)
                elif month > 4:
                    seasonal_adjustment = random.randint(-15, 0)
        elif rand < 0.55:  # 10% - Summer shopping bias (Jun-Jul)
            if month not in [6, 7]:
                if month < 6:
                    seasonal_adjustment = random.randint(0, 15)
                elif month > 7:
                    seasonal_adjustment = random.randint(-15, 0)
        # else: 45% - No seasonal adjustment
        
        # Apply seasonal adjustment while staying within 730-day range
        adjusted_days_back = max(0, min(730, days_back - seasonal_adjustment))
        sale_date = current_date - timedelta(days=adjusted_days_back)

        # Use random date format (consistent format when clean data enabled)
        if self.clean_data:
            return sale_date.strftime("%Y-%m-%d")
        else:
            date_format = random.choice(self.date_formats)
            return sale_date.strftime(date_format)

    def generate_customer_email(self) -> str:
        # 15% chance of null/empty email (only when dirty data enabled)
        if not self.clean_data and random.random() < 0.15:
            return random.choice(["", "NULL", "N/A"])

        # Generate realistic but sometimes flawed emails using cached lists
        first = random.choice(self.first_names)
        last = random.choice(self.last_names)
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
        category, correct_category = self.generate_category(base_product)
        quantity = self.generate_quantity(base_product)
        unit_price = self.generate_unit_price(base_product)
        discount_percent = self.generate_discount_percent(correct_category, unit_price)
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
