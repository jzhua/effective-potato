import random
try:
    from .typo_utils import generate_typo
    from .constants import CATEGORIES, CATEGORY_PRICE_RANGES
except ImportError:
    # For when running as main script
    from typo_utils import generate_typo
    from constants import CATEGORIES, CATEGORY_PRICE_RANGES


def generate_product_variations():
    # Electronics
    electronics_base = [
        # Phones with actual models
        "iPhone 12", "iPhone 13", "iPhone 14", "iPhone 15", "iPhone SE", "iPhone Pro", "iPhone Pro Max",
        "Samsung Galaxy S21", "Samsung Galaxy S22", "Samsung Galaxy S23", "Samsung Galaxy Note", 
        "Samsung Galaxy A54", "Samsung Galaxy Z Fold", "Samsung Galaxy Z Flip",
        "Google Pixel 6", "Google Pixel 7", "Google Pixel 8", "Google Pixel Pro",
        "OnePlus 11", "OnePlus Nord", "OnePlus 10T", "Xiaomi 13", "Xiaomi Redmi", "Huawei P60",
        
        # Tablets
        "iPad Air", "iPad Pro", "iPad Mini", "Samsung Galaxy Tab", "Microsoft Surface Pro", 
        "Microsoft Surface Go", "Amazon Fire HD", "Lenovo Tab", "Huawei MatePad",
        
        # Laptops
        "MacBook Air", "MacBook Pro", "Dell XPS", "Dell Inspiron", "HP Pavilion", "HP Spectre", 
        "Lenovo ThinkPad", "Lenovo IdeaPad", "Asus ZenBook", "Asus ROG", "Acer Aspire", "MSI Gaming Laptop",
        "Surface Laptop", "Chromebook", "Gaming Laptop",
        
        # TVs
        "Samsung QLED TV", "LG OLED TV", "Sony Bravia TV", "TCL 4K TV", "Hisense Smart TV", 
        "Roku Smart TV", "Vizio 4K TV", "Apple TV", "Fire TV Stick", "Chromecast",
        
        # Gaming
        "PlayStation 5", "Xbox Series X", "Xbox Series S", "Nintendo Switch OLED", "Steam Deck", 
        "Gaming Desktop", "VR Headset", "Meta Quest", "PlayStation VR", "Nintendo Pro Controller",
        
        # Audio
        "AirPods Pro", "AirPods Max", "Sony WH-1000XM5", "Bose QuietComfort", "JBL Flip", 
        "Beats Studio", "Marshall Speaker", "HomePod", "Amazon Echo Dot", "Google Nest Hub",
        
        # Cameras
        "Canon EOS R6", "Nikon Z7", "Sony Alpha", "Fujifilm X-T5", "GoPro Hero 11", 
        "DJI Mini Drone", "Canon PowerShot", "Polaroid Camera", "Ring Doorbell",
        
        # Wearables
        "Apple Watch Series 8", "Samsung Galaxy Watch", "Fitbit Versa", "Garmin Forerunner", 
        "Fitbit Charge", "Amazfit GTR", "Fossil Smartwatch",
        
        # Accessories
        "MagSafe Charger", "Wireless Mouse", "Gaming Mouse", "Mechanical Keyboard", "Webcam", 
        "USB-C Cable", "Lightning Cable", "HDMI Cable", "Power Bank", "Phone Case",
        "Screen Protector", "Car Charger", "Bluetooth Adapter",
        
        # Computing
        "Gaming Monitor", "4K Monitor", "Ultrawide Monitor", "MacBook Charger", "Laptop Stand", 
        "External SSD", "WiFi Router", "Mesh Router", "USB Hub", "Docking Station"
    ]
    
    # Clothing & Fashion
    clothing_base = [
        # Footwear
        "Nike Air Max", "Adidas Ultraboost", "Puma RS-X", "Converse Chuck Taylor", "Vans Old Skool", 
        "New Balance 990", "Jordan Retro", "Yeezy Boost", "Doc Martens", "Timberland Boots",
        "Running Shoes", "Cross Training Shoes", "Basketball Shoes", "Hiking Boots", "Dress Shoes",
        "High Heels", "Ballet Flats", "Sandals", "Flip Flops", "Loafers",
        
        # Tops
        "Graphic T-Shirt", "Basic T-Shirt", "Polo Shirt", "Button Down Shirt", "Henley Shirt",
        "Tank Top", "Crop Top", "Long Sleeve Shirt", "Hoodie", "Zip Up Hoodie", "Pullover",
        "Sweatshirt", "Cardigan", "Blazer", "Sweater", "Turtleneck",
        
        # Bottoms
        "Skinny Jeans", "Straight Jeans", "Bootcut Jeans", "High Waisted Jeans", "Ripped Jeans",
        "Chinos", "Dress Pants", "Cargo Pants", "Sweatpants", "Joggers", "Shorts",
        "Leggings", "Yoga Pants", "Athletic Shorts", "Board Shorts",
        
        # Dresses & Outerwear
        "Maxi Dress", "Mini Dress", "Cocktail Dress", "Sundress", "Wrap Dress", "Shirt Dress",
        "Winter Coat", "Puffer Jacket", "Leather Jacket", "Denim Jacket", "Bomber Jacket",
        "Trench Coat", "Peacoat", "Raincoat", "Windbreaker",
        
        # Accessories
        "Baseball Cap", "Snapback", "Beanie", "Bucket Hat", "Sunglasses", "Reading Glasses",
        "Leather Belt", "Canvas Belt", "Designer Belt", "Silk Scarf", "Winter Scarf",
        "Leather Gloves", "Winter Gloves", "Crossbody Bag", "Tote Bag", "Clutch",
        "Backpack", "Duffel Bag", "Fanny Pack", "Wallet", "Card Holder"
    ]
    
    # Home & Garden
    home_base = [
        "Coffee Maker", "Blender", "Toaster", "Microwave", "Air Fryer", "Instant Pot", "Food Processor",
        "Vacuum Cleaner", "Robot Vacuum", "Steam Mop", "Pressure Washer", "Carpet Cleaner",
        "Mattress", "Pillow", "Bed Sheets", "Comforter", "Blanket", "Curtains", "Rug",
        "Sofa", "Armchair", "Dining Table", "Coffee Table", "Bookshelf", "Desk", "Office Chair",
        "Floor Lamp", "Table Lamp", "Ceiling Fan", "Smart Light Bulb", "LED Strip Lights",
        "Air Purifier", "Humidifier", "Space Heater", "Fan", "Thermostat", "Smoke Detector",
        "Cookware Set", "Knife Set", "Cutting Board", "Mixing Bowls", "Baking Sheet", "Slow Cooker",
        "Garden Hose", "Lawn Mower", "Trimmer", "Fertilizer", "Plant Pot", "Garden Tools"
    ]
    
    # Sports & Fitness
    sports_base = [
        "Treadmill", "Exercise Bike", "Elliptical", "Rowing Machine", "Weight Set", "Dumbbells",
        "Yoga Mat", "Resistance Bands", "Foam Roller", "Exercise Ball", "Pull-up Bar",
        "Basketball", "Football", "Soccer Ball", "Tennis Racket", "Golf Clubs", "Baseball Bat",
        "Swimming Goggles", "Swimsuit", "Wetsuit", "Life Jacket", "Pool Float", "Diving Mask",
        "Hiking Boots", "Camping Tent", "Sleeping Bag", "Backpack", "Water Bottle", "Thermos",
        "Bicycle", "Mountain Bike", "Road Bike", "Electric Bike", "Bike Helmet", "Bike Lock",
        "Skateboard", "Roller Skates", "Scooter", "Hoverboard", "Snowboard", "Ski Equipment"
    ]
    
    # Books & Media
    books_base = [
        "Fiction Book", "Non-Fiction Book", "Biography", "Cookbook", "Travel Guide", "Textbook",
        "Children's Book", "Comic Book", "Graphic Novel", "Magazine", "Newspaper", "Journal",
        "DVD Movie", "Blu-ray Movie", "Video Game", "Board Game", "Puzzle", "Playing Cards",
        "Music CD", "Vinyl Record", "Bluetooth Headphones", "Portable Speaker", "Music Player"
    ]
    
    # Health & Beauty
    health_base = [
        "Shampoo", "Conditioner", "Body Wash", "Soap", "Lotion", "Moisturizer", "Sunscreen",
        "Toothbrush", "Toothpaste", "Mouthwash", "Dental Floss", "Electric Toothbrush",
        "Makeup", "Foundation", "Lipstick", "Mascara", "Eyeshadow", "Concealer", "Blush",
        "Perfume", "Cologne", "Deodorant", "Hair Dryer", "Straightener", "Curling Iron",
        "Vitamins", "Supplements", "Protein Powder", "Face Mask", "Skincare Set", "Nail Polish"
    ]
    
    # Automotive
    auto_base = [
        "Car Tires", "Motor Oil", "Car Battery", "Air Filter", "Brake Pads", "Spark Plugs",
        "Car Charger", "Phone Mount", "Dash Cam", "GPS Navigator", "Car Vacuum", "Car Cover",
        "Floor Mats", "Seat Covers", "Steering Wheel Cover", "Car Freshener", "Jumper Cables",
        "Tool Kit", "Emergency Kit", "First Aid Kit", "Car Jack", "Tire Pressure Gauge"
    ]
    
    # Toys & Games
    toys_base = [
        "LEGO Set", "Action Figure", "Doll", "Stuffed Animal", "Toy Car", "Remote Control Car",
        "Building Blocks", "Puzzle", "Board Game", "Card Game", "Video Game", "Gaming Console",
        "Art Supplies", "Coloring Book", "Craft Kit", "Science Kit", "Musical Instrument",
        "Bicycle", "Scooter", "Skateboard", "Basketball Hoop", "Soccer Goal", "Trampoline"
    ]
    
    # Kitchen & Dining
    kitchen_base = [
        "Refrigerator", "Dishwasher", "Oven", "Stove", "Range Hood", "Garbage Disposal",
        "Plates", "Bowls", "Cups", "Mugs", "Wine Glasses", "Silverware", "Serving Tray",
        "Pots and Pans", "Baking Dishes", "Measuring Cups", "Kitchen Scale", "Can Opener",
        "Stand Mixer", "Hand Mixer", "Juicer", "Espresso Machine", "Tea Kettle", "Rice Cooker"
    ]
    
    # Office Supplies
    office_base = [
        "Printer", "Scanner", "Shredder", "Laminator", "Calculator", "Stapler", "Hole Punch",
        "Notebooks", "Pens", "Pencils", "Markers", "Highlighters", "Sticky Notes", "Binders",
        "File Folders", "Paper", "Envelopes", "Labels", "Tape", "Scissors", "Ruler",
        "Desk Organizer", "Paper Clips", "Rubber Bands", "Push Pins", "Whiteboard", "Cork Board"
    ]
    
    # Pet Supplies
    pet_base = [
        "Dog Food", "Cat Food", "Pet Treats", "Dog Toy", "Cat Toy", "Litter Box", "Cat Litter",
        "Dog Leash", "Dog Collar", "Pet Carrier", "Pet Bed", "Scratching Post", "Fish Tank",
        "Pet Shampoo", "Dog Brush", "Cat Brush", "Pet Gate", "Dog Crate", "Bird Cage",
        "Hamster Cage", "Pet Food Bowl", "Water Dispenser", "Aquarium Filter", "Fish Food"
    ]
    
    # Jewelry & Accessories
    jewelry_base = [
        "Diamond Ring", "Gold Necklace", "Silver Bracelet", "Pearl Earrings", "Wedding Ring",
        "Engagement Ring", "Charm Bracelet", "Tennis Bracelet", "Pendant Necklace", "Stud Earrings",
        "Hoop Earrings", "Cufflinks", "Tie Clip", "Brooch", "Anklet", "Chain Necklace",
        "Vintage Watch", "Smart Watch", "Pocket Watch", "Jewelry Box", "Ring Holder"
    ]
    
    # Baby & Kids
    baby_base = [
        "Baby Stroller", "Car Seat", "High Chair", "Baby Monitor", "Diaper Bag", "Baby Bottles",
        "Pacifier", "Baby Formula", "Diapers", "Baby Wipes", "Baby Clothes", "Onesie",
        "Baby Blanket", "Crib", "Baby Mobile", "Teething Toy", "Baby Swing", "Bouncer",
        "Nursing Pillow", "Baby Bath", "Baby Thermometer", "Baby Gates", "Playpen", "Walker"
    ]
    
    # Grocery & Food
    grocery_base = [
        "Organic Apples", "Bananas", "Fresh Bread", "Milk", "Eggs", "Cheese", "Yogurt",
        "Chicken Breast", "Ground Beef", "Salmon Fillet", "Pasta", "Rice", "Cereal",
        "Peanut Butter", "Jam", "Honey", "Olive Oil", "Spices", "Tea", "Coffee Beans",
        "Frozen Pizza", "Ice Cream", "Chocolate", "Cookies", "Crackers", "Nuts", "Dried Fruit"
    ]
    
    # Garden & Outdoor
    garden_base = [
        "Garden Seeds", "Flower Seeds", "Fertilizer", "Mulch", "Garden Soil", "Plant Food",
        "Watering Can", "Garden Sprinkler", "Pruning Shears", "Garden Gloves", "Wheelbarrow",
        "Patio Furniture", "Outdoor Umbrella", "BBQ Grill", "Fire Pit", "Outdoor Heater",
        "Solar Lights", "Garden Statue", "Bird Feeder", "Wind Chimes", "Greenhouse", "Compost Bin"
    ]
    
    # Musical Instruments
    music_base = [
        "Guitar", "Electric Guitar", "Bass Guitar", "Drums", "Piano", "Keyboard",
        "Violin", "Cello", "Flute", "Trumpet", "Saxophone", "Clarinet", "Harmonica",
        "Ukulele", "Banjo", "Microphone", "Guitar Strings", "Piano Bench", "Music Stand",
        "Guitar Pick", "Drumsticks", "Guitar Case", "Piano Cover", "Metronome", "Tuner"
    ]
    
    # Craft & Hobby
    craft_base = [
        "Acrylic Paint", "Paint Brushes", "Canvas", "Colored Pencils", "Markers", "Sketchbook",
        "Clay", "Pottery Wheel", "Knitting Needles", "Yarn", "Fabric", "Sewing Machine",
        "Thread", "Scissors", "Glue Gun", "Beads", "Jewelry Wire", "Scrapbook", "Stickers",
        "Stamps", "Ink Pad", "Embroidery Hoop", "Cross Stitch Kit", "Model Kit", "Puzzle"
    ]
    
    # Tools & Hardware
    tools_base = [
        "Hammer", "Screwdriver Set", "Drill", "Saw", "Wrench Set", "Pliers", "Level",
        "Measuring Tape", "Socket Set", "Allen Keys", "Toolbox", "Work Bench", "Clamps",
        "Sandpaper", "Nails", "Screws", "Bolts", "Paint Roller", "Paint Brush", "Ladder",
        "Safety Goggles", "Work Gloves", "Extension Cord", "Flashlight", "Multi-tool"
    ]
    
    # Travel & Luggage
    travel_base = [
        "Suitcase", "Carry-on Bag", "Travel Backpack", "Duffel Bag", "Garment Bag",
        "Travel Pillow", "Eye Mask", "Passport Holder", "Luggage Tags", "Packing Cubes",
        "Travel Adapter", "Portable Charger", "Travel Mug", "Water Bottle", "Travel Guide",
        "Camera Bag", "Money Belt", "Travel Wallet", "Compression Socks", "Neck Pillow"
    ]
    
    # Create a comprehensive product catalog with category mappings
    all_products = []
    
    # Add all products with their proper categories
    for product in electronics_base:
        all_products.append((product, "Electronics"))
    for product in clothing_base:
        all_products.append((product, "Clothing"))
    for product in home_base:
        all_products.append((product, "Home & Garden"))
    for product in sports_base:
        all_products.append((product, "Sports"))
    for product in books_base:
        all_products.append((product, "Books"))
    for product in health_base:
        all_products.append((product, "Health & Beauty"))
    for product in auto_base:
        all_products.append((product, "Automotive"))
    for product in toys_base:
        all_products.append((product, "Toys & Games"))
    for product in kitchen_base:
        all_products.append((product, "Kitchen"))
    for product in office_base:
        all_products.append((product, "Office"))
    for product in pet_base:
        all_products.append((product, "Pet Supplies"))
    for product in jewelry_base:
        all_products.append((product, "Jewelry & Accessories"))
    for product in baby_base:
        all_products.append((product, "Baby & Kids"))
    for product in grocery_base:
        all_products.append((product, "Grocery & Food"))
    for product in garden_base:
        all_products.append((product, "Garden & Outdoor"))
    for product in music_base:
        all_products.append((product, "Musical Instruments"))
    for product in craft_base:
        all_products.append((product, "Craft & Hobby"))
    for product in tools_base:
        all_products.append((product, "Tools & Hardware"))
    for product in travel_base:
        all_products.append((product, "Travel & Luggage"))
    
    # Generate variations for each product
    variations_dict = {}
    
    # Common variation patterns
    def create_variations(product_name, variation_probability=0.85):
        variations = [product_name]
        
        # Only generate variations some fraction of the time
        if random.random() > variation_probability:
            return variations
        
        # Case variations (50% chance)
        if random.random() < 0.5:
            variations.extend([
                product_name.lower(),
                product_name.upper(),
                product_name.title()
            ])
        
        # Spacing variations (60% chance)
        if ' ' in product_name and random.random() < 0.6:
            variations.append(product_name.replace(' ', ''))
            variations.append(product_name.replace(' ', '-'))
            variations.append(product_name.replace(' ', '_'))
        
        # Common misspellings and typos using the typo generator (40% chance)
        if random.random() < 0.4:
            for _ in range(2):  # Generate 2 different typo variations
                typo_version = generate_typo(product_name, typo_probability=0.15)
                if typo_version != product_name:  # Only add if it's actually different
                    variations.append(typo_version)
        
        # Add colors (70% chance)
        if random.random() < 0.7:
            colors = ['Black', 'White', 'Blue', 'Red', 'Silver', 'Gold', 'Gray', 'Green', 'Pink', 'Purple']
            selected_colors = random.sample(colors, min(3, len(colors)))  # Pick 3 random colors
            for color in selected_colors:
                variations.append(f"{color} {product_name}")
                variations.append(f"{product_name} {color}")
        
        # Add sizes for clothing and applicable items (80% chance)
        if (any(item in product_name.lower() for item in ['shirt', 'pants', 'shoes', 'jacket', 'dress', 'jeans', 'hoodie', 'sweater']) and
            random.random() < 0.8):
            sizes = ['XS', 'Small', 'Medium', 'Large', 'XL', 'XXL', 'S', 'M', 'L']
            selected_sizes = random.sample(sizes, min(4, len(sizes)))  # Pick 4 random sizes
            for size in selected_sizes:
                variations.append(f"{product_name} {size}")
        
        # Add capacity/storage for tech products (60% chance)
        if (any(item in product_name.lower() for item in ['iphone', 'ipad', 'macbook', 'laptop', 'ssd', 'hard drive']) and
            random.random() < 0.6):
            capacities = ['64GB', '128GB', '256GB', '512GB', '1TB', '2TB']
            selected_capacities = random.sample(capacities, min(2, len(capacities)))
            for capacity in selected_capacities:
                variations.append(f"{product_name} {capacity}")
        
        # Add brands and prefixes (40% chance)
        if random.random() < 0.4:
            if any(term in product_name.lower() for term in ['tv', 'monitor']):
                variations.extend([f"Smart {product_name}", f"4K {product_name}", f"LED {product_name}"])
            elif 'speaker' in product_name.lower():
                variations.extend([f"Wireless {product_name}", f"Portable {product_name}"])
            elif 'mouse' in product_name.lower():
                variations.extend([f"Gaming {product_name}", f"Wireless {product_name}"])
            elif any(term in product_name.lower() for term in ['headphones', 'earbuds']):
                variations.extend([f"Wireless {product_name}", f"Noise Cancelling {product_name}"])
        
        return list(set(variations))  # Remove duplicates
    
    # Generate variations for all products  
    for product_name, category in all_products:
        variations_dict[product_name] = {
            "variations": create_variations(product_name),
            "category": category
        }
    
    
    return variations_dict

if __name__ == "__main__":
    variations = generate_product_variations()
    total_variations = sum(len(v) for v in variations.values())
    print(f"Generated {len(variations)} base products with {total_variations} total variations")