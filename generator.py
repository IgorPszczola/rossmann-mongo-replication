from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime, timezone

fake = Faker('pl_PL')

client = MongoClient("mongodb://rossmann_mongo:27017/")
db = client["rossmann_db"]
receipts_collection = db["receipts"]

def generate_receipt():
    products = [
        {"id": "1001", "name": "Szampon Isana 400ml", "price": 8.99},
        {"id": "1002", "name": "Pasta Elmex", "price": 14.50},
        {"id": "1003", "name": "Żel pod prysznic Nivea", "price": 12.99},
        {"id": "1004", "name": "Krem Nivea", "price": 19.99}
    ]
    
    purchased_items = []
    total_amount = 0.0
    for _ in range(random.randint(1, 3)):
        prod = random.choice(products)
        qty = random.randint(1, 2)
        total_amount += prod["price"] * qty
        purchased_items.append({
            "product_id": prod["id"],
            "name": prod["name"],
            "quantity": qty,
            "unit_price": prod["price"]
        })

    return {
        "transaction_id": fake.uuid4(),
        "store_id": random.randint(100, 999),
        "timestamp": datetime.now(timezone.utc),
        "customer": {
            "first_name": fake.first_name(),
            "loyalty_card": random.choice([True, False])
        },
        "items": purchased_items,
        "payment": {
            "method": random.choice(["BLIK", "Karta", "Gotówka"]),
            "amount_paid": round(total_amount, 2)
        }
    }

print("Rozpoczynam generowanie paragonów...")
for i in range(10):
    receipt_data = generate_receipt()
    receipts_collection.insert_one(receipt_data)
    print(f"Dodano paragon nr {i+1} dla klienta: {receipt_data['customer']['first_name']}")

print("\nSukces! Dane są w MongoDB.")