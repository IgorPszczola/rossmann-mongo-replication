from pymongo import MongoClient

import adapter_mongodb

active_adapters = [
    adapter_mongodb
]

client = MongoClient("mongodb://rossmann_mongo:27017/")
db = client["rossmann_db"]
receipts_collection = db["receipts"]

print("Uruchamiam nasłuchiwanie (Change Stream) na kolekcji 'receipts'...")
print("Czekam na nowe paragony. Naciśnij Ctrl+C, aby zakończyć.\n")

try:
    with receipts_collection.watch() as stream:
        for change in stream:
            operation = change["operationType"]
            
            if operation == "insert":
                full_document = change["fullDocument"]
                first_name = full_document["customer"]["first_name"]
                amount_paid = full_document["payment"]["amount_paid"]
                
                print(f"ZŁAPANO NOWY PARAGON! Operacja: {operation}")
                print(f"Klient: {first_name}, Kwota: {amount_paid} PLN")

                for adapter in active_adapters:
                    adapter.save_receipt(full_document)

                print("-" * 40)
                
                
except KeyboardInterrupt:
    print("\nZakończono nasłuchiwanie.")