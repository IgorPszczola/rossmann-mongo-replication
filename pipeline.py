from pymongo import MongoClient

client = MongoClient("mongodb://rossmann_mongo:27017/")
db = client["rossmann_db"]
receipts_collection = db["receipts"]

print("Uruchamiam nasłuchiwanie (Change Stream) na kolekcji 'receipts'...")
print("Czekam na nowe paragony. Naciśnij Ctrl+C, aby zakończyć.\n")

try:
    with receipts_collection.watch() as stream:
        for change in stream:
            operacja = change["operationType"]
            
            if operacja == "insert":
                pelny_dokument = change["fullDocument"]
                imie_klienta = pelny_dokument["customer"]["first_name"]
                kwota = pelny_dokument["payment"]["amount_paid"]
                
                print(f"ZŁAPANO NOWY PARAGON! Operacja: {operacja}")
                print(f"Klient: {imie_klienta}, Kwota: {kwota} PLN")
                print("-" * 40)
                
                
except KeyboardInterrupt:
    print("\nZakończono nasłuchiwanie.")