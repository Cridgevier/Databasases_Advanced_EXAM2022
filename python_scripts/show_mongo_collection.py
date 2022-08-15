from pymongo import MongoClient
client = MongoClient("mongodb://127.0.0.1:27017")
hashes_db = client["hashesdb"]
hashes_col = hashes_db["hashescol"]
for x in hashes_col.find():
    print(x)
