
from pymongo import MongoClient
import pprint

MONGO_URI = "mongodb+srv://dbadmin:natureCounter%401998@nature-counter-server-c.n8xv09r.mongodb.net/NC_dev_db?appName=Nature-Counter-Server-Cluster-1"
DB_NAME = "NC_dev_db"
JOURNALS_COL = "journals"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[JOURNALS_COL]

print("Checking for 'n_duration' or 'duration' fields...")
doc_n_duration = col.find_one({"n_duration": {"$exists": True}})
doc_duration = col.find_one({"duration": {"$exists": True}})

if doc_n_duration:
    print("Found document with 'n_duration':")
    pprint.pprint(doc_n_duration)
else:
    print("No document found with 'n_duration' field.")

if doc_duration:
    print("Found document with 'duration':")
    pprint.pprint(doc_duration)
else:
    print("No document found with 'duration' field.")
