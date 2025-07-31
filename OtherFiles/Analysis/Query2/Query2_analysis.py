import pprint
import warnings
from pymongo import MongoClient
from bson.json_util import dumps
import matplotlib.pyplot as plt

# Variables setup
warnings.filterwarnings('ignore')
client = MongoClient('localhost', 27017)
db = client['SMBUDProject24']
logainm = db['logainm']

# Explain execution without indexes
query_stats = db.logainm.find(
    {
        "includes.id": "L",
        "glossary.headword": "loch"
    },
    {
        "_id": 0,
        "id": 1,
        "placenames.language": 1,
        "placenames.wording": 1,
        "categories.nameEN": 1
    }
).explain()['executionStats']

# Results are saved in the specified file
filename = "Query2_analysis_result_noIndex.json"
json_data = dumps(query_stats, indent=4, ensure_ascii=False)
with open(filename, "w") as outfile:
    outfile.write(json_data)

db.logainm.create_index("includes.id")
db.logainm.create_index("glossary.headword")


# Explain execution with the indexes
query_stats_Idx = db.logainm.find(
    {
        "includes.id": "L",
        "glossary.headword": "loch"
    },
    {
        "_id": 0,
        "id": 1,
        "placenames.language": 1,
        "placenames.wording": 1,
        "categories.nameEN": 1
    }
).explain()["executionStats"]

# Results are saved in the specified file
filename = "Query2_analysis_result_withIndex.json"
json_data = dumps(query_stats_Idx, indent=4, ensure_ascii=False)
with open(filename, "w") as outfile:
    outfile.write(json_data)

db.logainm.drop_index("includes.id_1")
db.logainm.drop_index("glossary.headword_1")

print("Execution time without indexes: " + str(query_stats["executionTimeMillis"]) + " ms")
print("Execution time with indexes: " + str(query_stats_Idx["executionTimeMillis"]) + " ms")
