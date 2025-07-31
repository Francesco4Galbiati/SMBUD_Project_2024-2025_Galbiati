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

# Query pipeline in dictionary form
streets_query_pipeline = [
    {
        "$match": {
            "$or": [
                {
                    "placenames.wording": {
                        "$regex": "Phúca"
                    }
                },
                {
                    "placenames.wording": {
                        "$regex": "Púca"
                    }
                }
            ]
        }
    },
    {
        "$unwind": "$includedIn"
    },
    {
        "$match": {
            "includedIn.category.id": "CON"
        }
    },
    {
        "$project": {
            "_id": 0,
            "id": 1,
            "placenames.language": 1,
            "placenames.wording": 1,
            "county": {
                "nameGA": "$includedIn.nameGA",
                "nameEN": "$includedIn.nameEN"
            }
        }
    },
    {
        "$sort": {
            "county.nameGA": 1
        }
    }
]

# Explain execution without indexes
query_stats = db.command('explain', {
    'aggregate': 'logainm',
    'pipeline': streets_query_pipeline,
    'cursor': {}
}, verbosity='executionStats')

# Results are saved in the specified file
filename = "Query3_analysis_result_noIndex.json"
json_data = dumps(query_stats, indent=4, ensure_ascii=False)
with open(filename, "w") as outfile:
    outfile.write(json_data)


# Create index on categories.id
db.logainm.create_index('placenames.wording')

# Explain execution with the indexes
query_stats_idx = db.command('explain', {
    'aggregate': 'logainm',
    'pipeline': streets_query_pipeline,
    'cursor': {}
}, verbosity='executionStats')

# Results are saved in the specified file
filename = "Query3_analysis_result_withIndex.json"
json_data = dumps(query_stats_idx, indent=4, ensure_ascii=False)
with open(filename, "w") as outfile:
    outfile.write(json_data)

db.logainm.drop_index('placenames.wording_1')


# Result plotting
queryTimeNoIdx = [0]
queryTimeIdx = [0]
queryDocumentNoIdx = []
queryDocumentIdx = []
stageNames = ['Start']
i = 1
for s in query_stats['stages']:
    queryTimeNoIdx.append(s['executionTimeMillisEstimate'])
    queryDocumentNoIdx.append(s['nReturned'])
    stageNames.append("Stage " + str(i) + " (" + list(s.keys())[0] + ")")
    i += 1

for s in query_stats_idx['stages']:
    queryTimeIdx.append(s['executionTimeMillisEstimate'])
    queryDocumentIdx.append(s['nReturned'])

# Plot of execution time
plt.clf()
plt.plot(stageNames, queryTimeNoIdx, label="No Index")
plt.plot(stageNames, queryTimeIdx, label="With Index")
plt.legend(loc='best')
plt.xlabel("Stages")
plt.ylabel("Execution Time (ms)")
plt.title("Query Execution Time")
plt.setp(plt.gca().get_xticklabels(), rotation=45, ha="right")
plt.tight_layout()
plt.savefig("Query3_analysis_time.png", dpi=300)
plt.clf()


# Plot of analyzed documents
stageNames.remove("Start")
plt.plot(stageNames, queryDocumentNoIdx)
plt.legend(loc="best")
plt.xlabel("Stages")
plt.ylabel("Documents Number")
plt.title("Number of analyzed documents per stage")
plt.setp(plt.gca().get_xticklabels(), rotation=45, ha="right")
plt.tight_layout()
plt.savefig("Query3_analysis_docs.png", dpi=300)
