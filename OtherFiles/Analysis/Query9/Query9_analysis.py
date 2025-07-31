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
gaeltacht_query_pipeline = [
    {
        "$match": {
            "categories.id": "BF",
            "gaeltacht.extent": "all"
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
        "$group": {
            "_id": {
                "nameEN": "$includedIn.nameEN",
                "nameGA": "$includedIn.nameGA"
            },
            "count": {
                "$sum": 1
            }
        }
    }
]

# Explain execution without indexes
query_stats_noIdx = db.command('explain', {
    'aggregate': 'logainm',
    'pipeline': gaeltacht_query_pipeline,
    'cursor': {}
}, verbosity='executionStats')

# Results are saved in the specified file
filename = "Query9_analysis_result_noIndex.json"
json_data = dumps(query_stats_noIdx, indent=4, ensure_ascii=False)
with open(filename, "w") as outfile:
    outfile.write(json_data)


# Create index on categories.id
db.logainm.create_index('categories.id')

# Explain execution with the category.id index
query_stats_CatIdx = db.command('explain', {
    'aggregate': 'logainm',
    'pipeline': gaeltacht_query_pipeline,
    'cursor': {}
}, verbosity='executionStats')

# Results are saved in the specified file
filename = "Query9_analysis_result_withCategoryIndex.json"
json_data = dumps(query_stats_CatIdx, indent=4, ensure_ascii=False)
with open(filename, "w") as outfile:
    outfile.write(json_data)

# Create index on gaeltacht.extent
db.logainm.create_index('gaeltacht.extent')

# Explain execution with both indexes
query_stats_AllIdx = db.command('explain', {
    'aggregate': 'logainm',
    'pipeline': gaeltacht_query_pipeline,
    'cursor': {}
}, verbosity='executionStats')

# Results are saved in the specified file
filename = "Query9_analysis_result_withAllIndexes.json"
json_data = dumps(query_stats_AllIdx, indent=4, ensure_ascii=False)
with open(filename, "w") as outfile:
    outfile.write(json_data)

# Drop index on categories.id
db.logainm.drop_index('categories.id_1')

# Explain execution with the gaeltacht.extent index
query_stats_GaeltachtIdx = db.command('explain', {
    'aggregate': 'logainm',
    'pipeline': gaeltacht_query_pipeline,
    'cursor': {}
}, verbosity='executionStats')

# Results are saved in the specified file
filename = "Query9_analysis_result_withGealtachtIndex.json"
json_data = dumps(query_stats_GaeltachtIdx, indent=4, ensure_ascii=False)
with open(filename, "w") as outfile:
    outfile.write(json_data)

# Drop index on gaeltacht.extent
db.logainm.drop_index('gaeltacht.extent_1')

# Result plotting
queryTimeNoIdx = [0]
queryTimeCatIdx = [0]
queryTimeAllIdx = [0]
queryTimeGaeltachtIdx = [0]
queryDocumentNoIdx = []
stageNames = ['Start']
i = 1
for s in query_stats_noIdx['stages']:
    queryTimeNoIdx.append(s['executionTimeMillisEstimate'])
    queryDocumentNoIdx.append(s['nReturned'])
    stageNames.append("Stage " + str(i) + " (" + list(s.keys())[0] + ")")
    i += 1

for s in query_stats_CatIdx['stages']:
    queryTimeCatIdx.append(s['executionTimeMillisEstimate'])

for s in query_stats_AllIdx['stages']:
    queryTimeAllIdx.append(s['executionTimeMillisEstimate'])

for s in query_stats_GaeltachtIdx['stages']:
    queryTimeGaeltachtIdx.append(s['executionTimeMillisEstimate'])

# Plot of execution time
plt.clf()
plt.plot(stageNames, queryTimeNoIdx, label="No index")
plt.plot(stageNames, queryTimeCatIdx, label="With categories.id index")
plt.plot(stageNames, queryTimeAllIdx, label="With both indexes")
plt.plot(stageNames, queryTimeGaeltachtIdx, label="With gaeltacht.extent index")
plt.legend(loc='best')
plt.xlabel("Stages")
plt.ylabel("Execution Time (ms)")
plt.title("Query Execution Time")
plt.setp(plt.gca().get_xticklabels(), rotation=45, ha="right")
plt.tight_layout()
plt.savefig("Query9_analysis_time.png", dpi=300)
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
plt.savefig("Query9_analysis_docs.png", dpi=300)
