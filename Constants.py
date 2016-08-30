from os.path import abspath, dirname, join


ES_HOST = dict(host="localhost", port=9200)

BASE_DIR = abspath(dirname(__file__))
DATA_PATH = join(BASE_DIR, "Data")
QUERY_DATA_PATH = join(DATA_PATH, "query_data")
AP_DATA_PATH = join(DATA_PATH, "AP_DATA")

QUERIES_FILE = join(DATA_PATH, "queries.txt")
RESULTS_FILE = join(DATA_PATH, "bm25_results.txt")
QREL_FILE = join(DATA_PATH, "qrels_AP89.txt")

AP_89_INDEX = "ap_dataset"
AP_89_DOC_TYPE = "document"

PART_A_OUTPUT_FILE = join(DATA_PATH, "part_a_output")
PART_B_OUTPUT_FILE = join(DATA_PATH, "part_b_output")
ALL_IDS_FILE = join(DATA_PATH, "all_ids")
COMPOSITIONS_FILE = join(DATA_PATH, "output_composition.txt")
KEYS_FILE = join(DATA_PATH, "output_keys.txt")
ARFF_FILE = join(DATA_PATH, "data.arff")
DOCS_FILE = join(DATA_PATH, "doc_ids")

CLUSTERING_OUTPUT_FILE = join(DATA_PATH, "clustering_output")
