from pyserini.search.lucene import LuceneSearcher
import json
import os

from torch import topk, true_divide


def write_to_file(file_name, data):

    file = open(file_name, 'a')

    for entry in data:
        file.write(entry)

    file.close()


def search_contents_only(topic_number, hits):
    top1k = hits[:1000]
    results = []
    for i in range(len(top1k)):
        ret = str(topic_number) + " " + "q0" + " " + str(top1k[i].docid) + " " + str(i + 1) + " " + str(top1k[i].score) + " " +"y2238che" + "\n"
        results.append(ret)
    
    write_to_file("./pure_search_multi_fields.txt", results)


def check_gender(topic, raw_doc):
    
    if (topic["gender"] == 1 and raw_doc["eligibility"]["gender"] != -1):
        return True
    elif (topic["gender"] == -1 and raw_doc["eligibility"]["gender"] != 1):
        return True
    elif (topic["gender"] == 0):
        return True
    else:
        return False

def check_min_age(topic_age, criteria_min_age):
    if (criteria_min_age != "N/A"):
        if (topic_age >= criteria_min_age):
            return True
        else:
            return False
    else:
        return True

def check_max_age(topic_age, criteria_max_age):
    if (criteria_max_age != "N/A"):
        if (topic_age <= criteria_max_age):
            return True
        else:
            return False
    else:
        return True

def check_age(topic, raw_doc):
    if (check_min_age(topic["age"], raw_doc["eligibility"]["min_age"]) and check_max_age(topic["age"], raw_doc["eligibility"]["max_age"])):
        return True
    else:
        return False

def check_health(topic, raw_doc):

    if (topic["healthy"] == raw_doc["eligibility"]["healthy"]):
        return True
    else:
        return False

def search_by_meta_criteria(topic, hits, searcher):
    
    filter_hits = []
    for hit in hits:
        raw_doc = json.loads(searcher.doc(hit.docid).raw())
        if (bool(raw_doc["eligibility"])):
            if (check_gender(topic, raw_doc) and check_age(topic, raw_doc) and check_health(topic, raw_doc)):
                filter_hits.append(hit)
        else:
            filter_hits.append(hit)
    
    top1k = filter_hits[:1000]

    if (len(filter_hits) < 1000):
        print(topic["num"])
    
    results = []

    for i in range(len(top1k)):
        ret = str(topic["num"]) + " " + "q0" + " " + str(top1k[i].docid) + " " + str(i + 1) + " " + str(top1k[i].score) + " " +"y2238che" + "\n"
        results.append(ret)
    
    write_to_file("./meta_criteria_search_multi_fields_equal.txt", results)

def main():
    sparse_searcher = LuceneSearcher('indexes/sparse_multi_fields')
    topic_file = open('topic.json')
    topics = json.load(topic_file)

    for topic in topics:
        topic_number = topic["num"]
        query = topic["query"]
        hits = sparse_searcher.search(query, k = 30000, fields={"contents": 1, "inclusion": 1, "exlcusion":1})

        # search_contents_only(topic_number, hits)

        search_by_meta_criteria(topic, hits, sparse_searcher)

        # search_by_all_criteria(topic, hits, sparse_searcher)


    topic_file.close()

def clear_files():
    if os.path.exists("./pure_search.txt"):
        os.remove("./pure_search.txt")
    
    if os.path.exists("./meta_criteria_search.txt"):
        os.remove("./meta_criteria_search.txt")
    

if __name__ == "__main__":

    # clear_files()
    main()