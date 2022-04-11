'''
Modified from https://github.com/castorini/pyserini/blob/master/pyserini/search/hybrid/_searcher.py
'''

from typing import List, Dict
from pyserini.search.lucene import LuceneSearcher
from pyserini.search.faiss import FaissSearcher, DenseSearchResult, TctColBertQueryEncoder
import json

class HybridSearcher:
    """Hybrid Searcher for dense + sparse
        Parameters
        ----------
        dense_searcher : FaissSearcher
        sparse_searcher : LuceneSearcher
    """

    def __init__(self, dense_searcher, sparse_searcher):
        self.dense_searcher = dense_searcher
        self.sparse_searcher = sparse_searcher
    
    def __check_gender__(self, topic, raw_doc):
    
        if (topic["gender"] == 1 and raw_doc["eligibility"]["gender"] != -1):
            return True
        elif (topic["gender"] == -1 and raw_doc["eligibility"]["gender"] != 1):
            return True
        elif (topic["gender"] == 0):
            return True
        else:
            return False

    def __check_min_age__(self, topic_age, criteria_min_age):
        if (criteria_min_age != "N/A"):
            if (topic_age >= criteria_min_age):
                return True
            else:
                return False
        else:
            return True

    def __check_max_age__(self, topic_age, criteria_max_age):
        if (criteria_max_age != "N/A"):
            if (topic_age <= criteria_max_age):
                return True
            else:
                return False
        else:
            return True

    def __check_age__(self, topic, raw_doc):
        if (self.__check_min_age__(topic["age"], raw_doc["eligibility"]["min_age"]) and self.__check_max_age__(topic["age"], raw_doc["eligibility"]["max_age"])):
            return True
        else:
            return False

    def __check_health__(self, topic, raw_doc):

        if (topic["healthy"] == raw_doc["eligibility"]["healthy"]):
            return True
        else:
            return False
    
    def __meta_filter(self, hits, topic):
        filter_hits = []
        for hit in hits:
            raw_doc = json.loads(self.sparse_searcher.doc(hit.docid).raw())
            if (bool(raw_doc["eligibility"])):
                if (self.__check_gender__(topic, raw_doc) and self.__check_age__(topic, raw_doc) and self.__check_health__(topic, raw_doc)):
                    filter_hits.append(hit)
            else:
                filter_hits.append(hit)
        return filter_hits

    def search(self, query: str, k0: int = 10, k: int = 10, alpha: float = 0.1, normalization: bool = False, weight_on_dense: bool = False, fields: dict = {}, topic: dict = {}, threads: int = 1) -> List[DenseSearchResult]:
        dense_hits = self.dense_searcher.search(query, k0, threads = threads)
        sparse_hits = self.sparse_searcher.search(query, k0 * 3, fields = fields)

        sparse_hits = self.__meta_filter(sparse_hits, topic)[:k0]

        return self._hybrid_results(dense_hits, sparse_hits, alpha, k, normalization, weight_on_dense)

    def batch_search(self, queries: List[str], q_ids: List[str], k0: int = 10, k: int = 10, threads: int = 1,
            alpha: float = 0.1, normalization: bool = False, weight_on_dense: bool = False) \
            -> Dict[str, List[DenseSearchResult]]:
        dense_result = self.dense_searcher.batch_search(queries, q_ids, k0, threads)
        sparse_result = self.sparse_searcher.batch_search(queries, q_ids, k0, threads)
        hybrid_result = {
            key: self._hybrid_results(dense_result[key], sparse_result[key], alpha, k, normalization, weight_on_dense)
            for key in dense_result
        }
        return hybrid_result

    @staticmethod
    def _hybrid_results(dense_results, sparse_results, alpha, k, normalization=False, weight_on_dense=False):
        dense_hits = {hit.docid: hit.score for hit in dense_results}
        sparse_hits = {hit.docid: hit.score for hit in sparse_results}
        hybrid_result = []
        min_dense_score = min(dense_hits.values()) if len(dense_hits) > 0 else 0
        max_dense_score = max(dense_hits.values()) if len(dense_hits) > 0 else 1
        min_sparse_score = min(sparse_hits.values()) if len(sparse_hits) > 0 else 0
        max_sparse_score = max(sparse_hits.values()) if len(sparse_hits) > 0 else 1
        for doc in set(dense_hits.keys()) | set(sparse_hits.keys()):
            if doc not in dense_hits:
                sparse_score = sparse_hits[doc]
                dense_score = min_dense_score
            elif doc not in sparse_hits:
                sparse_score = min_sparse_score
                dense_score = dense_hits[doc]
            else:
                sparse_score = sparse_hits[doc]
                dense_score = dense_hits[doc]
            if normalization:
                sparse_score = (sparse_score - (min_sparse_score + max_sparse_score) / 2) \
                               / (max_sparse_score - min_sparse_score)
                dense_score = (dense_score - (min_dense_score + max_dense_score) / 2) \
                              / (max_dense_score - min_dense_score)
            score = alpha * sparse_score + (1.0 - alpha) * dense_score if not weight_on_dense else sparse_score + alpha * dense_score
            hybrid_result.append(DenseSearchResult(doc, score))
        return sorted(hybrid_result, key=lambda x: x.score, reverse=True)[:k]

def write_to_file(file_name, data):

    file = open(file_name, 'a')

    for entry in data:
        file.write(entry)

    file.close()

def main():
    sparse_searcher = LuceneSearcher('indexes/sparse_multi_fields')
    encoder = TctColBertQueryEncoder('castorini/tct_colbert-v2-hnp-msmarco')
    dense_searcher = FaissSearcher(
        'indexes/dense/',
        encoder
    )

    topic_file = open('topic.json')
    topics = json.load(topic_file)

    hybrid_searcher = HybridSearcher(dense_searcher, sparse_searcher)

    for topic in topics:
        topic_number = topic["num"]
        query = topic["query"]
        hits = hybrid_searcher.search(query, k0=30000, k=1000, alpha=0.7, normalization=True, fields={"contents": 0.2, "inclusion": 0.4, "exlcusion": 0.4}, topic=topic, threads=16)

        results = []
        for i in range(len(hits)):
            ret = str(topic_number) + " " + "q0" + " " + str(hits[i].docid) + " " + str(i + 1) + " " + str(hits[i].score) + " " +"y2238che" + "\n"
            results.append(ret)
    
        write_to_file("./hybrid_search_multi_fields_7.txt", results)
    
    topic_file.close()

if __name__ == "__main__":
    main()