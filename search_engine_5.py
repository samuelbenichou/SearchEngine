from search_engine_interface import search_engine_interface
from PyDictionary import PyDictionary
from searcher import Searcher
import configuration
import os


class SearchEngine(search_engine_interface):
    ##############################################
    ###########        Thesaurus       ###########
    ##############################################

    def __init__(self, config=None):
        super(SearchEngine, self).__init__(config)


    def search(self, query):
        query_as_list = self._parser.parse_sentence(query)
        query_expansion = self.query_expansion(query_as_list)
        query_as_list.extend(query_expansion)
        searcher = Searcher(self._parser, self._indexer, model=self._model)
        n_relevant, ranked_doc_ids = searcher.search(query_as_list, 5)
        print(ranked_doc_ids)
        return n_relevant, ranked_doc_ids


    def query_expansion(self, query_as_list):
        result = []
        dictionary = PyDictionary()
        for term in query_as_list:
            synonyms = dictionary.synonym(term)
            if synonyms is not None:
                for synonym in synonyms[:2]:
                    result.append(synonym)
        return result


if __name__ == '__main__':
    config = configuration.ConfigClass()
    s = SearchEngine(config)
    s.build_index_from_parquet(os.path.join('data', 'benchmark_data_train.snappy.parquet'))
    n_res, res = s.search("Coronavirus is less dangerous than the flu")