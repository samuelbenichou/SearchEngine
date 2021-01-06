from search_engine_interface import search_engine_interface
from searcher import Searcher
from nltk.corpus import wordnet
import nltk
from configuration import ConfigClass
import utils


class SearchEngine(search_engine_interface):
    ##############################################
    ###########         WordNet        ###########
    ##############################################

    def __init__(self):
        super().__init__()
        self.local_cache = {}


    def search(self, query):
        query_as_list = self._parser.parse_sentence(query)
        query_expansion = self.query_expansion(query_as_list)
        self.add_similar_word_to_query(query_as_list, query_expansion)
        searcher = Searcher(self._parser, self._indexer, model=self._model)
        print(query_as_list)
        n_relevant, ranked_doc_ids = searcher.search(query_as_list,5)
        print(ranked_doc_ids)
        return n_relevant, ranked_doc_ids


    def add_similar_word_to_query(self, query_as_list, query_expansion):
        query_as_list.extend([word for word in query_expansion])


    def query_expansion(self, query_as_list):
        result = []
        for term in query_as_list:
            if not term[0].isupper():
                if term in self.local_cache:
                    result.extend(self.local_cache[term])
                else:
                    synonyms = []
                    for syn in wordnet.synsets(term):
                        for l in filter(lambda el: el.name() not in [term, term.lower()] + result, syn.lemmas()[:3]):
                            synonyms.append(l.name())
                    self.local_cache[term] = synonyms[:3]
                    result.extend(synonyms[:3])
        return result


if __name__ == '__main__':
    s = SearchEngine()
    s.build_index_from_parquet("/Users/samuel/Desktop/Corpus/test")
    s.search("Coronavirus is less dangerous than the flu")
    #'bioweapon'
    s.query_expansion(['dangerous','Trump'])
