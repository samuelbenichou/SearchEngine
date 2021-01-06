from search_engine_interface import search_engine_interface
from searcher import Searcher
from configuration import ConfigClass
import utils
import requests
import time

class SearchEngine(search_engine_interface):
    ##############################################
    ###########        Word2Vec        ###########
    ##############################################

    def __init__(self):
        super().__init__()
        self.url = "http://bionlp-www.utu.fi/wv_demo/nearest"
        self.data = {'form[1][name]': 'topn', 'form[0][name]': 'word', 'model_name': "English GoogleNews Negative300", }
        self.local_cache = {}

    def search(self, query):
        query_as_list = self._parser.parse_sentence(query)
        query_expansion = self.query_expansion(query_as_list)
        self.add_similar_word_to_query(query_as_list, query_expansion)
        searcher = Searcher(self._parser, self._indexer, model=self._model)
        n_relevant, ranked_doc_ids = searcher.search(query_as_list, 10)
        print(ranked_doc_ids)
        return n_relevant, ranked_doc_ids

    def add_similar_word_to_query(self, query_as_list, query_expansion):
        for word in query_expansion.keys():
            if query_expansion[word] is not None:
                query_as_list.extend(query_expansion[word])

    def query_expansion(self, query_as_list):
        ret_dict = {}
        for word in query_as_list:
            if word in self.local_cache:
                ret_dict[word] = self.local_cache[word]
            else:
                ret_dict[word] = self.get_top_n(word)

        return ret_dict

    def get_top_n(self, word):
        self.data['form[1][value]'] = str(3)
        self.data['form[0][value]'] = word

        if word in self.local_cache:
            return self.local_cache[word]

        # Post the request to the server
        r = requests.post(self.url, self.data)

        # Check request status
        if r.status_code != "200" and r.reason != "OK":
            return None

        # Get the text from the response
        response_text = r.text

        # Given word is not in the dictionary
        if "is not in the vocabulary" in response_text:
            return None

        response_text = response_text.replace(r'{"tbl": "<div class=\"w2vresultblock bg-info\">\n\n', "") \
            .replace(r"\n", "").replace("</div>", " ").replace("</br>", " ").replace('"}', "")
        response_text = response_text.split()
        self.local_cache[word] = response_text
        return response_text

if __name__ == "__main__":
    s = SearchEngine()
    s.build_index_from_parquet("/Users/samuel/Desktop/Corpus/test")
    s.search("Coronavirus is less dangerous than the flu")
    #print(s.parser.parse_sentence("Coronavirus is less dangerous than the flu"))
    #1284110077012541440
    #1284920404302274561
