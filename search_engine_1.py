from search_engine_interface import search_engine_interface
from gensim.scripts.glove2word2vec import glove2word2vec
from gensim.models import KeyedVectors
from searcher import Searcher
from configuration import ConfigClass
import utils


class search_engine_1(search_engine_interface):
    ##############################################
    ###########          GloVe         ###########
    ##############################################

    def __init__(self):
        super().__init__()
        self.glove_input_file = 'glove.twitter.27B.25d.txt'
        self.word2vec_output_file = 'glove.twitter.27B.25d.txt.word2vec'
        self.local_cache = {}


    def search(self, query):
        query_as_list = self.parser.parse_sentence(query)
        query_expansion = self.query_expansion(query_as_list)
        self.add_similar_word_to_query(query_as_list, query_expansion)
        searcher = Searcher(self.parser, self.indexer, model=self.model)
        print(query_as_list)
        n_relevant, ranked_doc_ids = searcher.search(query_as_list,5)
        print(ranked_doc_ids)


    def add_similar_word_to_query(self, query_as_list, query_expansion):
        query_as_list.extend([word[0] for word in query_expansion])


    def query_expansion(self, query_as_list):
        glove2word2vec(self.glove_input_file, self.word2vec_output_file)
        model = KeyedVectors.load_word2vec_format(self.word2vec_output_file, binary=False)
        result = []
        for term in query_as_list:
            if term in self.local_cache.keys():
                result.extend(self.local_cache[term])
            else:
                try:
                    if term[0] == "@" or term[0] == "#":
                        continue
                    else:
                        result.extend(
                            [model.most_similar(term)[0], model.most_similar(term)[1], model.most_similar(term)[2]])
                        self.local_cache[term] = [model.most_similar(term)[0], model.most_similar(term)[1], model.most_similar(term)[2]]
                except:
                    pass
        return result


    def get_filename(self):
        filename = '../../../../glove.twitter.27B.25d.txt'

if __name__ == '__main__':
    s = search_engine_1()
    s.build_index_from_parquet("/Users/samuel/Desktop/Corpus/test")
    #s.search("Coronavirus is less dangerous than the flu")
    #'bioweapon'
