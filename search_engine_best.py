import pandas as pd
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import concurrent.futures
import timeit
import traceback
import requests
import utils
import configuration
import os


# DO NOT CHANGE THE CLASS NAME
class SearchEngine:

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation, but you must have a parser and an indexer.
    def __init__(self, config=None):
        self._config = config
        self._parser = Parse()
        self._indexer = Indexer(config)
        self._model = None
        self.url = "http://bionlp-www.utu.fi/wv_demo/nearest"
        self.data = {'form[1][name]': 'topn', 'form[0][name]': 'word', 'model_name': "English GoogleNews Negative300", }
        self.local_cache = {}

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def build_index_from_parquet(self, fn):
        """
        Reads parquet file and passes it to the parser, then indexer.
        Input:
            fn - path to parquet file
        Output:
            No output, just modifies the internal _indexer object.
        """
        # Configure the path
        config = ConfigClass()
        config.corpusPath = fn

        # Get list of all parquets files
        corpus = []
        df = pd.read_parquet(fn, engine="pyarrow")
        value = df.values.tolist()
        corpus.extend(value)
        # for file in filenames:
        #     df = pd.read_parquet(file, engine="pyarrow")
        #     value = df.values.tolist()
        #     corpus.extend(value)

        one_third = len(corpus) // 3

        with concurrent.futures.ProcessPoolExecutor() as executor:
            f1 = executor.submit(self.process_index, corpus[:one_third], 1)
            f2 = executor.submit(self.process_index, corpus[one_third:2 * one_third], 2)
            f3 = executor.submit(self.process_index, corpus[2 * one_third:], 3)

        indexer1 = f1.result()
        indexer2 = f2.result()
        indexer3 = f3.result()

        self._indexer.inverted_idx = self.merge_inverted_index(indexer1.inverted_idx, indexer2.inverted_idx,
                                                               indexer3.inverted_idx)
        self.three_way_external_merge(self._indexer.inverted_idx, indexer1.postingDict, indexer2.postingDict,
                                      indexer3.postingDict)
        self.merge_indexer(indexer1, indexer2, indexer3)
        self._indexer.save_index('idx_bench')
        #'bench_idx'

    def process_index(self, documents_list, num_thread):
        config = ConfigClass()

        p = Parse()
        indexer = Indexer(config)
        indexer.set_thread(num_thread)
        print("Number of tweet in the process {} : {}".format(num_thread, len(documents_list)))
        number_of_documents = 0
        start = timeit.default_timer()
        try:
            # Iterate over every document in the file
            for idx, document in enumerate(documents_list):
                # parse the document
                parsed_document = p.parse_doc(document)
                number_of_documents += 1
                # index the document data
                indexer.add_new_doc(parsed_document)
        except:
            print("Problem with process {}".format(num_thread))
            print(traceback.print_exc())

        stop = timeit.default_timer()
        print("Time of indexer and posting of process {} : ".format(num_thread), stop - start)

        return indexer

    def merge_indexer(self, *indexers):
        for indexer in indexers:
            for doc_id, value in indexer.tweet_index.items():
                self._indexer.tweet_index[doc_id] = value

    def merge_inverted_index(self, *inverted_index_dicts):
        super_dict = {}
        for inverted_index_dict in inverted_index_dicts:
            for term, value in inverted_index_dict.items():
                if term in super_dict:
                    super_dict[term][0] += value[0]
                    super_dict[term][1] += value[1]
                else:
                    super_dict[term] = value

        return super_dict


    def three_way_external_merge(self, inverted_index, posting_file1, posting_file2, posting_file3):
        try:
            line = 1
            with open("posting_file.txt", 'w', encoding="cp437", errors='ignore') as file:
                for term in sorted(inverted_index.keys()):
                    posting_file = []
                    lower_term = term.lower() if term[0].isupper() else None
                    is_duplicate = False
                    if term in posting_file1:
                        posting_file.extend(posting_file1[term])
                    if term in posting_file2:
                        posting_file.extend(posting_file2[term])
                    if term in posting_file3:
                        posting_file.extend(posting_file3[term])
                    if lower_term is not None:
                        if lower_term in posting_file1 and not is_duplicate:
                            posting_file1[lower_term].extend(posting_file)
                            is_duplicate = True
                        if lower_term in posting_file2 and not is_duplicate:
                            posting_file2[lower_term].extend(posting_file)
                            is_duplicate = True
                        if lower_term in posting_file3 and not is_duplicate:
                            posting_file3[lower_term].extend(posting_file)
                            is_duplicate = True
                    if is_duplicate:
                        inverted_index[lower_term][0] += inverted_index[term][0]
                        inverted_index[lower_term][1] += inverted_index[term][1]
                        del inverted_index[term]
                    else:
                        file.write("{}: {} \n".format(term, posting_file))
                        inverted_index[term][2] = line
                        line += 1

        except:
            print(traceback.print_exc())

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_index(self, fn):
        """
        Loads a pre-computed index (or indices) so we can answer queries.
        Input:
            fn - file name of pickled index.
        """
        self._indexer.load_index(fn)

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_precomputed_model(self, model_dir=None):
        """
        Loads a pre-computed model (or models) so we can answer queries.
        This is where you would load models like word2vec, LSI, LDA, etc. and 
        assign to self._model, which is passed on to the searcher at query time.
        """
        pass



    def search(self, query):
        """
        Executes a query over an existing index and returns the number of
        relevant docs and an ordered list of search results.
        Input:
            query - string.
        Output:
            A tuple containing the number of relevant search results, and
            a list of tweet_ids where the first element is the most relavant
            and the last is the least relevant result.
        """
        query_as_list = self._parser.parse_sentence(query)
        query_expansion = self.query_expansion(query_as_list)
        self.add_similar_word_to_query(query_as_list, query_expansion)
        searcher = Searcher(self._parser, self._indexer, model=self._model)
        n_relevant, ranked_doc_ids = searcher.search(query_as_list)
        #print(ranked_doc_ids)
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
                result = self.get_top_n(word)
                if result is not None:
                    ret_dict[word] = word

        return ret_dict

    def get_top_n(self, word):
        self.data['form[1][value]'] = str(3)
        self.data['form[0][value]'] = word

        if word in self.local_cache:
            return self.local_cache[word]

        # Post the request to the server
        try:
            r = requests.post(self.url, self.data)
        except:
            pass

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
    config = configuration.ConfigClass()
    engine = SearchEngine(config=config)
    #print(os.path.join('data', 'benchmark_data_train.snappy.parquet'))
    engine.build_index_from_parquet(os.path.join('data', 'benchmark_data_train.snappy.parquet'))
    n_res, res = engine.search('bioweapon')
    print(res)
    # s.build_index_from_parquet("/Users/samuel/Desktop/Corpus/test")
    # s.search("Coronavirus is less dangerous than the flu")
