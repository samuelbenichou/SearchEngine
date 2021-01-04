from parser_module import Parse
from ranker import Ranker
import utils
from GloVeMethod import GloVeMethod
import linecache
from stemmer import Stemmer
from Word2Vec import Word2VecScrapper
import os
import traceback

class Searcher:

    def __init__(self, parser, indexer, model=None):
        """
        :param inverted_index: dictionary of inverted index
        """
        self._parser = parser
        self._indexer = indexer
        self._ranker = Ranker()
        self._model = model

    def relevant_docs_from_posting(self, query): # list of word after parsing
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        # glove = GloVeMethod('../../../../glove.twitter.27B.25d.txt')
        # similar_words = glove.most_similar(query)
        # for word in similar_words:
        #     query.append(word[0])
        term_in_query = {}
        for term in query:
            if term not in term_in_query.keys():
                term_in_query[term] = 1
            else:
                term_in_query[term] += 1
            try:
                if term in self.inverted_index:
                    self.get_file_from_pointer(self.inverted_index[term])
                elif self.stemmer.stem_term(term) in self.inverted_index:
                    self.get_file_from_pointer(self.inverted_index[self.stemmer.stem_term(term)])

                # if term not in term_in_query.keys():
                #     term_in_query[term] = 1
                # else:
                #     term_in_query[term] += 1

            except:
                print(traceback.print_exc())
                print('term {} not found in posting'.format(term))

        #print(self.relevant_docs)
        #print(term_in_query)
        return self.relevant_docs, term_in_query

    def relevant_docs_from_posting_with_word2Vec(self, query): # list of word after parsing
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        w2v = Word2VecScrapper()
        word_dict = w2v.get_top_n_dictionary(query)
        similar_words = []
        #print(word_dict)
        if word_dict is not None:
            for word in word_dict.keys():
                if word_dict[word] is not None:
                    similar_words.extend(word_dict[word])

        query.extend(similar_words)
        term_in_query = {}
        for term in query:
            try:
                if term in self.inverted_index:
                    self.get_file_from_pointer(self.inverted_index[term])
                elif self.stemmer.stem_term(term) in self.inverted_index:
                    self.get_file_from_pointer(self.inverted_index[self.stemmer.stem_term(term)])

                if term not in term_in_query.keys():
                    term_in_query[term] = 1
                else:
                    term_in_query[term] += 1

            except:
                print('term {} not found in posting'.format(term))

        #print(self.relevant_docs)
        #print(term_in_query)
        return self.relevant_docs, term_in_query

    def get_file_from_pointer(self, list_inverted_index):
        if len(list_inverted_index) == 3:
            pointer = list_inverted_index[2]
            self.get_pointer_from_string(pointer)
        elif len(list_inverted_index) == 4:
            pointer = list_inverted_index[2]
            pointer1 = list_inverted_index[3]
            self.get_pointer_from_string(pointer, pointer1)
        elif len(list_inverted_index) == 5:
            pointer = list_inverted_index[2]
            pointer1 = list_inverted_index[3]
            pointer2 = list_inverted_index[4]
            self.get_pointer_from_string(pointer, pointer1, pointer2)

    def get_pointer_from_string(self, *filename):
        for file in filename:
            path, line = file.split(' ')
            posting = linecache.getline(os.path.join(self.output_path, path), int(line)+2)
            term , tweet_id = posting.split(':')
            result = list(eval(tweet_id[1:-2]))
            if term not in self.relevant_docs:
                self.relevant_docs[term] = result
            else:
                self.relevant_docs[term].extend(result)
            #print(self.relevant_docs)

    def add_hashtag(self, query):
        result = ["#{}".format(term) for term in query]
        return result

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def search(self, query, k=None):
        """
        Executes a query over an existing index and returns the number of
        relevant docs and an ordered list of search results (tweet ids).
        Input:
            query - string.
            k - number of top results to return, default to everything.
        Output:
            A tuple containing the number of relevant search results, and
            a list of tweet_ids where the first element is the most relavant
            and the last is the least relevant result.
        """

        relevant_docs, posting_dic = self._relevant_docs_from_posting(query)
        n_relevant = len(relevant_docs)
        document_score = self._ranker.BM25(relevant_docs, query, posting_dic, self._indexer.tweet_index)
        #print(document_score)
        ranked_doc_ids = Ranker.rank_relevant_docs(document_score, k)
        return n_relevant, ranked_doc_ids

    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """
        relevant_docs = {}
        posting_dic = {}
        for term in query_as_list:
            posting_list = self._indexer.get_term_posting_list(term)
            if posting_list is not None:
                doc_id = {}
                for posting_term in posting_list:
                    df = relevant_docs.get(posting_term[0], 0)
                    relevant_docs[posting_term[0]] = df + 1
                    doc_id[posting_term[0]] = posting_term[1]
                posting_dic[term] = doc_id
            else:
                posting_dic[term] = {}
        return relevant_docs, posting_dic


if __name__ == '__main__':
    inverted_index = utils.load_obj('inverted_index')
    #print(inverted_index)
    s =Searcher('inverted_index')
    #print(s.add_hashtag(["SAmuel"]))
    #going
    s.relevant_docs_from_posting(['fucking', '#coronavirus' , '@JoeBiden'])
    #s.get_file_from_pointer([29, 29, 'postingMerge1.txt 1520', 'postingMerge2.txt 1504', 'postingMerge3.txt 1494'])

