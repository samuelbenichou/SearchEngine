from posting import Posting
import json
from io import BytesIO
from gzip import GzipFile
import traceback
import linecache
import os
from configuration import ConfigClass
import utils

class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.tweet_index = {}
        self.config = config
        self.posting_file = []
        self.inverted_idx_file = []
        self.tweet_index_file = []
        self.number_of_term = 0
        self.num_of_file = 0
        self.config = config

    def set_thread(self, thread_number):
        self.num_of_file = thread_number

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        most_recurrent_term = 0
        unique_term = 0
        number_of_word = 0
        number_of_different_term = 0
        create_posting_file = 1000000
        """
        inverted_idx[term] = [ df , tf , pointer to the posting file (line in the text file) ]
        postingDict[term] = [tweet_id, term appearance in tweet , [positional index of the term]] 
        tweet_index[tweet_id] = [max tf, number of words, number of unique words, number of different term]
        We create a posting file every each certain amount of term
        """
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = [1, document_dictionary[term][0], 0]
                    self.postingDict[term] = []
                else:
                    self.inverted_idx[term][0] += 1
                    self.inverted_idx[term][1] += document_dictionary[term][0]

                if document_dictionary[term][0] > most_recurrent_term:
                    most_recurrent_term = document_dictionary[term][0]

                if document_dictionary[term][0] == 1:
                    unique_term += 1

                number_of_word += document_dictionary[term][0]
                if term in self.postingDict:
                    self.postingDict[term].append((document.tweet_id, document_dictionary[term][0], document_dictionary[term][1]))
                else:
                    self.postingDict[term] = [(document.tweet_id, document_dictionary[term][0], document_dictionary[term][1])]

                self.number_of_term += 1

                # if self.number_of_term == create_posting_file:
                #     self.create_posting_text()
                #     self.create_inverted_index()
                #     self.create_tweet_index()
                #     self.num_of_file += 3
                #     self.postingDict = {}  # delete the posting file
                #     self.inverted_idx = {}
                #     self.tweet_index = {}
                #     self.number_of_term = 0

                number_of_different_term += 1

            except:
                print('problem with the following key {}'.format(term[0]))


        self.tweet_index[document.tweet_id] = (most_recurrent_term, number_of_word, unique_term, number_of_different_term)

    def create_posting_text(self):
        p = Posting(self.postingDict, self.num_of_file)
        p.create_posting_file()
        self.posting_file.append(p.get_posting_path())
        # self.num_of_file += 3
        #print(self.postingDict)
        # self.postingDict = {}  # delete the posting file
        # self.number_of_term = 0

    def create_inverted_index(self):
        try:
            with open("inverted_index{}.json".format(self.num_of_file), "w", encoding="cp437",errors='ignore') as fp:
                json.dump(self.inverted_idx, fp)
                self.inverted_idx_file.append("inverted_index{}.json".format(self.num_of_file))
        except:
            print(traceback.print_exc())

    def create_tweet_index(self):
        try:
            with open("tweet_index{}.json".format(self.num_of_file), "w", encoding="cp437",errors='ignore') as fp:
                json.dump(self.tweet_index, fp)
                self.tweet_index_file.append("tweet_index{}.json".format(self.num_of_file))
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
        return utils.load_obj(fn)

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def save_index(self, fn):
        """
        Saves a pre-computed index (or indices) so we can save our work.
        Input:
              fn - file name of pickled index.
        """
        utils.save_obj(self, fn)

    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def _is_term_exist(self, term):
        """
        Checks if a term exist in the dictionary.
        """
        return term in self.inverted_idx

    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def get_term_posting_list(self, term):
        """
        Return the posting list from the index for a term.
        """
        if self._is_term_exist(term):
            line = self.inverted_idx[term][2]
            posting = linecache.getline('posting_file.txt', line)
            term, tweet_id = posting.split(':')
            result = list(eval(tweet_id[1:-2]))
            return result





