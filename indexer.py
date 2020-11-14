class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.tweet_index = {}
        self.config = config

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        most_recurrent_term = ('',0)
        unique_term = 0
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:

                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = []
                else:
                    self.inverted_idx[term] += 1

                if self.inverted_idx[term] > most_recurrent_term[1]:
                    most_recurrent_term = (term, self.inverted_idx[term])

                if document_dictionary.get(term) == 1:
                    unique_term += 1

                self.postingDict[term].append((document.tweet_id, document_dictionary[term]))

            except:
                print('problem with the following key {}'.format(term[0]))

        self.tweet_index[document.tweet_id] = (most_recurrent_term[1], unique_term)

    def get_tweet_info(self, tweet_id):
        print("*** TWEET INFORMATION ***")
        print("tweet id : {} ".format(tweet_id))
        print("max tf : {}".format(self.tweet_index.get(tweet_id)[0]))
        print("num of umique word : {}".format(self.tweet_index.get(tweet_id)[1]))

    def get_term_info(self, term):
        print("*** TERM INFORMATION ***")
        print("term : {} ".format(term))
        print("df : {}".format(len(self.postingDict.get(term))))
        print("term frequency : {}".format(self.inverted_idx.get(term)))
        print("******")
