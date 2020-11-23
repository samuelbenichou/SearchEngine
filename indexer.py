from posting import Posting

class Indexer:


    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.tweet_index = {}
        self.config = config
        self.posting_file = []
        self.number_of_term = 0
        self.num_of_file = 1

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
        create_posting_file = 2000
        """
        inverted_idx[term] = [ df , tf , pointer to the posting file ]
        postingDict[term] = [tweet_id, term appearance in tweet , [positional index of the term]] 
        tweet_index[tweet_id] = [max tf, number of words, number of unique words]
        We create a posting file every each certain amount of term
        """
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = [1, document_dictionary[term][0], ""]
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

                if self.number_of_term == create_posting_file:
                    self.create_posting_text()

            except:
                print('problem with the following key {}'.format(term[0]))

        """if self.number_of_term == 20:
            self.create_posting_text()
            p = Posting(self.postingDict, self.num_of_file)
            p.create_posting_file()
            self.posting_file.append(p.get_posting_path())
            self.num_of_file += 3
            print(self.postingDict)
            self.postingDict = {}  # delete the posting file
            self.number_of_term = 0"""

        self.tweet_index[document.tweet_id] = (most_recurrent_term, number_of_word, unique_term)

    def create_posting_text(self):
        p = Posting(self.postingDict, self.num_of_file)
        p.create_posting_file()
        self.posting_file.append(p.get_posting_path())
        self.num_of_file += 3
        #print(self.postingDict)
        self.postingDict = {}  # delete the posting file
        self.number_of_term = 0


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
