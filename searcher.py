from parser_module import Parse
from ranker import Ranker
import utils
from GloVeMethod import GloVeMethod
import linecache
from stemmer import Stemmer

class Searcher:

    def __init__(self, inverted_index):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.stemmer = Stemmer()
        self.inverted_index = inverted_index
        self.relevant_docs = {}

    def relevant_docs_from_posting(self, query): # list of word after parsing
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        inverted_index = utils.load_obj(self.inverted_index)
        glove = GloVeMethod('glove.twitter.27B.25d.txt')
        similar_words = glove.most_similar(query)
        query.extend(similar_words)
        #query.extend(self.add_hashtag(query))
        for term in query:
            try:
                if term in inverted_index:
                    self.get_file_from_pointer(inverted_index[term])
                elif self.stemmer.stem_term(term) in inverted_index:
                    self.get_file_from_pointer(inverted_index[self.stemmer.stem_term(term)])
                else:
                    continue
            except:
                print('term {} not found in posting'.format(term))

        #print(self.relevant_docs)
        return self.relevant_docs

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
            posting = linecache.getline(path, int(line)+2)
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




if __name__ == '__main__':
    inverted_index = utils.load_obj('inverted_index')
    #print(inverted_index)
    s =Searcher('inverted_index')
    #print(s.add_hashtag(["SAmuel"]))
    #going
    s.relevant_docs_from_posting(['fucking', '#coronavirus' , '@JoeBiden'])
    #s.get_file_from_pointer([29, 29, 'postingMerge1.txt 1520', 'postingMerge2.txt 1504', 'postingMerge3.txt 1494'])

