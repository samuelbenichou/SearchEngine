from math import sqrt
from math import log10
import numpy as np
import math


class Ranker:

    def __init__(self):
        # FREE PARAMETERS
        self.B = 0.75
        self.K = 1.2

    # you can change whatever you want in this module, just make sure it doesn't
    # break the searcher module
    @staticmethod
    def rank_relevant_docs(relevant_docs, k=None):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param k: number of most relevant docs to return, default to everything.
        :param relevant_docs: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ranked_results = sorted(relevant_docs.items(), key=lambda item: item[1], reverse=True)
        if k is not None:
            ranked_results = ranked_results[:k]
        print(ranked_results)
        return [d[0] for d in ranked_results]

    def BM25(self, relevant_docs, query, posting_dic, tweet_index):
        """
        Calculate the score for each document according to the Okapi BM25
        """
        document_score = {}
        N = len(tweet_index)
        avg_field_len = self.total_len(relevant_docs, tweet_index)/N
        for document in relevant_docs.keys():
            score = 0
            for term in query:
                if document in posting_dic[term].keys():
                    f = posting_dic[term][document]
                    field_len = tweet_index[document][1]
                    score += self.IDF(posting_dic, term, N) * ((self.K+1)*f)/(f+self.K*(1-self.B+self.B*(field_len/avg_field_len)))
            document_score[document] = score
        return document_score

    def IDF(self, posting_dic, term, N):
        """
        Return the Inverse document frequency weight of the query term
        """
        idf = np.log(((N - len(posting_dic[term]) + 0.5)/(len(posting_dic[term]) + 0.5)) + 1)
        return idf

    def total_len(self, relevant_docs, tweet_index):
        """
        Return the total document lenght in the text collection from which documents are drawn
        """
        total = 0
        for doc, value in relevant_docs.items():
            total += tweet_index[doc][1]
        return total

    @staticmethod
    def getAxiomaticTermWeightingScore(docDF, termDF, documentLength, docLengthAvg, numOfDocs):
        #         https://pdfs.semanticscholar.org/94c9/30d010c17f3edc0df39ea99fd311d33327c1.pdf

        mone = docDF * math.pow(numOfDocs, 0.35)

        mehane = (docDF + 0.5 + (0.5 * float(documentLength) / docLengthAvg)) * termDF

        return mone / mehane

    @staticmethod
    def _rank_relevant_doc(relevant_doc, number_of_tweet, tweet_file, term_in_query):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        tweet_id = {}
        df = {}

        for term in relevant_doc.keys():
            df[term] = len(relevant_doc[term])
            for data in relevant_doc[term]:
                if data[0] not in tweet_id:
                    tweet_id[data[0]] = [[term, data[1], data[2]]]
                else:
                    tweet_id[data[0]].append([term, data[1], data[2]])

        #print(tweet_id)
        ranked_tweet = Ranker.cosSim(tweet_id, df, number_of_tweet, tweet_file, term_in_query)
        #print(ranked_tweet)
        return sorted(ranked_tweet.items(), key=lambda item: item[1], reverse=True)

        #return sorted(relevant_doc.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        if k > len(sorted_relevant_doc):
            return sorted_relevant_doc
        result = [i for i in sorted_relevant_doc[:k]]
        return result

    @staticmethod
    def cosSim(tweet_id, df, number_of_tweet, tweet_file, term_in_query):
        ranked_tweet = {}
        #tweet_file = utils.load_obj('tweet_index')
        #inverted_index = utils.load_obj('inverted_index')
        #print(inverted_index)
        #print(inverted_index['COVID19'])
        #print("term query : {}".format(term_in_query))
        for id in tweet_id.keys():
            tweet_data = tweet_id[id]
            for data in tweet_data:
                if id not in ranked_tweet:
                    # print(" tweet file by id {}".format( tweet_file[id]))
                    #print(data)
                    # print(data[1])
                    # print("***{}***".format(data[0]))
                    # print(inverted_index[data[0][:-1]])
                    #if id == '1280966287972237314':
                        # print(self.tf(tweet_file[id], data[1])*self.idf(data[0]))
                        # print(inverted_index[data[0][:-1]])
                        # print(tweet_file[id])
                    #ranked_tweet[id] = self.tf(tweet_file[id], data[1])*self.idf(data[0])
                    # print(id)
                    # print(data[1])
                    # print(tweet_file[id])
                    # print(df[data[0]])
                    # print(term_in_query[data[0][:-1]])
                    try:
                        ranked_tweet[id] = int(data[1])/int(tweet_file[id][0]) * log10(number_of_tweet/df[data[0]]) * term_in_query[data[0][:-1]]
                    except:
                        ranked_tweet[id] = int(data[1]) / int(tweet_file[id][0]) * log10(number_of_tweet / df[data[0]])
                else:
                    #if id == '1280966287972237314':
                        # print(self.tf(tweet_file[id], data[1])*self.idf(data[0]))
                        # print(inverted_index[data[0][:-1]])
                        # print(tweet_file[id])
                    #ranked_tweet[id] += self.tf(tweet_file[id], data[1])*self.idf(data[0])
                    # print(id)
                    # print(data[1])
                    # print(tweet_file[id])
                    # print(df[data[0]])
                    # print(term_in_query[data[0][:-1]])
                    try:
                        ranked_tweet[id] = int(data[1]) / int(tweet_file[id][0]) * log10(
                            number_of_tweet / df[data[0]]) * term_in_query[data[0][:-1]]
                    except:
                        ranked_tweet[id] = int(data[1]) / int(tweet_file[id][0]) * log10(number_of_tweet / df[data[0]])
        Wiq = 0
        for term in term_in_query.keys():
            Wiq += term_in_query[term]*term_in_query[term]
        #print(Wiq)
        for id in ranked_tweet.keys():
            ranked_tweet[id] /= sqrt(Wiq*tweet_file[id][1]*tweet_file[id][1])
        return ranked_tweet



if __name__ == '__main__':
    r = Ranker()
    dic = {'COVID19 ': [('1280966292908974083', 1, [15]), ('1280966293588344832', 1, [25]), ('1280966294624403461', 1, [13]), ('1280966295350054913', 1, [5]), ('1280966296595763201', 1, [16]), ('1280966297614876672', 1, [3]), ('1280966298219032577', 1, [4]), ('1280966299087253505', 1, [30]), ('1280966301788319744', 1, [13]), ('1280966304241860608', 1, [9]), ('1280966305546469378', 1, [16]), ('1280966306808819712', 1, [17]), ('1280966307266072582', 1, [9]), ('1280966309551865857', 1, [17]), ('1280966309795246082', 1, [10]), ('1280966313435774977', 1, [13]), ('1280966313481994240', 1, [14]), ('1280966314433982464', 1, [15]), ('1280966314756988928', 1, [13]), ('1280966317357572096', 1, [24]), ('1280966318150180865', 1, [14]), ('1280966320255893506', 1, [13]), ('1280966321409269760', 1, [14]), ('1280966321354805249', 1, [14]), ('1280966322646470656', 1, [15]), ('1280966325783932934', 3, [14, 18, 20]), ('1280966330280222720', 1, [16]), ('1280966330431295488', 1, [10]), ('1280966330670354432', 1, [13]), ('1280966332599734277', 1, [20]), ('1280966332121591813', 1, [19]), ('1280966334013038592', 1, [19]), ('1280966333711233026', 1, [15]), ('1280966336961826827', 1, [10]), ('1280966336982781953', 1, [13]), ('1280966337351823360', 1, [13])]}
    rd = r.rank_relevant_doc(dic)
    print(r.retrieve_top_k(rd , 5))

    #r.rank_relevant_doc({'fucking ': [('1280966300202725376', 1, [2]), ('1280966300525715456', 1, [7]), ('1280966302522318849', 1, [7]), ('1280966305324171264', 1, [2]), ('1280966306854920192', 1, [7]), ('1280966312110493696', 1, [7]), ('1280966328640307200', 1, [3]), ('1280966330372493312', 1, [3])], '#coronavirus ': [('1280966293370425349', 1, [16]), ('1280966295442411521', 1, [27]), ('1280966296612593664', 1, [22]), ('1280966302664884226', 1, [12]), ('1280966305831673856', 1, [15]), ('1280966319207309316', 1, [9]), ('1280966325783932934', 1, [17]), ('1280966332599734277', 1, [19])], '@JoeBiden ': [('1280966306292998145', 1, [1]), ('1280966310357356554', 1, [1]), ('1280966317357572098', 1, [1]), ('1280966336550785026', 1, [1])]})