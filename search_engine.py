from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
from ExternalMergeSort import ExternalMergeSort
import utils
import concurrent.futures
import timeit
import traceback
import numpy as np
import json
import os
import pandas as pd


def split_list_into_equal_sublist(corpusPath):

    r = ReadFile(corpus_path=corpusPath)
    filenames = r.get_filenames_path()

    total = 0
    file_dict = {}
    for file in filenames:
        full_path = os.path.join(corpusPath, file)
        size = os.path.getsize(full_path)
        total += size
        file_dict[file] = size
    #print(file_dict)
    partSum = []
    partIndex = 0
    noOfParts = 3  # Initialize the no. of parts
    result = [None] * 3  # Initialize result array with no. of locations equal to no. of parts, to store partSums
    threshold = (total / noOfParts) - (total / len(filenames))/ 2  # Calculate a minimum threshold value for partSum
    for key, value in file_dict.items():
        partSum.append((key,value))  # Add array values to partSum incrementally
        if (sum([pair[1] for pair in partSum]) >= threshold):  # If partSum reaches the threshold value, add it to result[] and reset partSum
            result[partIndex] = partSum
            partIndex += 1
            partSum = []
            continue
    if (partIndex < noOfParts):  # If no. of parts in result[] is less than the no. of parts required, add the remaining partSum value
        result[partIndex] = partSum
    ans = []
    for sublist in result:
        ans.append([i[0] for i in sublist])
    # print(result)
    # print(ans)
    # print("first file {}".format(19963349+39056658+23823236+80627004+78211025+83557131+99902906+112147350+49091027+82911409))
    # print("second file {}".format(126968465+64148629+13210841+87456048+70016103+104997308+172847038+156207406))
    # print("second file {}".format(73820682+107380580+138330977+49523686+79172235+75045303+80345602))
    return ans

def run_engine(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):

    # Configure the path
    config = ConfigClass()
    config.corpusPath = corpus_path
    config.savedFileMainFolder = output_path
    config.toStem = stemming

    # Get list of all parquets files
    r = ReadFile(corpus_path=config.get__corpusPath())
    filenames = r.get_filenames_path()
    corpus = []
    for file in filenames:
        corpus.extend(r.read_file(file))

    three_split = np.array_split(corpus, 3)
    process1_list = list(three_split[0])
    process2_list = list(three_split[1])
    process3_list = list(three_split[2])
    # if len(filenames) > 3:
    #     filenames = split_list_into_equal_sublist(config.get__corpusPath())
    #     process1_list = filenames[0]
    #     process2_list = filenames[1]
    #     process3_list = filenames[2]
    # else:
    #     # Split the list into three equal part
    #     three_split = np.array_split(filenames, 3)
    #     process1_list = list(three_split[0])
    #     process2_list = list(three_split[1])
    #     process3_list = list(three_split[2])

    print(filenames)

    # Start 3 processes
    with concurrent.futures.ProcessPoolExecutor() as executor:
        f1 = executor.submit(process_index, process1_list, 1, corpus_path, output_path, stemming)
        f2 = executor.submit(process_index, process2_list, 2, corpus_path, output_path, stemming)
        f3 = executor.submit(process_index, process3_list, 3, corpus_path, output_path, stemming)

    list_posting_file = [f1.result()[0], f2.result()[0], f3.result()[0]]
    list_inverted_index_file = [f1.result()[1], f2.result()[1], f3.result()[1]]
    list_tweet_file = [f1.result()[2], f2.result()[2], f3.result()[2]]
    number_of_tweet = f1.result()[3] + f2.result()[3] + f3.result()[3]
    print("***********")
    print(list_posting_file)
    print(list_inverted_index_file)
    print(list_tweet_file)
    print(number_of_tweet)

    # if os.path.isfile(queries):  # If the queries are stored in a file
    #     with open(queries, encoding="utf8") as file:
    #         queries = file.readlines()

    # p = Parse(config.toStem)
    # tweet_file = merge_inverted_index_dict(list_tweet_file[0], list_tweet_file[1], list_tweet_file[2])
    # inverted_index = merge_inverted_index_dict(list_inverted_index_file[0], list_inverted_index_file[1], list_inverted_index_file[2])
    # if type(queries) != list:
    #     with open(queries, encoding="utf8") as file:
    #         queries = file.readlines()
    # resultGloVe = pd.DataFrame(columns=['Query_num', 'Tweet_id', 'Rank' , 'Score'])
    # resultWOrd2Vec = pd.DataFrame(columns=['Query_num', 'Tweet_id', 'Rank'])
    # counter = 1
    # rank = 1
    # rank1 = 1
    # for query in queries:
    #     if query != '\n':
    #         searcher = Searcher(inverted_index)
    #         #searcher1 = Searcher(inverted_index)
    #         query_as_list = p.parse_sentence(query)
    #         relevant_docs, term_in_query = searcher.relevant_docs_from_posting(query_as_list)
    #         #relevant_docs1, term_in_query1 = searcher1.relevant_docs_from_posting_with_word2Vec(query_as_list)
    #         ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, number_of_tweet, tweet_file, term_in_query)  # number_of_tweet
    #         #ranked_docs1 = searcher1.ranker.rank_relevant_doc(relevant_docs1, number_of_tweet, tweet_file,term_in_query1)  # number_of_tweet
    #         ranked_tweets = searcher.ranker.retrieve_top_k(ranked_docs, num_docs_to_retrieve)
    #         #ranked_tweets1 = searcher1.ranker.retrieve_top_k(ranked_docs1, num_docs_to_retrieve)
    #         print(ranked_tweets)
    #         for doc_tuple in ranked_tweets:
    #             #resultGloVe = resultGloVe.append({"Query_num":counter , "Tweet_id": doc_tuple[0], "Rank": rank, "Score": doc_tuple[1]},ignore_index=True)
    #             print("Tweet id: {} Score: {}".format(doc_tuple[0], doc_tuple[1]))
    #             rank += 1
    #         #print(ranked_tweets1)
    #         # for doc in ranked_tweets1:
    #         #     #resultWOrd2Vec = resultWOrd2Vec.append({"Query_num":counter , "Tweet_id": doc[0], "Rank": rank1, "Score": doc[1]},ignore_index=True)
    #         #     print("Tweet id: {} Score: {}".format(doc[0], doc[1]))
    #         #     rank1 += 1
    #         counter += 1
    # # resultGloVe.to_csv(os.path.join(output_path, "resultsGloVe.csv"))
    # # resultWOrd2Vec.to_csv(os.path.join(output_path, "resultsWord2Vec.csv"))

    #print(resultGloVe)
    #return resultGloVe



def process_index(documents_list, num_thread, corpus_path, output_path, stemming):
    config = ConfigClass()
    config.corpusPath = corpus_path
    config.savedFileMainFolder = output_path
    config.toStem = stemming
    p = Parse(config.toStem)
    indexer = Indexer(config, num_thread)
    print("Number of tweet in the process {} : {}".format(num_thread,len(documents_list)))
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

    if indexer.number_of_term > 0: # create a new files in we didnt reach a certain amouts of tweets
        indexer.create_posting_text()
        indexer.create_inverted_index()
        indexer.create_tweet_index()
        indexer.postingDict = {} # delete the posting file
        indexer.inverted_idx = {} # delete the inverted index file
        indexer.tweet_index = {} # delete the tweet index file

    try:
        mergeSort = ExternalMergeSort(indexer.posting_file, indexer.inverted_idx_file, indexer.tweet_index_file,
                                      num_thread)
        # print(indexer.posting_file)
        # print(indexer.inverted_idx_file)
        # print(indexer.tweet_index_file)
        mergeSort.external_merge_sort()  # merge the posting file
        mergeSort.external_inverted_index_merge()  # merge the inverted index file
        mergeSort.external_tweet_index_merge()  # merge the tweet index file
        # print(mergeSort.inverted_idx_file)
        # print(mergeSort.posting_file)
        # print(mergeSort.tweet_index_file)
        # inverted_index = load_inverted_index(mergeSort.inverted_idx_file[0])
        # inverted_index_with_pointer = mergeSort.connect_pointer_to_term(inverted_index)
        mergeSort.update_pointer()
        inverted_index_file = mergeSort.get_inverted_index_file()
        # list_inverted_index_file.append(inverted_index_file)
        # print(list_inverted_index_file)
        posting_file = mergeSort.get_posting_file()
        # list_posting_file.append(posting_file)
        # print(list_posting_file)
        tweet_file = mergeSort.get_tweet_file()
        # list_tweet_file.append(tweet_file)
        # print(list_tweet_file)
        result = [posting_file, inverted_index_file, tweet_file, number_of_documents]
        stop = timeit.default_timer()
        print("Time of indexer and posting of process {} : ".format(num_thread), stop - start)
        return result
    except:
        print(traceback.print_exc())





    # r = ReadFile(corpus_path=config.get__corpusPath())
    # number_of_documents = 0
    # start = timeit.default_timer()
    # #print(documents_list)
    # if len(documents_list) > 0:
    #     for doc in documents_list:
    #         try:
    #             print(doc)
    #             document_list = r.read_file(doc)
    #             # Iterate over every document in the file
    #             for idx, document in enumerate(document_list):
    #                 # parse the document
    #                 parsed_document = p.parse_doc(document)
    #                 number_of_documents += 1
    #                 # index the document data
    #                 indexer.add_new_doc(parsed_document)
    #
    #         except:
    #             print("Problem with process {}".format(num_thread))
    #             print(traceback.print_exc())
    #
    #     if indexer.number_of_term > 0: # create a new files in we didnt reach a certain amouts of tweets
    #         indexer.create_posting_text()
    #         indexer.create_inverted_index()
    #         indexer.create_tweet_index()
    #         indexer.postingDict = {} # delete the posting file
    #         indexer.inverted_idx = {} # delete the inverted index file
    #         indexer.tweet_index = {} # delete the tweet index file
    #
    #     try:
    #         mergeSort = ExternalMergeSort(indexer.posting_file, indexer.inverted_idx_file, indexer.tweet_index_file, num_thread)
    #         # print(indexer.posting_file)
    #         # print(indexer.inverted_idx_file)
    #         # print(indexer.tweet_index_file)
    #         mergeSort.external_merge_sort() # merge the posting file
    #         mergeSort.external_inverted_index_merge() # merge the inverted index file
    #         mergeSort.external_tweet_index_merge() # merge the tweet index file
    #         # print(mergeSort.inverted_idx_file)
    #         # print(mergeSort.posting_file)
    #         # print(mergeSort.tweet_index_file)
    #         # inverted_index = load_inverted_index(mergeSort.inverted_idx_file[0])
    #         # inverted_index_with_pointer = mergeSort.connect_pointer_to_term(inverted_index)
    #         mergeSort.update_pointer()
    #         inverted_index_file = mergeSort.get_inverted_index_file()
    #         # list_inverted_index_file.append(inverted_index_file)
    #         #print(list_inverted_index_file)
    #         posting_file = mergeSort.get_posting_file()
    #         # list_posting_file.append(posting_file)
    #         #print(list_posting_file)
    #         tweet_file = mergeSort.get_tweet_file()
    #         # list_tweet_file.append(tweet_file)
    #         #print(list_tweet_file)
    #         result = [posting_file, inverted_index_file, tweet_file, number_of_documents]
    #         stop = timeit.default_timer()
    #         print("Time of indexer and posting of process {} : ".format(num_thread), stop - start)
    #         return result
    #     except:
    #         print(traceback.print_exc())
    # else:
    #     return [None, None, None, 0]


def load_inverted_index(inverted_index):
    with open(inverted_index, encoding="cp437", errors='ignore') as file1:
        dic1 = json.load(file1)
    return dic1


def merge_inverted_index_dict(file1, file2, file3):
    dict = []
    if file1 is not None:
        with open(file1, encoding="cp437", errors='ignore') as f1:
            dict1 = json.load(f1)
            dict.append(dict1)
    if file2 is not None:
        with open(file2, encoding="cp437", errors='ignore') as f2:
            dict2 = json.load(f2)
            dict.append(dict2)
    if file3 is not None:
        with open(file3, encoding="cp437", errors='ignore') as f3:
            dict3 = json.load(f3)
            dict.append(dict3)
    for dic in dict[1:]:
        for key, value in dic.items():
            if key in dict[0]:
                dict[0][key][0] += value[0]
                dict[0][key][1] += value[1]
                dict[0][key].append(value[2])
                # dict1[key][2] += value[2]
            else:
                dict[0][key] = value

        # for key, value in dict3.items():
        #     if key in dict1:
        #         dict1[key][0] += value[0]
        #         dict1[key][1] += value[1]
        #         dict1[key].append(value[2])
        #     else:
        #         dict1[key] = value

    return dict[0]


def load_index(name):
    print("Load 1")
    #inverted_index = utils.load_obj("inverted_idx")
    inverted_index = utils.load_obj(name)
    #print(inverted_index)
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs,term_in_query = searcher.relevant_docs_from_posting(query_as_list)
    print("***************")
    #print(relevant_docs)
    number_of_tweet = 316837
    tweet_file = merge_inverted_index_dict("tweet_index3.json", "tweet_index_Merge1", "tweet_index_Merge2")
   # r = Ranker(number_of_tweet)
   #  ranked_docs = r.rank_relevant_doc(relevant_docs)
   #  return r.retrieve_top_k(ranked_docs, k)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, number_of_tweet,tweet_file, term_in_query) #number_of_tweet
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main():
    run_engine('/Users/samuel/Desktop/Test','/Users/samuel/Desktop/Test', False,['COVID cases'], 20)
    #tweet_file = utils.load_obj('tweet_index')
    #print(tweet_file)
    #inverted_index = utils.load_obj('inverted_index')
    #print(inverted_index)
    # tweet_file = merge_inverted_index_dict("tweet_index1.json", None, None)
    # inverted_index = merge_inverted_index_dict("inverted_index_pointer1.json", "inverted_index_pointer2.json", "inverted_index_pointer3.json")
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    #
    # print(search_and_rank_query(query, inverted_index, k))
    # '1287558160618725377', 'Mon Jul 27 01:19:21 +0000 2020', 'RT @ASlavitt: Florida just passed NY in total COVID cases.
    #split_list_into_equal_sublist('/Users/samuel/Desktop/Test')




