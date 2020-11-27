from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
from ExternalMergeSort import ExternalMergeSort
import utils
import concurrent.futures
import timeit
from ranker import Ranker
import traceback
import numpy as np

list_posting_file = []
number_of_tweet = 0

def run_engine():
    config = ConfigClass()
    r = ReadFile(corpus_path=config.get__corpusPath())
    filenames = r.get_filenames_path() # list of path
    print(filenames)

    four_split = np.array_split(filenames, 3)
    process1_list = list(four_split[0])
    process2_list = list(four_split[1])
    process3_list = list(four_split[2])

    with concurrent.futures.ProcessPoolExecutor() as executor:
        f1 = executor.submit(process_index, process1_list, 1)
        f2 = executor.submit(process_index, process2_list, 2)
        f3 = executor.submit(process_index, process3_list, 3)


def process_index(documents_list, num_thread):
    config = ConfigClass()
    p = Parse()
    indexer = Indexer(config, num_thread)
    print("Number of doc in the process {} : {}".format(num_thread,len(documents_list)))
    r = ReadFile(corpus_path=config.get__corpusPath())
    number_of_documents = 0
    start = timeit.default_timer()
    #print(documents_list)
    for doc in documents_list:
        print(doc)
        document_list = r.read_file(doc)

        # Iterate over every document in the file
        for idx, document in enumerate(document_list):
            # parse the document
            parsed_document = p.parse_doc(document)
            number_of_documents += 1
            # index the document data
            indexer.add_new_doc(parsed_document)
        if indexer.number_of_term > 0:
            indexer.create_posting_text()

    mergeSort = ExternalMergeSort(indexer.posting_file, num_thread)
    mergeSort.external_merge_sort()
    mergeSort.connect_pointer_to_term(indexer.inverted_idx)
    posting_file = mergeSort.get_posting_file()
    list_posting_file.append(posting_file)
    # print(posting_file)
    stop = timeit.default_timer()
    print("Time of indexer and posting of process {} : ".format(num_thread), stop - start)
    return indexer


def merge_inverted_index_dict(dict1, dict2, dict3):
    for key, value in dict2.items():
        if key in dict1:
            dict1[key][0] += value[0]
            dict1[key][1] += value[1]
            dict1[key].append(value[2])
            #dict1[key][2] += value[2]
        else:
            dict1[key] = value

    for key, value in dict3.items():
        if key in dict1:
            dict1[key][0] += value[0]
            dict1[key][1] += value[1]
            dict1[key].append(value[2])
        else:
            dict1[key] = value

    return dict1


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
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    print("***************")
    print(relevant_docs)
    number_of_tweet = 1105
   # r = Ranker(number_of_tweet)
   #  ranked_docs = r.rank_relevant_doc(relevant_docs)
   #  return r.retrieve_top_k(ranked_docs, k)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, number_of_tweet) #number_of_tweet
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main():
    run_engine()
    #tweet_file = utils.load_obj('tweet_index')
    #print(tweet_file)
    #inverted_index = utils.load_obj('inverted_index')
    #print(inverted_index)
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    #posting1 = load_index("posting1")
    #posting2 = load_index("posting2")
    #merge(posting1,posting2)
    #print(dict3)
    # print(search_and_rank_query(query, 'inverted_index', k))
    #1280966287972237314 Indoor malls reopen coronavirus shutdown Friday




