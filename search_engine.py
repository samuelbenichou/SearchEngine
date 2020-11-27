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
    #p = Parse()
    #indexer = Indexer(config)
    filenames = r.get_filenames_path() # list of path
    print(filenames)

    #process_index(filenames, 1)
    four_split = np.array_split(filenames, 4)
    process1_list = list(four_split[0])
    process2_list = list(four_split[1])
    process3_list = list(four_split[2])
    process4_list = list(four_split[3])

    with concurrent.futures.ProcessPoolExecutor() as executor:
        f1 = executor.submit(process_index, process1_list, 1)
        f2 = executor.submit(process_index, process2_list, 2)
        f3 = executor.submit(process_index, process3_list, 3)
        f4 = executor.submit(process_index, process4_list, 4)


    #documents_list = r.read_file(file_name='sample3.parquet')
    # documents_list = r.read_file(file_name='/Users/samuel/Desktop/Corpus')
    # print("Number of tweets in the corpus {}".format(len(documents_list)))
    #
    # number_of_tweet = len(documents_list)
    # third = len(documents_list) // 3
    # two_third = 2 * third
    # start = timeit.default_timer()

    def fuck():
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     f1 = executor.submit(parse_index_doc, documents_list[:third], 1)
        #     f2 = executor.submit(parse_index_doc, documents_list[third:two_third], 2)
        #     f3 = executor.submit(parse_index_doc, documents_list[two_third:], 3)
        #
        # stop = timeit.default_timer()
        # print('total time: ', stop - start)

        # Merge inverted index
        # new_inverted_index = merge_inverted_index_dict(f1.result().inverted_idx, f2.result().inverted_idx, f3.result().inverted_idx)
        # tweet_index = {**f1.result().tweet_index, **f2.result().tweet_index, **f3.result().tweet_index}
        # utils.save_obj(new_inverted_index, "inverted_index")
        # utils.save_obj(tweet_index, "tweet_index")
        # print(new_inverted_index)
        # print(list_posting_file)

        # stop = timeit.default_timer()
        # print('Time of parser: ', stop - start)

        # print(documents_list)
        """start = timeit.default_timer()
        # Iterate over every document in the file
        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = p.parse_doc(document)
            #number_of_documents += 1
            # index the document data
            indexer.add_new_doc(parsed_document)
        #print(indexer.inverted_idx)
        if indexer.number_of_term > 0:
            indexer.create_posting_text()


        mergeSort = ExternalMergeSort(indexer.posting_file)
        mergeSort.external_merge_sort()
        mergeSort.connect_pointer_to_term(indexer.inverted_idx)
        posting_file = mergeSort.get_posting_file()
        print(posting_file)"""
        # print(indexer.postingDict)
        # print(indexer.posting_file)

        # print(indexer.inverted_idx)
        # print(indexer.tweet_index)
        # print(indexer.get_tweet_info('1280966306292998145'))
        # print(indexer.get_term_info("Donald Trump"))
        # print('Finished parsing and indexing. Starting to export files')

        # stop = timeit.default_timer()
        # print('Time of indexer : ', stop - start)



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
        #print(document_list)
        #outF = open("parsing1.txt", "w")
        # Iterate over every document in the file
        for idx, document in enumerate(document_list):
            # parse the document
            parsed_document = p.parse_doc(document)
            #print(parsed_document.term_doc_dictionary)
            # outF.write(parsed_document.term_doc_dictionary)
            # outF.write("\n")
            #print(parsed_document)
            number_of_documents += 1
            # index the document data
    #         indexer.add_new_doc(parsed_document)
    #     if indexer.number_of_term > 0:
    #         indexer.create_posting_text()
        #outF.close()
    print(" Num of tweet in process {} is {}".format(num_thread, number_of_documents))

    # mergeSort = ExternalMergeSort(indexer.posting_file, num_thread)
    # mergeSort.external_merge_sort()
    # mergeSort.connect_pointer_to_term(indexer.inverted_idx)
    # posting_file = mergeSort.get_posting_file()
    # list_posting_file.append(posting_file)
    # # print(posting_file)
    stop = timeit.default_timer()
    print("Time of indexer and posting of process {} : ".format(num_thread), stop - start)
    # outF = open("parsing.txt", "w")
    # for line in parsed_document:
    #     # write line to output file
    #     outF.write(line)
    #     outF.write("\n")
    # outF.close()
    # return indexer

def parse_index_doc(documents_list, num_thread):
    config = ConfigClass()
    p = Parse()
    indexer = Indexer(config, num_thread)
    print("Number of tweets in the thread {}".format(len(documents_list)))


    start = timeit.default_timer()
    # Iterate over every document in the file
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        # number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)
    if indexer.number_of_term > 0:
        indexer.create_posting_text()

    mergeSort = ExternalMergeSort(indexer.posting_file, num_thread)
    mergeSort.external_merge_sort()
    mergeSort.connect_pointer_to_term(indexer.inverted_idx)
    posting_file = mergeSort.get_posting_file()
    list_posting_file.append(posting_file)
    #print(posting_file)
    stop = timeit.default_timer()
    print("Time of indexer and posting of thread {} : ".format(num_thread), stop - start)
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




