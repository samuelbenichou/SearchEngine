from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
from ExternalMergeSort import ExternalMergeSort
import utils
import concurrent.futures
import timeit

list_posting_file = []

def run_engine():
    config = ConfigClass()
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse()
    #indexer = Indexer(config)


    #documents_list = r.read_file(file_name='sample3.parquet')
    documents_list = r.read_file(file_name='/Users/samuel/Desktop/Corpus')
    print("Number of tweets in the corpus {}".format(len(documents_list)))


    third = len(documents_list) // 3
    two_third = 2 * third
    start = timeit.default_timer()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        f1 = executor.submit(parse_index_doc, documents_list[:third], 1)
        f2 = executor.submit(parse_index_doc, documents_list[third:two_third], 2)
        f3 = executor.submit(parse_index_doc, documents_list[two_third:], 3)

    stop = timeit.default_timer()
    print('total time: ', stop - start)


    # Merge inverted index
    new_inverted_index = merge_inverted_index_dict(f1.result().inverted_idx, f2.result().inverted_idx, f3.result().inverted_idx)
    utils.save_obj(new_inverted_index, "inverted_index")
    print(list_posting_file)

    #print(f1.result().inverted_idx)
    #print(f2.result().inverted_idx)

    #stop = timeit.default_timer()
    #print('Time of parser: ', stop - start)

    #utils.save_obj(f1.result(), "posting1")
    #utils.save_obj(f2.result(), "posting2")
    #print(f1.result())
    """print("******************************************************************")
    print("******************************************************************")
    print("******************************************************************")
    print("******************************************************************")
    print("******************************************************************")"""
    #print(f2.result())


    #print(documents_list)
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
    #print(indexer.postingDict)
    #print(indexer.posting_file)

    #print(indexer.inverted_idx)
    #print(indexer.tweet_index)
    #print(indexer.get_tweet_info('1280966306292998145'))
    #print(indexer.get_term_info("Donald Trump"))
    #print('Finished parsing and indexing. Starting to export files')


    #stop = timeit.default_timer()
    #print('Time of indexer : ', stop - start)

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

def merge_inverted_index_dict(*dict):
    result_dict = {}
    for d in dict:
        for k, v in d.items():
            result_dict.setdefault(k, []).extend(v)
    #print(result_dict)
    return result_dict

"""def final_merge():
    with open("special_character_posting.txt", 'w') as file1,open("az.txt", 'w') as file2:
        for file in list_posting_file:
            with open(file) as post_file:
                line = post_file.readline()
                while line:
                    # print(line.split(':')[0])
                    line = file.readline()
                    key = line.split(':')[0][:-1]
                    if key in inverted_idx:
                        inverted_idx[key][2] = "{} {}".format(self.posting_file[0], counter_line)
                        # print(inverted_idx[key])
                    counter_line += 1"""

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
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main():
    run_engine()
    #query = input("Please enter a query: ")
    #k = int(input("Please enter number of docs to retrieve: "))
    #posting1 = load_index("posting1")
    #posting2 = load_index("posting2")
    #merge(posting1,posting2)
    #print(dict3)

