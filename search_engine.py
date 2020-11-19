from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import concurrent.futures
import timeit


def run_engine():
    config = ConfigClass()
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse()
    indexer = Indexer(config)
    start = timeit.default_timer()

    #documents_list = r.read_file(file_name='sample3.parquet')
    documents_list = r.read_file(file_name='~/Desktop/Corpus')
    print("Number of tweets in the corpus {}".format(len(documents_list)))

    stop = timeit.default_timer()
    print('Time of reader: ', stop - start)

    middle_index = len(documents_list) // 2

    """with concurrent.futures.ThreadPoolExecutor() as executor:
        f1 = executor.submit(parse_index_doc, documents_list[:middle_index])
        f2 = executor.submit(parse_index_doc, documents_list[middle_index:])

    stop = timeit.default_timer()
    print('Time of parser: ', stop - start)

    utils.save_obj(f1.result(), "posting1")
    utils.save_obj(f2.result(), "posting2")"""
    #print(f1.result())
    """print("******************************************************************")
    print("******************************************************************")
    print("******************************************************************")
    print("******************************************************************")
    print("******************************************************************")"""
    #print(f2.result())


    #print(documents_list)

    # Iterate over every document in the file
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        #number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)
    #print(indexer.inverted_idx)
    #print(indexer.postingDict)
    #print(indexer.get_tweet_info('1280966306292998145'))
    #print(indexer.get_term_info("Donald Trump"))
    #print('Finished parsing and indexing. Starting to export files')

    #utils.save_obj(indexer.inverted_idx, "inverted_idx")
    #utils.save_obj(indexer.postingDict, "posting")

def parse_index_doc(documents_list):
    config = ConfigClass()
    p = Parse()
    indexer = Indexer(config)
    print("Number of tweets in the thread {}".format(len(documents_list)))

    # Iterate over every document in the file
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        # index the document data
        indexer.add_new_doc(parsed_document)
    # print(indexer.inverted_idx)
    # print(indexer.postingDict)

    print('Finished parsing and indexing of thread. Starting to export files')
    return indexer.postingDict

def merge(*dict):
    result_dict = {}
    for d in dict:
        for k, v in d.items():
            result_dict.setdefault(k, []).extend(v)
    #print(result_dict)
    utils.save_obj(result_dict, "posting3")

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
    """for doc_tuple in search_and_rank_query(query, inverted_index, k):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
        """
