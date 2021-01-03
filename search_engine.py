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
        df = pd.read_parquet(file, engine="pyarrow")
        value = df.values.tolist()
        corpus.extend(value)

    one_third = len(corpus)//3

    # three_split = np.array_split(corpus, 3)
    # process1_list = list(three_split[0])
    # process2_list = list(three_split[1])
    # process3_list = list(three_split[2])

    #Start 3 processes
    with concurrent.futures.ProcessPoolExecutor() as executor:
        f1 = executor.submit(process_index, corpus[:one_third], 1, corpus_path, output_path, stemming)
        f2 = executor.submit(process_index, corpus[one_third:2*one_third], 2, corpus_path, output_path, stemming)
        f3 = executor.submit(process_index, corpus[2*one_third:], 3, corpus_path, output_path, stemming)

    list_posting_file = [f1.result()[0], f2.result()[0], f3.result()[0]]
    list_inverted_index_file = [f1.result()[1], f2.result()[1], f3.result()[1]]
    list_tweet_file = [f1.result()[2], f2.result()[2], f3.result()[2]]
    number_of_tweet = f1.result()[3] + f2.result()[3] + f3.result()[3]
    print("***********")
    print(list_posting_file)
    print(list_inverted_index_file)
    print(list_tweet_file)
    print(number_of_tweet)

    p = Parse(config.toStem)

    # Merge the tweet index and load it in the memory
    utils.create_tweet_index(output_path, list_tweet_file[0], list_tweet_file[1], list_tweet_file[2])
    tweet_file = utils.load_tweet_index(output_path)

    # Merge the inverted index and load it in the memory
    utils.create_invertex_index(output_path, list_inverted_index_file[0], list_inverted_index_file[1], list_inverted_index_file[2])
    inverted_index = utils.load_my_inverted_index(output_path)

    print("Start ranker")

    if type(queries) != list:
        with open(queries, encoding="utf8") as file:
            queries = file.readlines()
    resultGloVe = pd.DataFrame(columns=['Query_num', 'Tweet_id', 'Rank'])
    #resultWOrd2Vec = pd.DataFrame(columns=['Query_num', 'Tweet_id', 'Rank'])
    counter = 1
    rank = 1
    #rank1 = 1
    for query in queries:
        if query != '\n':
            searcher = Searcher(inverted_index, output_path)
            #searcher1 = Searcher(inverted_index, output_path)
    #         query_as_list = p.parse_sentence(query)
    #         relevant_docs, term_in_query = searcher.relevant_docs_from_posting(query_as_list)
    #         #relevant_docs1, term_in_query1 = searcher1.relevant_docs_from_posting_with_word2Vec(query_as_list)
    #         ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, number_of_tweet, tweet_file, term_in_query)  # number_of_tweet
    #         #ranked_docs1 = searcher1.ranker.rank_relevant_doc(relevant_docs1, number_of_tweet, tweet_file,term_in_query1)  # number_of_tweet
    #         ranked_tweets = searcher.ranker.retrieve_top_k(ranked_docs, num_docs_to_retrieve)
    #         #ranked_tweets1 = searcher1.ranker.retrieve_top_k(ranked_docs1, num_docs_to_retrieve)
    #         # print(ranked_tweets)
    #         for doc_tuple in ranked_tweets:
    #             resultGloVe = resultGloVe.append({"Query_num":counter , "Tweet_id": doc_tuple[0], "Rank": doc_tuple[1]},ignore_index=True)
    #             #print("Tweet id: {} Score: {}".format(doc_tuple[0], doc_tuple[1]))
    #             rank += 1
    #         #print(ranked_tweets1)
    #         # for doc in ranked_tweets1:
    #         #     resultWOrd2Vec = resultWOrd2Vec.append({"Query_num":counter , "Tweet_id": doc[0], "Rank": rank1, "Score": doc[1]},ignore_index=True)
    #         #     #print("Tweet id: {} Score: {}".format(doc[0], doc[1]))
    #         #     rank1 += 1
    #         counter += 1
    # resultGloVe.to_csv(os.path.join(output_path, "results.csv"), index=False)
    #resultWOrd2Vec.to_csv(os.path.join(output_path, "resultsWord2Vec.csv"))




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
                                      num_thread, output_path)

        mergeSort.external_merge_sort()  # merge the posting file
        mergeSort.external_inverted_index_merge()  # merge the inverted index file
        mergeSort.external_tweet_index_merge()  # merge the tweet index file
        #Merge the inverted index file
        mergeSort.update_pointer()
        inverted_index_file = mergeSort.get_inverted_index_file()

        # Merge the posting file
        posting_file = mergeSort.get_posting_file()

        # Merge the tweet index file
        tweet_file = mergeSort.get_tweet_file()

        result = [posting_file, inverted_index_file, tweet_file, number_of_documents]
        stop = timeit.default_timer()
        print("Time of indexer and posting of process {} : ".format(num_thread), stop - start)
        return result
    except:
        print(traceback.print_exc())



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

    return dict[0]



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
    queries = [
        'Dr. Anthony Fauci wrote in a 2005 paper published in Virology Journal that hydroxychloroquine was effective in treating SARS.',
        'The seasonal flu kills more people every year in the U.S. than COVID-19 has to date.',
        'Coronavirus is less dangerous than the flu',
        'The coronavirus pandemic is a cover for a plan to implant trackable microchips and that the Microsoft co-founder Bill Gates is behind it',
        'Microsoft co-founder Bill Gates said "only the people who have all the vaccines will still be able to move freely".',
        'Bill Gates owns the patent and vaccine for coronavirus.',
        'Herd immunity has been reached.',
        'Children are “almost immune from this disease.”',
        'A study from the CDC and the WHO “proves face masks do not prevent the spread of a virus.”',
        'hydroxychloroquine, zinc, and Zithromax can cure coronavirus',
        'U.S. has “one of the lowest mortality rates in the world” from COVID-19',
        'The spread of COVID-19 will slow down as the weather warms up',
        '5G helps the spread of Covid-19',
        'Injecting or consuming bleach or disinfectant can cure coronavirus',
        'The COVID-19 pandemic was planned by the Rockefeller Foundation in "Operation Lockstep."',
        'COVID-19 could lose its epidemic status in the United States because of declining coronavirus death rates according to CDC data.',
        'healthy people should NOT wear masks',
        'coronavirus is a bioweapon created in a lab in Wuhan',
        'The outbreak began because people ate bat soup',
        'Outbreak people ate bat',
        'coronavirus eat bat soup ',
        'Wearing a mask to prevent the spread of COVID-19 is unnecessary because the disease can also be spread via farts.',
        'For younger people, seasonal flu is “in many cases” a deadlier virus than COVID-19. ',
        'The coronavirus disease (COVID-19) is caused by a virus',
        'Covid-19 is not caused by bacteria',
        'The prolonged use of medical masks when properly worn, DOES NOT cause CO2 intoxication nor oxygen deficiency',
        "Masks don't cause CO2 intoxication.",
        'The COVID-19 coronavirus pandemic caused a nationwide shortage of U.S. coins in circulation during the summer of 2020.',
        'Coins shortage due to coronavirus',
        'People should NOT wear masks while exercising']
    run_engine('/Users/samuel/Desktop/Corpus','/Users/samuel/Desktop/Corpus', False, queries, 20)
    # shutil.move("/Users/samuel/Desktop/Test/posting_file1.txt", "/Users/samuel/Desktop/Corpus/posting_file1.txt")
    # os.replace("/Users/samuel/Desktop/Test/posting_file1.txt", "/Users/samuel/Desktop/Corpus/posting_file1.txt")
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




