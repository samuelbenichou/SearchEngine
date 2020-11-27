import os
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import timeit
from multiprocessing.pool import ThreadPool



class ReadFile:
    def __init__(self, corpus_path):
        self.corpus_path = corpus_path

    def read_corpus(self, corpus_path):
        result = []
        filenames = []
        start = timeit.default_timer()
        for root, dirs, files in os.walk(corpus_path):
            for file in files:
                if file.endswith(".parquet"):
                    filenames.append(file)
        pool = ThreadPool(5)
        for file in filenames:
            result.extend(pool.map(self.read_file, file))
        #result.extend(pool.map(self.read_file, filenames))
        pool.close()
        pool.join()
        stop = timeit.default_timer()
        print('Time of reader: ', stop - start)
        return result

    def get_filenames_path(self):
        """
        This function is reading a parquet file contains several tweets
        :return:return list of documents
        """
        filenames = []
        for root, dirs, files in os.walk(self.corpus_path):
            for file in files:
                if file.endswith(".parquet"):
                    filenames.append(file)
        return filenames


    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets using python multi-threading
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        full_path = os.path.join(self.corpus_path, file_name)
        #print(full_path)
        df = pd.read_parquet(full_path, engine="pyarrow")
        value = df.values.tolist()
        #print(value)
        return value


if __name__ == '__main__':
    r = ReadFile('/Users/samuel/Desktop/Corpus')
    #print(r.read_file('/Users/samuel/Desktop/Corpus'))
    #r.read_corpus('/Users/samuel/Desktop/Corpus')
    l = r.get_filenames_path()
    print(l)
    start = timeit.default_timer()
    for dic in l:
        r.read_file(dic)
    stop = timeit.default_timer()
    print("Time   : ", stop - start)
    #r.read_file('/Users/samuel/Desktop/Data/date=08-07-2020')
    # r.read_file(l[0])


