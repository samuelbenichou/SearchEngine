import os
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import timeit


class ReadFile:
    def __init__(self, corpus_path):
        self.corpus_path = corpus_path
        self.result = []

    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets using python multi-threading
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        filenames = []
        start = timeit.default_timer()
        for root, dirs, files in os.walk(file_name):
            for file in files:
                if file.endswith(".parquet"):
                    filenames.append(file)
        pool = ThreadPool(3)
        pool.map(self.read_filenames, filenames)
        pool.close()
        pool.join()
        stop = timeit.default_timer()
        print('Time of reader: ', stop - start)
        return self.result


    def read_filenames(self,path):
        full_path = os.path.join(self.corpus_path, path)
        df = pd.read_parquet(full_path, engine="pyarrow")
        self.result.extend(df.values.tolist())



if __name__ == '__main__':
    r = ReadFile(corpus_path='/Users/samuel/Desktop/Corpus')
    r.read_file('/Users/samuel/Desktop/Corpus')

