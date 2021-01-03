import os
import pandas as pd
import timeit
from tqdm import tqdm
import glob


class ReadFile:
    def __init__(self, corpus_path):
        self.corpus_path = corpus_path


    def get_filenames_path(self):
        """
        This function is reading a parquet file contains several tweets
        :return:return list of documents
        """
        result = [os.path.join(dp, f) for dp, dn, filenames in os.walk(self.corpus_path) for f in filenames if os.path.splitext(f)[1] == '.parquet']
        return result



    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets using python multi-threading
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        full_path = os.path.join(self.corpus_path, file_name)
        df = pd.read_parquet(full_path, engine="pyarrow")
        value = df.values.tolist()
        return value




if __name__ == '__main__':
    r = ReadFile('/Users/samuel/Desktop/Corpus/test')
    docs = r.read_file('sample12.parquet')
    for doc in docs:
        if doc[0] in ['1280966290254049281', '1280966305559064577', '1280966294821408768', '1280966314564206592', '1280966323019714560']:
            print("{} : {}".format(doc[0],doc[2]))