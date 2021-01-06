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
    docs = r.read_file('benchmark_data_train.snappy.parquet')
    for doc in docs:
        if doc[0] in ['1284696357039747073', '1285703407353892865', '1287435315662266371', '1291170311602413574', '1284110077012541440', '1291807809688961025', '1291112181468139521', '1286280212943118343', '1284920404302274561', '1280961319290667009']:
            print("{} : {}".format(doc[0],doc[2]))