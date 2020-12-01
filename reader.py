import os
import pandas as pd
import timeit
from tqdm import tqdm


class ReadFile:
    def __init__(self, corpus_path):
        self.corpus_path = corpus_path


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

        # pathes = []
        # for filename in os.listdir(self.corpus_path):
        #     if filename.endswith(".parquet"):
        #         pathes.append([self.corpus_path + "\\", filename])
        #     elif os.path.isdir(self.corpus_path + '\\' + filename):
        #         for filenameParquet in os.listdir(self.corpus_path + '\\' + filename):
        #             if filenameParquet.endswith(".parquet"):
        #                 temp_folder_path = self.corpus_path
        #                 self.corpus_path = self.corpus_path + '\\' + filename
        #                 pathes.append([self.corpus_path, filenameParquet])
        #                 self.corpus_path = temp_folder_path
        #     else:
        #         continue
        # return pathes

        # all_docs = []
        #
        # for dir, subdirs, files in os.walk(self.corpus_path):
        #     if subdirs:
        #         for subdir in subdirs:
        #             for d, dirs, subfiles in os.walk(os.path.join(dir, subdir)):
        #                 for file in subfiles:
        #                     if file.endswith(".parquet"):
        #                         all_docs.append(os.path.join(subdir, file))



        #return all_docs


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
    # '1287558160618725377', 'Mon Jul 27 01:19:21 +0000 2020', 'RT @ASlavitt: Florida just passed NY in total COVID cases.
    from parser_module import Parse
    r = ReadFile('/Users/samuel/Desktop/Test')
    b = r.get_filenames_path()
    print(r.get_filenames_path())
    #r.read_file('a/covid19_07-27.snappy.parquet')

    val = r.read_file(b[0])
    p =Parse(False)
    for v in val:
        print(p.parse_sentence(v[2]))
    # for v in val:
    #     if v[0] in ['1288834760685412352', '1288831744263753728', '1288845739532189697', '1288846785482559494', '1288848123452768258', '1288832416153444352', '1288849095621255168', '1288831879970459648', '1288836639184347137', '1288842214848815105', '1288830107445006336', '1288832911060295680', '1288847910856056832', '1288847931974594560', '1288841919854833670', '1288838316557832192', '1288839899043508225', '1288843859598946304', '1288837542289321995', '1288834531261329408']:
    #
    #         print(v[2] )

    # print(len(['1287581036789014528', '1287538709706035201', '1287598542975127552', '1287560180851335171', '1287559792207175681', '1287582995902279680', '1287550787745325056', '1287546260694749187', '1287561707104227330', '1287541275902836736', '1287580351544438792', '1287762016975892480', '1287539361001529344', '1287570189572354048', '1287563465939353600', '1287570689097117701', '1287544256220008448', '1287568976298553344', '1287548277697794048', '1287554340807258115', '1287546415338541056', '1287571184289067009', '1287558371818668032', '1287555440021184514', '1287547261745090560', '1287560572280733696', '1287765121884811264', '1287596597317181441', '1287549626061660160', '1287580345957855237', '1287565921511444482', '1287540509410578432', '1287586294131032069', '1287589284740886530', '1287564037933469696', '1287540605699186694', '1287581847786885120', '1287547480322842624', '1287547746015014912', '1287757520249868289', '1287549346431619073', '1287552460572127233', '1287555748231180293', '1287559235325145089', '1287540903435833344', '1287577185289506816', '1287560886245298177', '1287543392763772928', '1287543704534896641', '1287586457125892096', '1287595378595049472', '1287549236503089152', '1287579887717568512', '1287576364934144001', '1287544459471794176', '1287545953327689728', '1287539205212639236', '1287562851708628992', '1287540945953538055', '1287586790174420992', '1287582260015898629', '1287563349128097792', '1287559970121277440', '1287571323279966208', '1287555150148599819', '1287553875809984516', '1287578006442188800', '1287598728891764737', '1287541880042004486', '1287539137403334661', '1287549339058032640', '1287545950643220480', '1287546091819294721', '1287545228631605257', '1287544821947736064', '1287557364422328321', '1287538942359871488', '1287543328893022208', '1287567280105238528', '1287570727093317632', '1287580839161798656', '1287584689310642178', '1287562158256074752', '1287557699463294979', '1287548445738307584', '1287576223841845250', '1287540736347590658', '1287570228289757185', '1287578028730494976', '1287754807806234624', '1287556863299473415', '1287562596736995330', '1287574633454002177', '1287558160618725377', '1287570484385783816', '1287544898812600322', '1287587840524771328', '1287761900768571392', '1288843917044191233', '1287563583946207233']))
    # #print(r.read_file('/Users/samuel/Desktop/Corpus'))
    # #r.read_corpus('/Users/samuel/Desktop/Corpus')
    #
    # #r.read_file('/Users/samuel/Desktop/Data/date=08-07-2020')
    # # r.read_file(l[0])
    #['1280966293403963400', '1280966303885545487', '1280966310432854019', '1280966327268769799', '1280966336794054658']
    #['1288830875082268672', '1288833512490045441', '1288833942188154881', '1288836783153664001', '1288844367944282112', '1288848829509509124', '1288849415164264449', '1288849116584280073', '1288831577976221696', '1288839214432309250', '1288832099135500290', '1288831346975150082', '1288845342532935680', '1288844572190298115', '1288837665929015299', '1288838579712602114', '1288831500171894784', '1288830664301633538', '1288845709664387073', '1288834529038233600']:

    # [('1280966319764934656', 0.12143410157729917), ('1280966290417618944', 0.09107557618297438), ('1280966296470007816', 0.09107557618297438), ('1280966300735614978', 0.09107557618297438), ('1280966334222864385', 0.09053616863856032), ('1280966306423070720', 0.08095606771819944), ('1280966322931851265', 0.08095606771819944), ('1280966302320996352', 0.06623678267852681), ('1280966329894416385', 0.06623678267852681), ('1280966331312099328', 0.06623678267852681), ('1280966305466789894', 0.060717050788649585), ('1280966322336280578', 0.060717050788649585), ('1280966333014904832', 0.060717050788649585), ('1280966336143863810', 0.060717050788649585), ('1280966307266072582', 0.060357445759040215), ('1280966289159335937', 0.05604650842029192), ('1280966291554271232', 0.05604650842029192), ('1280966319328894977', 0.05604650842029192), ('1280966325574291458', 0.05604650842029192), ('1280966331731341312', 0.05604650842029192)]
