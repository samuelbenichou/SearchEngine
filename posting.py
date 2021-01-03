import traceback

class Posting:

    def __init__(self, postingDict, num_posting_file):
        self.postingDict = postingDict
        self.path = 'posting{}.txt'.format(num_posting_file)
        self.num_posting_file = num_posting_file
        self.merge_file = []
        self.merge_counter = 0

    def get_posting_path(self):
        return self.path

    def create_posting_file(self):
        try:
            with open(self.path, 'a', encoding="cp437",errors='ignore') as f:
                for key, value in sorted(self.postingDict.items()):
                    f.write("{} : {} \n".format(str(key), str(value)))
        except:
            print('problem with the following key sammm {}'.format(key))
            print(traceback.print_exc())

    def sort_by_id(self, value):
        return value[0]

