import traceback
import os
import json


class ExternalMergeSort:

    def __init__(self, posting_file, inverted_idx_file, tweet_index_file, num_thread):
        self.merge_posting_file = []
        self.merge_inverted_index = []
        self.merge_tweet_index = []
        self.merge_counter = 1
        self.posting_file = posting_file
        self.inverted_idx_file = inverted_idx_file
        self.tweet_index_file = tweet_index_file
        self.num_posting = num_thread
        self.num_inverted_index = num_thread
        self.num_tweet_index = num_thread
        self.num = num_thread

    def external_tweet_index_merge(self):
        #print(self.tweet_index_file)
        if len(self.tweet_index_file) == 1:
            # print(1)
            self.merge_tweet_index.append(self.tweet_index_file[0])
        elif len(self.tweet_index_file) == 2:
            # print(2)
            self.merge_tweet_index_doc(self.tweet_index_file[0], self.tweet_index_file[1])
        elif len(self.tweet_index_file) > 2:
            # print(3)
            self.merge_tweet_index_doc(self.tweet_index_file[0], self.tweet_index_file[1])
            self.external_tweet_index_merge()

    def merge_tweet_index_doc(self, tweet_index1, tweet_index2):
        try:
            with open(tweet_index1, encoding="cp437", errors='ignore') as file1, open(tweet_index2, encoding="cp437",errors='ignore') as file2:
                dic1 = json.load(file1)
                dic2 = json.load(file2)
                dic1.update(dic2)
                with open("tweet_index_Merge{}.json".format(self.num_tweet_index), 'w', encoding="cp437", errors='ignore') as f:
                    json.dump(dic1, f)

            self.tweet_index_file.append("tweet_index_Merge{}.json".format(self.num_tweet_index))
            self.num_tweet_index += 3
            self.tweet_index_file.remove(tweet_index1)
            self.tweet_index_file.remove(tweet_index2)
            os.remove(tweet_index1)
            os.remove(tweet_index2)

        except Exception:
            print(traceback.print_exc())

    def external_inverted_index_merge(self):
        #print(self.inverted_idx_file)
        if len(self.inverted_idx_file) == 1:
            #print(1)
            self.merge_inverted_index.append(self.inverted_idx_file[0])
        elif len(self.inverted_idx_file) == 2:
            #print(2)
            self.merge_inverted_index_doc(self.inverted_idx_file[0], self.inverted_idx_file[1])
        elif len(self.inverted_idx_file) > 2:
            #print(3)
            self.merge_inverted_index_doc(self.inverted_idx_file[0], self.inverted_idx_file[1])
            self.external_inverted_index_merge()

    def merge_inverted_index_doc(self, inverted_index1, inverted_index2):
        try:
            with open(inverted_index1, encoding="cp437", errors='ignore') as file1, open(inverted_index2, encoding="cp437",errors='ignore') as file2:
                dic1 = json.load(file1)
                dic2 = json.load(file2)
                for key, value in dic2.items():
                    if key in dic1:
                        dic1[key][0] += value[0]
                        dic1[key][1] += value[1]
                        # dic1[key].append(value[2])
                    else:
                        dic1[key] = value
                with open("inverted_index_Merge{}.json".format(self.num_inverted_index), 'w', encoding="cp437", errors='ignore') as f:
                    json.dump(dic1, f)

            self.inverted_idx_file.append("inverted_index_Merge{}.json".format(self.num_inverted_index))
            self.num_inverted_index += 3
            self.inverted_idx_file.remove(inverted_index1)
            self.inverted_idx_file.remove(inverted_index2)
            os.remove(inverted_index1)
            os.remove(inverted_index2)

        except Exception:
            print(traceback.print_exc())

    def external_merge_sort(self):
        #print(self.posting_file)
        if len(self.posting_file) == 1:
            #print(1)
            self.merge_posting_file.append(self.posting_file[0])
        elif len(self.posting_file) == 2:
            #print(2)
            self.merge_doc(self.posting_file[0], self.posting_file[1])
        elif len(self.posting_file) > 2:
            #print(3)
            self.merge_doc(self.posting_file[0], self.posting_file[1])
            self.external_merge_sort()


    def merge_doc(self, posting_file1, posting_file2):# Combine content of both lists  and Write to third file
        try:
            #print("merge beetwen {} and {}".format(posting_file1, posting_file2))
            with open(posting_file1, encoding="cp437", errors='ignore') as file1, open(posting_file2, encoding="cp437",errors='ignore') as file2:
                with open("postingMerge{}.txt".format(self.num_posting), 'w', encoding="cp437", errors='ignore') as f:
                    line1 = file1.readline()
                    line2 = file2.readline()
                    while line1 or line2:
                        if not line1:
                            f.write("{}:{}".format(line2.split(':')[0], line2.split(':')[1]))
                            #print(line2.split(':')[0])
                            #print(line2.split(':')[1])
                            line2 = file2.readline()
                        elif not line2:
                            f.write("{}:{}".format(line1.split(':')[0], line1.split(':')[1]))
                            #print(line1.split(':')[0])
                            #print(line1.split(':')[1])
                            line1 = file1.readline()
                        elif line1.split(':')[0] < line2.split(':')[0]:
                            f.write("{}:{}".format(line1.split(':')[0], line1.split(':')[1]))
                            #print(line1.split(':')[0])
                            #print(line1.split(':')[1])
                            line1 = file1.readline()
                        elif line1.split(':')[0] > line2.split(':')[0]:
                            f.write("{}:{}".format(line2.split(':')[0], line2.split(':')[1]))
                            #print(line2.split(':')[0])
                            #print(line2.split(':')[1])
                            line2 = file2.readline()
                        else: # Merge the same term
                            f.write("{}: [{}, {}] \n".format(line1.split(':')[0], line1.split(':')[1][2:len(line1.split(':')[1])-3], line2.split(':')[1][2:len(line2.split(':')[1])-3]))
                            #print(line1.split(':')[0])
                            #print(line1.split(':')[1][2:len(line1.split(':')[1])-3])
                            #print(line2.split(':')[1][2:len(line2.split(':')[1])-3])
                            line1 = file1.readline()
                            line2 = file2.readline()

            self.posting_file.append("postingMerge{}.txt".format(self.num_posting))
            self.merge_counter += 1
            self.num_posting += 3
            #print(posting_file1)
            #print(posting_file2)
            self.posting_file.remove(posting_file2)
            self.posting_file.remove(posting_file1)
            os.remove(posting_file1)
            os.remove(posting_file2)
            #print(self.posting_file)


        except Exception:
            print(traceback.print_exc())

    def connect_pointer_to_term(self, inverted_idx):
        #if len(self.posting_file) != 1:
        counter_line = 0
        with open(self.posting_file[0], encoding='cp437') as file:
            line = file.readline()
            while line:
                #print(line.split(':')[0])
                line = file.readline()
                key = line.split(':')[0][:-1]
                if key in inverted_idx:
                    inverted_idx[key][2] = "{} {}".format(self.posting_file[0], counter_line)
                    #print(inverted_idx[key])
                counter_line += 1
        return inverted_idx
        #print(inverted_idx)

    def get_posting_file(self):
        return self.posting_file[0]

    def get_inverted_index_file(self):
        return self.inverted_idx_file[0]

    def get_tweet_file(self):
        return self.tweet_index_file[0]

    def update_pointer(self):
        # if len(self.inverted_idx_file) == 0:
        #     return
        to_remove = self.inverted_idx_file[0]
        with open(self.inverted_idx_file[0], encoding="cp437", errors='ignore') as file1:
            dic1 = json.load(file1)
        update_dic = self.connect_pointer_to_term(dic1)
        with open("inverted_index_pointer{}.json".format(self.num), 'w', encoding="cp437", errors='ignore') as f:
            json.dump(update_dic, f)
        self.inverted_idx_file.append("inverted_index_pointer{}.json".format(self.num))
        self.inverted_idx_file.remove(to_remove)
        os.remove(to_remove)

