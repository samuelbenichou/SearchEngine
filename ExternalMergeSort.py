import traceback
import os


class ExternalMergeSort:

    def __init__(self, posting_file):
        self.merge_file = []
        self.merge_counter = 1
        self.posting_file = posting_file

    def external_merge_sort(self):
        if len(self.posting_file) == 1:
            #print(1)
            self.merge_file.append(self.posting_file[0])
        elif len(self.posting_file) == 2:
            #print(2)
            self.merge_doc(self.posting_file[0], self.posting_file[1])
        else:
            #print(3)
            self.merge_doc(self.posting_file[0], self.posting_file[1])
            self.external_merge_sort()


    def merge_doc(self, posting_file1, posting_file2):# Combine content of both lists  and Write to third file
        try:
            #print("merge beetwen {} and {}".format(posting_file1, posting_file2))
            with open(posting_file1) as file1, open(posting_file2) as file2:
                with open("postingMerge{}.txt".format(self.merge_counter), 'w') as f:
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

            self.posting_file.append("postingMerge{}.txt".format(self.merge_counter))
            self.merge_counter += 1
            #print(posting_file1)
            #print(posting_file2)
            self.posting_file.remove(posting_file2)
            self.posting_file.remove(posting_file1)
            os.remove(posting_file1)
            os.remove(posting_file2)


        except Exception:
            print(traceback.print_exc())

    def connect_pointer_to_term(self, inverted_idx):
        #if len(self.posting_file) != 1:
        counter_line = 0
        with open(self.posting_file[0]) as file:
            line = file.readline()
            while line:
                #print(line.split(':')[0])
                line = file.readline()
                key = line.split(':')[0][:-1]
                if key in inverted_idx:
                    inverted_idx[key][2] = "{}.txt {}".format(self.posting_file[0], counter_line)
                    #print(inverted_idx[key])
                counter_line += 1

        print(inverted_idx)

    def get_posting_file(self):
        return self.posting_file[0]


if __name__ == '__main__':
    e = ExternalMergeSort(["posting1.txt", "posting4.txt", "posting7.txt"])
    e.external_merge_sort()
    #e.merge_doc("posting1.txt", "posting4.txt")
    #print(e.merge_counter)
    #print(e.posting_file)
    inverted = {'RT': [4, 4, ''], '@alsoto_7': [1, 1, ''], 'US': [1, 1, ''], 'right': [2, 2, ''], 'literally': [1, 1, ''], 'embarrassment': [1, 1, ''], 't.co': [2, 2, ''], '6ypNr91AS0': [1, 1, ''], '@bujaboy19': [1, 1, ''], 'actually': [1, 1, ''], 'believed': [1, 1, ''], '5G': [1, 1, ''], 'caused': [1, 1, ''], 'coronavirus': [1, 1, ''], 'forget': [1, 1, ''], '@113rii': [1, 1, ''], 'Wear': [1, 1, ''], 'yer': [1, 1, ''], 'masks': [1, 1, ''], 'Zglc8D4xws': [1, 1, ''], '@fishsports': [1, 1, ''], 'move': [1, 1, ''], 'WR': [1, 1, ''], 'Jon': [1, 1, ''], 'Vea': [1, 1, ''], 'Johnson': [1, 1, ''], 'new': [1, 1, ''], 'NFL': [1, 1, ''], 'Reserve/COVID-19': [1, 1, ''], 'list': [1, 1, ''], 'players': [1, 1, ''], 'tested': [1, 1, ''], 'positive/been': [1, 1, ''], 'close': [1, 1, ''], 'con…': [1, 1, ''], 'cowboys': [1, 1, ''], '#cowboys': [1, 1, ''], '@realDonaldTrump': [1, 1, ''], '@Yankees': [1, 1, ''], 'say': [1, 1, ''], 'Chinese': [1, 1, ''], 'virus': [1, 1, ''], 'make': [1, 1, ''], 'trouble': [1, 1, ''], 'nothing': [1, 2, ''], 'China': [1, 1, ''], 'owes': [1, 1, '']}
    e.connect_pointer_to_term(inverted)

