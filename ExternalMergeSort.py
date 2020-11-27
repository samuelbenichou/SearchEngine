import traceback
import os


class ExternalMergeSort:

    def __init__(self, posting_file, num_thread):
        self.merge_file = []
        self.merge_counter = 1
        self.posting_file = posting_file
        self.num_thread = num_thread

    def external_merge_sort(self):
        print(self.posting_file)
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
                with open("postingMerge{}.txt".format(self.num_thread), 'w') as f:
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

            self.posting_file.append("postingMerge{}.txt".format(self.num_thread))
            self.merge_counter += 1
            self.num_thread += 3
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
        with open(self.posting_file[0]) as file:
            line = file.readline()
            while line:
                #print(line.split(':')[0])
                line = file.readline()
                key = line.split(':')[0][:-1]
                if key in inverted_idx:
                    inverted_idx[key][2] = "{} {}".format(self.posting_file[0], counter_line)
                    #print(inverted_idx[key])
                counter_line += 1

        #print(inverted_idx)

    def get_posting_file(self):
        return self.posting_file[0]

