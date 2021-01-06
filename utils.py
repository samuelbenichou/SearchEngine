import pickle
import json
import traceback
import os


def save_obj(obj, name):
    """
    This function save an object as a pickle.
    :param obj: object to save
    :param name: name of the pickle file.
    :return: -
    """
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name):
    """
    This function will load a pickle file
    :param name: name of the pickle file
    :return: loaded pickle file
    """
    with open(name, 'rb') as f:
        return pickle.load(f)


def load_inverted_index(output_path=None):
    with open(os.path.join(output_path, "inverted_index.json"), encoding="cp437", errors='ignore') as file1:
        inverted_index = json.load(file1)
        result = {}
        for term in inverted_index.keys():
            result[term] = inverted_index[term][0]
        return result

def load_my_inverted_index(output_path=None):
    with open(os.path.join(output_path, "inverted_index.json"), encoding="cp437", errors='ignore') as file1:
        return json.load(file1)



def create_invertex_index(output_path, file1, file2, file3):
    try:
        with open(file1, encoding="cp437", errors='ignore') as f1, open(file2, encoding="cp437",errors='ignore') as f2, open(file3, encoding="cp437",errors='ignore') as f3:
            inverted_index = json.load(f1)
            dic1 = json.load(f2)
            dic2 = json.load(f3)
            for key, value in dic1.items():
                if key in inverted_index:
                    inverted_index[key][0] += value[0]
                    inverted_index[key][1] += value[1]
                    inverted_index[key].append(value[2])
                else:
                    inverted_index[key] = value

            for key, value in dic2.items():
                if key in inverted_index:
                    inverted_index[key][0] += value[0]
                    inverted_index[key][1] += value[1]
                    inverted_index[key].append(value[2])
                else:
                    inverted_index[key] = value

        with open(os.path.join(output_path, "inverted_index.json"), "w", encoding="cp437", errors='ignore') as fp:
            json.dump(inverted_index, fp)

        os.remove(file1)
        os.remove(file2)
        os.remove(file3)
    except:
        print(traceback.print_exc())

def create_tweet_index(output_path, file1, file2, file3):
    try:
        with open(file1, encoding="cp437", errors='ignore') as f1, open(file2, encoding="cp437",errors='ignore') as f2, open(file3, encoding="cp437",errors='ignore') as f3:
            dic1 = json.load(f1)
            dic2 = json.load(f2)
            dic3 = json.load(f3)
            tweet_index = {**dic1, **dic2, **dic3}

        with open(os.path.join(output_path, "tweet_index.json"), "w", encoding="cp437", errors='ignore') as fp:
            json.dump(tweet_index, fp)

        os.remove(file1)
        os.remove(file2)
        os.remove(file3)
    except:
        print(traceback.print_exc())

def load_tweet_index(output_path):
    with open(os.path.join(output_path, "tweet_index.json"), encoding="cp437", errors='ignore') as file1:
        return json.load(file1)



if __name__ == '__main__':
    # create_invertex_index('/Users/samuel/Desktop/Test','inverted_index_pointer1.json', 'inverted_index_pointer2.json', 'inverted_index_pointer3.json')
    # p = load_inverted_index('/Users/samuel/Desktop/Test')
    # print(p['covid'])
    p = load_inverted_index('/Users/samuel/Desktop/Search Engine/posting')
    print(p['#Asivikelane'])