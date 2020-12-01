import pickle
import json
import traceback


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
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)


def load_inverted_index(path):
    with open("path/invertedIndex.json", encoding="cp437", errors='ignore') as file1:
        return json.load(file1)


def create_invertex_index(path):
    try:
        with open("invertedIndex.json", "w", encoding="cp437", errors='ignore') as fp:
            #json.dump(self.tweet_index, fp)
            print()
    except:
        print(traceback.print_exc())
