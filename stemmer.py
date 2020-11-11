from nltk.stem import snowball
from nltk.stem import PorterStemmer


class Stemmer:
    def __init__(self):
        self.stemmer = snowball.SnowballStemmer("english")

    def stem_term(self, token):
        stemmed_words = [self.stemmer.stem(item) for item in token]
        #print(stemmed_words)
        return stemmed_words

if __name__ == '__main__':
    s = Stemmer()
    s.stem_term(['Indoor', 'malls', 'set', 'reopen', 'coronavirus', 'shutdown', 'Friday', 'Long', 'Island', 'proper', 'air', 'filtration', 'systems', 'installed', 'Gov', 'Andrew', 'Cuomo', 'announced', 'Wednesday', 't.co', 'Zh1zYiH5GY'])