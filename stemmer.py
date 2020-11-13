from nltk.stem import snowball
from nltk.stem import PorterStemmer


class Stemmer:
    def __init__(self):
        self.stemmer = snowball.SnowballStemmer("english")

    def stem_tweet(self , tweet_list):
        stemmed_words = [self.stemmer.stem(item) for item in tweet_list]
        # print(stemmed_words)
        return stemmed_words

    def stem_term(self, token):
        """
        This function stem a token
        :param token: string of a token
        :return: stemmed token
        """
        return self.stemmer.stem(token)

if __name__ == '__main__':
    s = Stemmer()
    s.stem_tweet(['Indoor', 'malls', 'set', 'reopen', 'coronavirus', 'shutdown', 'Friday', 'Long', 'Island', 'proper', 'air', 'filtration', 'systems', 'installed', 'Gov', 'Andrew', 'Cuomo', 'announced', 'Wednesday', 't.co', 'Zh1zYiH5GY'])
    s.stem_term('malls')