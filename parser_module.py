from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
        print(text_tokens)
        text_hashtags = self.parse_hashtags(text_tokens)
        text_tokens.extend(text_hashtags)
        #print(text_hashtags)
        self.stop_words.extend(["#", '.'])
        #text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        if "@" in text_tokens:
            self.parse_at(text_tokens)
        text_tokens_without_stopwords = [w for w in text_tokens if w.lower() not in self.stop_words]
        print(text_tokens_without_stopwords)
        return text_tokens_without_stopwords

    def parse_percentage(self, text):
        for i, term in enumerate(text):
            if (text[i] == "%" or text[i] == "percent" or text[i] == "percentage") and (text[i-1].isnumeric() or text[i-1].replace('.', '', 1).isdigit()):
                text[i] = "{}%".format(''.join(text[i - 1]))
                del text[i-1]
        print(text)

    def parse_hashtags(self, text):
        """
        This function takes a tweet document list and parse the different hashtags
        :param tweet document list: list re-preseting the tweet.
        :return: List the of different hashtags.
        """
        hashtags_list = [text[w+1] for w in range(len(text)) if text[w] == "#"]
        broken_hashtags = []
        for term_to_parse in hashtags_list:
            if len(term_to_parse.split('_')) > 1:
                broken_hashtags.extend(term_to_parse.split('_'))
            elif 1 < len((''.join(' ' + x if 'A' <= x <= 'Z' else x for x in term_to_parse)).split(" ")) < len(term_to_parse):
                broken_hashtags.extend((''.join(' ' + x if 'A' <= x <= 'Z' else x for x in term_to_parse)).split(" "))
            else:
                broken_hashtags.append(term_to_parse)
            text.remove(term_to_parse)
        return broken_hashtags

    def parse_URL(self ,url):
        url_encoding_special_characters = ["$", "&", "+", ",", "/", ":", ";", "=", "?", "@", "%", "#", "//"]
        url_list = re.split('[/:&=@;%#?+-]', url[0])
        url_list = [e for e in url_list if e not in ('')]
        if "www" in url_list[1]:
            url_list.append("www")
            url_list.append(url_list[1][4:])
            del url_list[1]

        return url_list

    def parse_at(self, text): # Assume there is a @
        for i,term in enumerate(text):
            if text[i] == "@":
                text[i] = "@{}".format(''.join(text[i+1]))
                del text[i+1]
        print(text)


    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}
        tokenized_text = self.parse_sentence(full_text)

        url = self.parse_URL(url)

        doc_length = len(tokenized_text)  # after text operations(without stopword).

        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        """print(full_text)
        print(tokenized_text)"""
        print(term_dict)
        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document


if __name__ == '__main__':
    #print("Samuel")
    p = Parse()
    l = ['1280947322000531456', 'Wed Jul 08 19:30:15 +0000 2020', '@MrStache9 The idiot can money and the media worries about Scheer with no mask. Laser focus guys well done. #GOAT', '[]', '', None, '', None]
    #p.parse_hashtags(['@', 'samuel', 'futur', 'ballon', "d'or", '#', "stayAtHome", "#", "stay_at_home", '#', "goat"])
    #p.parse_hashtags(['#', "stayAtHome", "#", "stay_at_home" , '#',"goat" ])
    #p.parse_doc(l)
    n = '@MrStache9 The idiot can money and the media worries about Scheer with no mask 10.6 percentage. #GOAT'
    #p.parse_sentence(n)
    p.parse_percentage(['@MrStache9', 'The', 'idiot', 'can', 'money', 'and', 'the', 'media', 'worries', 'about', 'Scheer', 'with', 'no', 'mask', '55', 'percentage', '.', '#', 'GOAT'])
    #p.parse_doc(l)
    #p.parse_at(['rt', '@', 'kylekulinski', ':', 'japan', 'less', '1,000', 'covid', 'deaths', 'never', 'went', 'full', 'economic', 'shut', 'unemployment', 'rate', 'of…'])
    #p.parse_URL(["https://www.stackoverflow.com/questions/8247792/python-how-to-cut-a-string-in-python"])