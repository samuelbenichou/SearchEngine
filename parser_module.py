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
        self.parse_percentage(text_tokens)
        text_tokens_without_stopwords = [w for w in text_tokens if w.lower() not in self.stop_words]
        print(text_tokens_without_stopwords)
        return text_tokens_without_stopwords

    def parse_percentage(self, text):
        """
        This function takes a tweet document list and parse the percentage
        :param text:
        :return:
        """
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
        """
        This function takes a tweet document list and parse the URL
        :param url:
        :return:
        """
        url_encoding_special_characters = ["$", "&", "+", ",", "/", ":", ";", "=", "?", "@", "%", "#", "//"]
        url_list = re.split('[/:&=@;%#?+-]', url[0])
        url_list = [e for e in url_list if e not in ('')]
        if "www" in url_list[1]:
            url_list.append("www")
            url_list.append(url_list[1][4:])
            del url_list[1]

        return url_list

    def parse_at(self, text): # Assume there is a @
        """
        This function takes a tweet document list and parse the @
        :param url:
        :return:
        """
        for i,term in enumerate(text):
            if text[i] == "@":
                text[i] = "@{}".format(''.join(text[i+1]))
                del text[i+1]
        print(text)

    def parse_number(self, text):
        """
        This function takes a tweet document list and parse the numbers
        :param text:
        :return:
        """
        for i, term in enumerate(text):
            if self.is_int(term.replace(',','')) or self.is_float(term.replace(',','')):
                converted_number = self.convert_to_num(term.replace(',',''))
                print(converted_number)
                if (text[i + 1] == "Thousand") or (1000 <= converted_number < 1000000):
                    text[i] = "{}K".format(''.join(text[i])) if text[i + 1] == "Thousand" else "{}K".format("%.3f" % (converted_number/1000))
                    del text[i + 1]
                elif (text[i + 1] == "Million") or (1000000 <= converted_number < 1000000000):
                    text[i] = "{}M".format(''.join(text[i])) if text[i + 1] == "Million" else "{}M".format("%.3f" % (converted_number/1000000))
                    del text[i + 1]
                elif (text[i + 1] == "Billion") or (1000000000 <= converted_number):
                    text[i] = "{}B".format(''.join(text[i])) if text[i + 1] == "Billion" else "{}B".format("%.3f" % (converted_number/1000000000))
                    del text[i + 1]
                elif converted_number < 1000 and self.check_if_term_is_fraction(text[i + 1]) and not self.is_float(term):
                    text[i] = term + " {}".format(''.join(text[i+1]))
                    del text[i + 1]

        print(text)

    def check_if_term_is_fraction(self, term):
        values = term.split('/')
        return len(values) == 2 and all(i.isdigit() for i in values)

    def is_float(self, value):
        try:
            float(value)
            return True
        except:
            return False

    def is_int(self, value):
        try:
            int(value)
            return True
        except:
            return False

    def convert_to_num(self, value):
        try:
            return int(value)
        except ValueError:
            return float(value)

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
    n = '@MrStache9 The idiot can money 35 3/4 and the media worries about Scheer with no mask 10.6 percentage. #GOAT'
    #p.parse_sentence(n)
    #p.parse_percentage(['@MrStache9', 'The', 'idiot', 'can', 'money', 'and', 'the', 'media', 'worries', 'about', 'Scheer', 'with', 'no', 'mask', '55', 'percentage', '.', '#', 'GOAT'])
    #p.parse_doc(l)
    #p.parse_at(['rt', '@', 'kylekulinski', ':', 'japan', 'less', '1,000', 'covid', 'deaths', 'never', 'went', 'full', 'economic', 'shut', 'unemployment', 'rate', 'of…'])
    #p.parse_URL(["https://www.stackoverflow.com/questions/8247792/python-how-to-cut-a-string-in-python"])
    #p.parse_number(['@MrStache9', 'The', 'idiot', 'can', 'money', 'and', 'the', 'media', 'worries', 'about', 'Scheer', 'with', 'no', 'mask', '10012,3', 'dollar', '.', '#', 'GOAT'])
    #p.parse_number(['10,123,000,000', 'pp', '.', '#', 'GOAT'])
    p.parse_number(['@MrStache9', 'The', 'idiot', 'can', 'money', '35.5', '3/4', 'and', 'the', 'media', 'worries', 'about', 'Scheer', 'with', 'no', 'mask', '10.6%', '.', '#', 'GOAT'])

