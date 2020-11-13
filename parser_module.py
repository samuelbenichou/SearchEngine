from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re
from stemmer import Stemmer
from nltk import ne_chunk, pos_tag
from nltk.tree import Tree


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')
        self.corona_words = ["COVID-19", "covid", "coronavirus"]

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        #print(text)
        text_tokens = word_tokenize(text)
        if "@" in text_tokens:
            self.parse_at(text_tokens)
        self.get_continuous_chunks(text_tokens)
        text_hashtags = self.parse_hashtags(text_tokens)
        text_tokens.extend(text_hashtags)
        self.stop_words.extend(["#", '.',':','’',"'s",'?',',', 'https','”','“','...',"''",'!','•', '(', ')','',"n't"])
        # if "@" in text_tokens:
        #     self.parse_at(text_tokens)
        self.parse_URL(text_tokens)
        self.parse_number(text_tokens)
        self.parse_percentage(text_tokens)
        text_tokens_without_stopwords = [w for w in text_tokens if w.lower() not in self.stop_words]
        #print(text_tokens_without_stopwords)
        return text_tokens_without_stopwords

    def get_continuous_chunks(self, text):
        chunked = ne_chunk(pos_tag(text))
        prev = None
        continuous_chunk = []
        current_chunk = []

        for i in chunked:
            if type(i) == Tree:
                current_chunk.append(" ".join([token for token, pos in i.leaves()]))
            elif current_chunk:
                named_entity = " ".join(current_chunk)
                if named_entity not in continuous_chunk:
                    continuous_chunk.append(named_entity)
                    current_chunk = []
            else:
                continue

        if continuous_chunk:
            named_entity = " ".join(current_chunk)
            if named_entity not in continuous_chunk and named_entity != '':
                continuous_chunk.append(named_entity)

        for named_entity_recognition in continuous_chunk:
            if named_entity_recognition not in text:
                text.append(named_entity_recognition)

    def parse_percentage(self, text):
        """
        This function takes a tweet document list and parse the percentage symbol
        :param text: List of term
        :return:
        """
        for i, term in enumerate(text):
            if (text[i] == "%" or text[i] == "percent" or text[i] == "percentage") and (text[i-1].isnumeric() or text[i-1].replace('.', '', 1).isdigit()):
                text[i] = "{}%".format(''.join(text[i - 1]))
                del text[i-1]
        #print(text)

    def parse_hashtags(self, text):
        """
        This function takes a tweet document list and parse the different hashtags
        :param tweet document list: list re-preseting the tweet.
        :return: List the of different hashtags.
        """
        try:
            hashtags_list = [text[w+1] for w in range(len(text)) if text[w] == "#"]
        except:
            return [].extend(hashtags_list)
        broken_hashtags = []
        for term_to_parse in hashtags_list:
            if len(term_to_parse.split('_')) > 1:
                broken_hashtags.extend(term_to_parse.split('_'))
                broken_hashtags.append('#{}'.format(term_to_parse.replace('_', '')))
            elif 1 < len((''.join(' ' + x if 'A' <= x <= 'Z' else x for x in term_to_parse)).split(" ")) < len(term_to_parse) - 1:
                broken_hashtags.extend(x.lower() for x in (''.join(' ' + x if 'A' <= x <= 'Z' else x for x in term_to_parse)).split(" "))
                broken_hashtags.append('#{}'.format(term_to_parse.lower()))
            else:
                broken_hashtags.append(term_to_parse)
                broken_hashtags.append('#{}'.format(term_to_parse))
            text.remove(term_to_parse)
        return broken_hashtags

    def parse_URL(self ,text):
        """
        This function takes a tweet document list and parse the URL
        :param url: ["'ve", 'come', 'https', '//t.co/UsLTzX1Z7e']
        :return:
        """
        for i, url in enumerate(text):
            if url == 'https':
                url_list = []
                url_list.extend(re.split('[/:&=@;%#?+-]', text[i+2]))
                url_list = [e for e in url_list if e not in ('')]
                if "www" in url_list[0]:
                    url_list.append("www")
                    url_list.append(url_list[0][4:])
                    del url_list[0]
                del text[i + 1]
                del text[i + 1]
                text.extend(url_list)
        #url_encoding_special_characters = ["$", "&", "+", ",", "/", ":", ";", "=", "?", "@", "%", "#", "//"]


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
        #print(text)

    def parse_number(self, text):
        """
        This function takes a tweet document list and parse the numbers
        :param text:
        :return:
        """
        for i, term in enumerate(text):
            if self.is_int(term.replace(',','')) or self.is_float(term.replace(',','')):
                converted_number = self.convert_to_num(term.replace(',',''))
                if i == len(text) - 1:
                    if 1000 <= converted_number < 1000000:
                        text[i] = "{}K".format("%.3f" % (converted_number / 1000)) if not (converted_number / 1000).is_integer() else "{}K".format(int(converted_number / 1000))
                    elif 1000000 <= converted_number < 1000000000:
                        text[i] = "{}M".format("%.3f" % (converted_number / 1000000)) if not (converted_number / 1000000).is_integer() else "{}M".format(int(converted_number / 1000000))
                    elif 1000000000 <= converted_number:
                        text[i] = "{}B".format("%.3f" % (converted_number / 1000000000)) if not (converted_number / 1000000000).is_integer() else "{}B".format(int(converted_number / 1000000000))
                try:
                    if (text[i + 1] == "Thousand") or (1000 <= converted_number < 1000000):
                        if text[i + 1] == "Thousand":
                            text[i] = "{}K".format(''.join(text[i]))
                            del text[i + 1]
                        else:
                            text[i] = "{}K".format("%.3f" % (converted_number / 1000)) if not (converted_number / 1000).is_integer() else "{}K".format(int(converted_number / 1000))
                    elif (text[i + 1] == "Million") or (1000000 <= converted_number < 1000000000):
                        if text[i + 1] == "Million":
                            text[i] = "{}M".format(''.join(text[i]))
                            del text[i + 1]
                        else:
                            text[i] = "{}M".format("%.3f" % (converted_number / 1000000)) if not (converted_number / 1000000).is_integer() else "{}M".format(int(converted_number / 1000000))
                    elif (text[i + 1] == "Billion") or (1000000000 <= converted_number):
                        if text[i + 1] == "Billion":
                            text[i] = "{}B".format(''.join(text[i]))
                            del text[i + 1]
                        else:
                            text[i] = "{}B".format("%.3f" % (converted_number / 1000000000)) if not (converted_number / 1000000000).is_integer() else "{}B".format(int(converted_number / 1000000000))
                    elif converted_number < 1000 and self.check_if_term_is_fraction(text[i + 1]) and not self.is_float(
                            term):
                        text[i] = term + " {}".format(''.join(text[i + 1]))
                        del text[i + 1]
                except:
                    continue
        #print(text)

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

        doc_length = len(tokenized_text)  # after text operations(without stopword).
        stemmer = Stemmer()
        #tokenized_text = stemmer.stem_tweet(tokenized_text)
        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        #print(term_dict)
        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document


if __name__ == '__main__':
    p = Parse()
    l = ['1280947322000531456', 'Wed Jul 08 19:30:15 +0000 2020', '@MrStache9 The idiot can money and the media worries about Scheer with no mask. Laser focus guys well done. #GOAT', '[]', '', None, '', None]
    #p.parse_hashtags(['@', 'samuel', 'futur', 'ballon', "d'or", '#', "stayAtHome", "#", "stay_at_home", '#', "GOAT"])
    #p.parse_hashtags(['@', 'samuel', 'futur', 'ballon', "d'or", '#', "stayAtHome"])
    #p.parse_hashtags(['#', 'COVID19'])
    #p.parse_doc(l)
    n = '@MrStache9 The idiot can money 35 3/4 and the media worries about Scheer with no mask 10.6 percentage. #GOAT'
    #p.parse_sentence(n)
    #p.parse_percentage(['@MrStache9', 'The', 'idiot', 'can', 'money', 'and', 'the', 'media', 'worries', 'about', 'Scheer', 'with', 'no', 'mask', '55', 'percentage', '.', '#', 'GOAT'])
    #p.parse_doc(l)
    #p.parse_at(['rt', '@', 'kylekulinski', ':', 'japan', 'less', '1,000', 'covid', 'deaths', 'never', 'went', 'full', 'economic', 'shut', 'unemployment', 'rate', 'of…'])
    #p.parse_URL([ 'https', ':', '//www.github.com/samuelbenichou'])
    #p.parse_number(['@MrStache9', 'The', 'idiot', 'can', 'money', 'and', 'the', 'media', 'worries', 'about', 'Scheer', 'with', 'no', 'mask', '10012,3', 'dollar', '.', '#', 'GOAT'])
    #p.parse_number([ '🇩🇪', '298','55555', '55,442','🇺🇸', '100000','20000000' , '10000','1000000'])
    #p.parse_number(['@MrStache9', 'The', 'idiot', 'can', 'money', '35.5', '3/4', 'and', 'the', 'media', 'worries', 'about', 'Scheer', 'with', 'no', 'mask', '10.6%', '.', '#', 'GOAT'])
    #p.parse_URL(['@', 'samuel', 'futur', 'ballon', "d'or","https://twitter.com/i/web/status/1280947321581248514" , '#', "stayAtHome", "#", "stay_at_home", '#', "GOAT"])
    #p.parse_URL(["https://www.twitter.com/i/web/status/1280947321581248514", 'samuel', 'benichou' , "https://www.chelseafc.com/en", "https://github.com/samuelbenichou"])
    #p.parse_sentence(" #schoolsreopening #COVID19 ")
    print(p.parse_sentence('Former Vice President Dick Cheney told conservative radio host Laura Ingraham that he was honored to be compared to Alexandria Ocasio-Cortez while in office.'))
    #print(p.get_continuous_chunks(['Barack', 'Obama', 'husband', 'Alexandria', 'Ocasio-Cortez', 'Donald', 'Trump']))
    #print(p.parse_sentence("Barack Obama is the husband of Alexandria Ocasio-Cortez and Donald Trump."))
    #print(p.get_continuous_chunks('Former Vice President Dick Cheney told conservative radio host Laura Ingraham that he was honored to be compared to Alexandria Ocasio-Cortez while in office.'))

