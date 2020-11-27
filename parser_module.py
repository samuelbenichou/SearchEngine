# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize
# from document import Document
# import re
# from stemmer import Stemmer
# from nltk import ne_chunk, pos_tag
# from nltk.tree import Tree
# import flag
#
#
# class Parse:
#
#     def __init__(self):
#         self.stop_words = stopwords.words('english')
#         self.corona_words = ["COVID-19", "covid", "coronavirus"]
#
#     def parse_sentence(self, text):
#         """
#         This function tokenize, remove stop words and apply lower case for every word within the text
#         :param text:
#         :return:
#         """
#         #print(text)
#         text_tokens = word_tokenize(text)
#         if "@" in text_tokens:
#             self.parse_at(text_tokens)
#         self.get_continuous_chunks(text_tokens)
#         text_hashtags = self.parse_hashtags(text_tokens)
#         text_tokens.extend(text_hashtags)
#         self.stop_words.extend(["#", '.',':','’',"'s",'?',',', 'https','”','“','...',"''",'!','•', '(', ')','',"n't",'…','......','.....',''])
#         # if "@" in text_tokens:
#         #     self.parse_at(text_tokens)
#         self.remove_emoji(text_tokens)
#         self.parse_URL_in_tweet(text_tokens)
#         self.parse_number(text_tokens)
#         self.parse_percentage(text_tokens)
#         #self.remove_emoji(text_tokens)
#         text_tokens_without_stopwords = [w for w in text_tokens if w.lower() not in self.stop_words]
#         #print(text_tokens_without_stopwords)
#         return text_tokens_without_stopwords
#
#     def get_continuous_chunks(self, text):
#         try:
#             chunked = ne_chunk(pos_tag(text)) # check exception
#         except:
#             return
#         prev = None
#         continuous_chunk = []
#         current_chunk = []
#
#         for i in chunked:
#             if type(i) == Tree:
#                 current_chunk.append(" ".join([token for token, pos in i.leaves()]))
#             elif current_chunk:
#                 named_entity = " ".join(current_chunk)
#                 if named_entity not in continuous_chunk:
#                     continuous_chunk.append(named_entity)
#                     current_chunk = []
#             else:
#                 continue
#
#         if continuous_chunk:
#             named_entity = " ".join(current_chunk)
#             if named_entity not in continuous_chunk and named_entity != '':
#                 continuous_chunk.append(named_entity)
#
#         for named_entity_recognition in continuous_chunk:
#             if named_entity_recognition not in text:
#                 text.append(named_entity_recognition)
#
#     def remove_emoji(self, text):
#         regrex_pattern = re.compile(pattern="["
#                                             "\U0001F1E0-\U0001F1FF"  # flags (iOS)
#                                             "\U0001F300-\U0001F5FF"  # symbols & pictographs
#                                             "\U0001F600-\U0001F64F"  # emoticons
#                                             "\U0001F680-\U0001F6FF"  # transport & map symbols
#                                             "\U0001F700-\U0001F77F"  # alchemical symbols
#                                             "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
#                                             "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
#                                             "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
#                                             "\U0001FA00-\U0001FA6F"  # Chess Symbols
#                                             "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
#                                             "\U00002702-\U000027B0"  # Dingbats
#                                             "\U000024C2-\U0001F251"
#                                             "]+", flags=re.UNICODE)
#         for i,term in enumerate(text):
#             if self.is_flag_emoji(term):
#                 #print(flag.dflagize(term)[1:3])
#                 #print(flag.ISO3166[flag.dflagize(term)[1:3]])
#                 try:
#                     text[i] = flag.ISO3166[flag.dflagize(term)[1:3]]
#                 except:
#                     text[i] = ''
#             else :
#                 #print(regrex_pattern.sub(r'', term))
#                 text[i] = regrex_pattern.sub(r'', term)
#
#
#     def parse_percentage(self, text):
#         """
#         This function takes a tweet document list and parse the percentage symbol
#         :param text: List of term
#         :return:
#         """
#         for i, term in enumerate(text):
#             if (text[i] == "%" or text[i] == "percent" or text[i] == "percentage") and (text[i-1].isnumeric() or text[i-1].replace('.', '', 1).isdigit()):
#                 text[i] = "{}%".format(''.join(text[i - 1]))
#                 del text[i-1]
#         #print(text)
#
#     def parse_hashtags(self, text):
#         """
#         This function takes a tweet document list and parse the different hashtags
#         :param tweet document list: list re-preseting the tweet.
#         :return: List the of different hashtags.
#         """
#         try:
#             hashtags_list = [text[w+1] for w in range(len(text)) if text[w] == "#"]
#         except:
#             return [].extend(hashtags_list)
#         broken_hashtags = []
#         for term_to_parse in hashtags_list:
#             if len(term_to_parse.split('_')) > 1:
#                 broken_hashtags.extend(term_to_parse.split('_'))
#                 broken_hashtags.append('#{}'.format(term_to_parse.replace('_', '')))
#             elif 1 < len((''.join(' ' + x if 'A' <= x <= 'Z' else x for x in term_to_parse)).split(" ")) < len(term_to_parse) - 1:
#                 broken_hashtags.extend(x.lower() for x in (''.join(' ' + x if 'A' <= x <= 'Z' else x for x in term_to_parse)).split(" "))
#                 broken_hashtags.append('#{}'.format(term_to_parse.lower()))
#             else:
#                 broken_hashtags.append(term_to_parse)
#                 broken_hashtags.append('#{}'.format(term_to_parse))
#             text.remove(term_to_parse)
#         return broken_hashtags
#
#     def parse_URL_in_tweet(self ,text):
#         """
#         This function takes a tweet document list and parse the URL
#         :param url: ["'ve", 'come', 'https', '//t.co/UsLTzX1Z7e']
#         :return:
#         """
#         for i, url in enumerate(text):
#             if url == 'https':
#                 url_list = []
#                 url_list.extend(re.split('[/:&=@;%#?+-]', text[i+2]))
#                 url_list = [e for e in url_list if e not in ('')]
#                 if "www" in url_list[0]:
#                     url_list.append("www")
#                     url_list.append(url_list[0][4:])
#                     del url_list[0]
#                 del text[i + 1]
#                 del text[i + 1]
#                 text.extend(url_list)
#         #url_encoding_special_characters = ["$", "&", "+", ",", "/", ":", ";", "=", "?", "@", "%", "#", "//"]
#
#
#     def parse_at(self, text): # Assume there is a @
#         """
#         This function takes a tweet document list and parse the @
#         :param url:
#         :return:
#         """
#         for i,term in enumerate(text):
#             if text[i] == "@":
#                 text[i] = "@{}".format(''.join(text[i+1]))
#                 del text[i+1]
#         #print(text)
#
#     def parse_number(self, text):
#         """
#         This function takes a tweet document list and parse the numbers
#         :param text:
#         :return:
#         """
#         for i, term in enumerate(text):
#             if self.is_int(term.replace(',','')) or self.is_float(term.replace(',','')):
#                 converted_number = self.convert_to_num(term.replace(',',''))
#                 if i == len(text) - 1:
#                     if 1000 <= converted_number < 1000000:
#                         text[i] = "{}K".format("%.3f" % (converted_number / 1000)) if not (converted_number / 1000).is_integer() else "{}K".format(int(converted_number / 1000))
#                     elif 1000000 <= converted_number < 1000000000:
#                         text[i] = "{}M".format("%.3f" % (converted_number / 1000000)) if not (converted_number / 1000000).is_integer() else "{}M".format(int(converted_number / 1000000))
#                     elif 1000000000 <= converted_number:
#                         text[i] = "{}B".format("%.3f" % (converted_number / 1000000000)) if not (converted_number / 1000000000).is_integer() else "{}B".format(int(converted_number / 1000000000))
#                 try:
#                     if (text[i + 1] == "Thousand") or (1000 <= converted_number < 1000000):
#                         if text[i + 1] == "Thousand":
#                             text[i] = "{}K".format(''.join(text[i]))
#                             del text[i + 1]
#                         else:
#                             text[i] = "{}K".format("%.3f" % (converted_number / 1000)) if not (converted_number / 1000).is_integer() else "{}K".format(int(converted_number / 1000))
#                     elif (text[i + 1] == "Million") or (1000000 <= converted_number < 1000000000):
#                         if text[i + 1] == "Million":
#                             text[i] = "{}M".format(''.join(text[i]))
#                             del text[i + 1]
#                         else:
#                             text[i] = "{}M".format("%.3f" % (converted_number / 1000000)) if not (converted_number / 1000000).is_integer() else "{}M".format(int(converted_number / 1000000))
#                     elif (text[i + 1] == "Billion") or (1000000000 <= converted_number):
#                         if text[i + 1] == "Billion":
#                             text[i] = "{}B".format(''.join(text[i]))
#                             del text[i + 1]
#                         else:
#                             text[i] = "{}B".format("%.3f" % (converted_number / 1000000000)) if not (converted_number / 1000000000).is_integer() else "{}B".format(int(converted_number / 1000000000))
#                     elif converted_number < 1000 and self.check_if_term_is_fraction(text[i + 1]) and not self.is_float(
#                             term):
#                         text[i] = term + " {}".format(''.join(text[i + 1]))
#                         del text[i + 1]
#                 except:
#                     continue
#         #print(text)
#
#     def check_if_term_is_fraction(self, term):
#         values = term.split('/')
#         return len(values) == 2 and all(i.isdigit() for i in values)
#
#     def is_float(self, value):
#         try:
#             float(value)
#             return True
#         except:
#             return False
#
#     def is_int(self, value):
#         try:
#             int(value)
#             return True
#         except:
#             return False
#
#     def convert_to_num(self, value):
#         try:
#             return int(value)
#         except ValueError:
#             return float(value)
#
#     def parse_doc(self, doc_as_list):
#         """
#         This function takes a tweet document as list and break it into different fields
#         :param doc_as_list: list re-preseting the tweet.
#         :return: Document object with corresponding fields.
#         """
#         tweet_id = doc_as_list[0]
#         tweet_date = doc_as_list[1]
#         full_text = doc_as_list[2]
#         url = doc_as_list[3]
#         retweet_text = doc_as_list[4]
#         retweet_url = doc_as_list[5]
#         quote_text = doc_as_list[6]
#         quote_url = doc_as_list[7]
#         term_dict = {}
#         tokenized_text = self.parse_sentence(full_text)
#
#
#
#         doc_length = len(tokenized_text)  # after text operations(without stopword).
#         stemmer = Stemmer()
#         #tokenized_text = stemmer.stem_tweet(tokenized_text)
#         """
#         term_dict[term] = [how many time the current term appear in the tweet, [list of position of the term in the tweet]]
#         """
#         for i, term in enumerate(tokenized_text):
#             if term not in term_dict.keys():
#                 term_dict[term] = [1, [i]]
#             else:
#                 term_dict[term][0] += 1
#                 term_dict[term][1].append(i)
#
#
#         #print(term_dict)
#         document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
#                             quote_url, term_dict, doc_length)
#         return document
#
#     def is_flag_emoji(self, c):
#         return "\U0001F1E6\U0001F1E8" <= c <= "\U0001F1FF\U0001F1FC" or c in [
#             "\U0001F3F4\U000e0067\U000e0062\U000e0065\U000e006e\U000e0067\U000e007f",
#             "\U0001F3F4\U000e0067\U000e0062\U000e0073\U000e0063\U000e0074\U000e007f",
#             "\U0001F3F4\U000e0067\U000e0062\U000e0077\U000e006c\U000e0073\U000e007f"]


from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re

class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')
        self.stop_words.extend(['+','~','{','|','}',"#", '.',':','’',"'s",'?',',', 'https','”','“','...',"''",'!','•', '(', ')',"n't",'…','......','.....',''])

    def isEnglish(self,s):
        try:
            s.encode(encoding='utf-8').decode('ascii')
        except UnicodeDecodeError:
            return False
        else:
            return True

    def isSymbol(self,term):
        symbols = "!@#$%^&*()_-+={}[].,:?"
        for ch in term:
            if ch not in symbols:
                return False
        return True

    def hashtage(self, i, tokens, l):
        token = tokens[i+1]
        if self.isSymbol(token):
            return False
        if '_' in token:
            temp = token.split('_')
        else:
            temp = re.findall('[^a-z]*[^A-Z][^A-Z]*', token)
        for j in temp:
            l.append(j.lower())
        l.append('#' + token.replace('_','').lower())
        return True

    def tag(self, i, tokens, l):
        token = tokens[i]
        l.append('@'+token.lower())

    def cleaning(self, token, tokens, l):
        if token == '':
            return True
        if '.' in token:
            tokens.extend(token.split('.'))
            return True
        elif '=' in token:
            tokens.extend(token.split('='))
            return True
        elif '/' in token:
            tokens.extend(token.split('/'))
            return True
        elif self.isSymbol(token):
            return True
        elif self.isEnglish(token):
            l.append(token)
            return True

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

    def isNumber(self, term): # We dont need this function fuck guy
        return term.replace('.', '', 1).replace(',','').isdigit()


    def parse_number(self, term):
        pass


    def parse_sentence(self, text):
        l = []
        tokens = word_tokenize(text)
        print(tokens)
        size = len(tokens)
        skip = False

        i = 0
        for token in tokens:
            i += 1
            if skip:
                skip = False
                continue

            if token == '#' and '#'+tokens[i] in text:
                self.hashtage(i,tokens,l)
                skip = True;
                continue

            if token == '@' and '@'+tokens[i] in text:
                self.tag(i,tokens,l)
                skip = True
                continue

            if self.is_int(token.replace(',','')) or self.is_float(token.replace(',','')):
                converted_number = self.convert_to_num(token.replace(',',''))
                if i == len(tokens):
                    if 1000 <= converted_number < 1000000:
                        l.append("{}K".format("%.3f" % (converted_number / 1000)) if not (converted_number / 1000).is_integer() else "{}K".format(int(converted_number / 1000)))
                    elif 1000000 <= converted_number < 1000000000:
                        l.append("{}M".format("%.3f" % (converted_number / 1000000)) if not (converted_number / 1000000).is_integer() else "{}M".format(int(converted_number / 1000000)))
                    elif 1000000000 <= converted_number:
                        l.append("{}B".format("%.3f" % (converted_number / 1000000000)) if not (converted_number / 1000000000).is_integer() else "{}B".format(int(converted_number / 1000000000)))
                try:
                    if (tokens[i] == "Thousand") or (1000 <= converted_number < 1000000):
                        if tokens[i] == "Thousand":
                            l.append("{}K".format(''.join(token)))
                            skip = True
                        else:
                            l.append("{}K".format("%.3f" % (converted_number / 1000)) if not (converted_number / 1000).is_integer() else "{}K".format(int(converted_number / 1000)))
                    elif (tokens[i] == "Million") or (1000000 <= converted_number < 1000000000):
                        if tokens[i] == "Million":
                            l.append("{}M".format(''.join(token)))
                            skip = True
                        else:
                            l.append("{}M".format("%.3f" % (converted_number / 1000000)) if not (converted_number / 1000000).is_integer() else "{}M".format(int(converted_number / 1000000)))
                    elif (tokens[i] == "Billion") or (1000000000 <= converted_number):
                        if tokens[i] == "Billion":
                            l.append("{}B".format(''.join(token)))
                            skip = True
                        else:
                            l.append("{}B".format("%.3f" % (converted_number / 1000000000)) if not (converted_number / 1000000000).is_integer() else "{}B".format(int(converted_number / 1000000000)))
                    elif converted_number < 1000 and self.check_if_term_is_fraction(tokens[i]) and not isinstance(token, float):
                        l.append(token + " {}".format(''.join(tokens[i])))
                        skip = True
                except:
                    pass

                continue


            if self.cleaning(token, tokens, l):
                size = len(tokens)
                continue

        stop_words = stopwords.words('english')
        stop_words.extend(
            ['@','com','www','','+', '~', '{', '|', '}', "#", '.', ':', '’', "'s", '?', ',', 'https', '”', '“', '...', "''", '!', '•', '(',
             ')', "n't", '…', '......', '.....', ''])
        text_tokens_without_stopwords = [w for w in l if w.lower() not in stop_words]
        print(text_tokens_without_stopwords)
        return text_tokens_without_stopwords


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
    #print(p.parse_sentence('Former Vice President Dick Cheney told conservative radio host Laura Ingraham that he was honored to be compared to Alexandria Ocasio-Cortez while in office.'))
    #print(p.get_continuous_chunks(['Barack', 'Obama', 'husband', 'Alexandria', 'Ocasio-Cortez', 'Donald', 'Trump']))
    #print(p.parse_sentence("Barack Obama is the husband of Alexandria Ocasio-Cortez and Donald Trump."))
    #print(p.get_continuous_chunks('Former Vice President Dick Cheney told conservative radio host Laura Ingraham that he was honored to be compared to Alexandria Ocasio-Cortez while in office.'))
    #p.parse_emojis(['RT', '@ProjectLincoln', ':', 'Yesterday', '’', 's', 'new', 'coronavirus', 'cases', ':', '🇩🇰', '10', '🇳🇴', '11', '🇸🇪', '57', '🇩🇪', '298', '🇺🇸', '55.442K'])
    #['👇🏼😢😡😢😡😡😡😡👇🏼', 'Truth']  ['RT', '@Mariloune', 'WEAR', 'MASK', '😷🙌🏼👊🏽', 'everyone', 'around', '❤️'  'Carsyn', 'Leigh', 'Davis', 'Coronavirus', 'Story', 'Truth', 'MSM', '😂😂😂😂'
    #['@GovMurphy', 'GTFO', 'fucking', 'problem', 'virus', 'weakened', '🖕😷🖕😷🖕😷🖕😷🖕😷🖕']
    #print(p.remove_emoji(['RT', '@ProjectLincoln', ':', 'Yesterday', '’', 's', 'new', 'coronavirus', 'cases', ':', '🇩🇰', '10', '🇳🇴', '11', '🇸🇪', '57', '🇩🇪', '298', '🇺🇸', '55.442K']))
    #p.parse_sentence("This shit right here making me wanna work out 🤔🤔🤔https://t.co/wcB3TZuxPR")
    #p.parse_sentence("@bisping Wear a mask 😷 u donkeys 👊🏼🇨🇦👊🏼")
    p.parse_sentence('1 Million 35 3/4 cases 1000000 55,442')
    print(p.check_if_term_is_fraction('3/4'))

