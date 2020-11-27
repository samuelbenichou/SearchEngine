from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re
from stemmer import Stemmer
from nltk import ne_chunk, pos_tag
from nltk.tree import Tree
import flag


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')
        self.stop_words.extend(["'m","'ve",'+', '~', '{', '|', '}', "#", '.', ':', '’', "'s", '?', ',', 'https', '”', '“', '...', "''","``",';', '!', '•', '(',')', "n't", '…', '......', '.....', '','||'])
        self.corona_words = ["COVID-19", "covid", "coronavirus", "covid_19", "covid19", "Covid-19", "covid-19", "COVID"]
        self.regrex_pattern = re.compile(pattern="["
                                            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                            "\U0001F300-\U0001F5FF"  # symbols & pictographs
                                            "\U0001F600-\U0001F64F"  # emoticons
                                            "\U0001F680-\U0001F6FF"  # transport & map symbols
                                            "\U0001F700-\U0001F77F"  # alchemical symbols
                                            "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
                                            "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                                            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                                            "\U0001FA00-\U0001FA6F"  # Chess Symbols
                                            "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                                            "\U00002702-\U000027B0"  # Dingbats
                                            "\U000024C2-\U0001F251"
                                            "]+", flags=re.UNICODE)

    def isEnglish(self,s):
        try:
            s.encode(encoding='utf-8').decode('ascii')
        except UnicodeDecodeError:
            return False
        else:
            return True

    def isSymbol(self,term):
        symbols = "!@#$%^&*()_-+={}[].,:?|+~"
        for ch in term:
            if ch not in symbols:
                return False
        return True

    def isNumber(self, term):
        return term.replace('.', '', 1).replace(',','').isdigit()

    def hashtage(self, i, tokens, l):
        token = tokens[i]
        if self.isSymbol(token) or not self.isEnglish(token):
            return False
        if token in self.corona_words:
            l.append('#covid')
            return True
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
        if self.isSymbol(token) or not self.isEnglish(token):
            return False
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

    def parse_percent(self, num, i, tokens, l):
        if (i < len(tokens)):
            t = tokens[i]
            if t == '%' or t.lower() == 'percent' or t.lower() == 'percentage':
                l.append(num + '%')
                return True
        return False


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


    def parse_sentence(self, text):
        l = []
        tokens = word_tokenize(text)
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
                skip = True;
                continue

            if self.isNumber(token) and self.parse_percent(token, i, tokens, l):
                skip = True;
                continue

            if self.is_int(token.replace(',', '')) or self.is_float(token.replace(',', '')):
                converted_number = self.convert_to_num(token.replace(',', ''))
                if i == len(tokens):
                    if 1000 <= converted_number < 1000000:
                        l.append("{}K".format("%.3f" % (converted_number / 1000)) if not (
                                    converted_number / 1000).is_integer() else "{}K".format(
                            int(converted_number / 1000)))
                    elif 1000000 <= converted_number < 1000000000:
                        l.append("{}M".format("%.3f" % (converted_number / 1000000)) if not (
                                    converted_number / 1000000).is_integer() else "{}M".format(
                            int(converted_number / 1000000)))
                    elif 1000000000 <= converted_number:
                        l.append("{}B".format("%.3f" % (converted_number / 1000000000)) if not (
                                    converted_number / 1000000000).is_integer() else "{}B".format(
                            int(converted_number / 1000000000)))
                try:
                    if (tokens[i] == "Thousand") or (1000 <= converted_number < 1000000):
                        if tokens[i] == "Thousand":
                            l.append("{}K".format(''.join(token)))
                            skip = True
                        else:
                            l.append("{}K".format("%.3f" % (converted_number / 1000)) if not (
                                        converted_number / 1000).is_integer() else "{}K".format(
                                int(converted_number / 1000)))
                    elif (tokens[i] == "Million") or (1000000 <= converted_number < 1000000000):
                        if tokens[i] == "Million":
                            l.append("{}M".format(''.join(token)))
                            skip = True
                        else:
                            l.append("{}M".format("%.3f" % (converted_number / 1000000)) if not (
                                        converted_number / 1000000).is_integer() else "{}M".format(
                                int(converted_number / 1000000)))
                    elif (tokens[i] == "Billion") or (1000000000 <= converted_number):
                        if tokens[i] == "Billion":
                            l.append("{}B".format(''.join(token)))
                            skip = True
                        else:
                            l.append("{}B".format("%.3f" % (converted_number / 1000000000)) if not (
                                        converted_number / 1000000000).is_integer() else "{}B".format(
                                int(converted_number / 1000000000)))
                    elif converted_number < 1000 and self.check_if_term_is_fraction(tokens[i]) and not isinstance(token,
                                                                                                                  float):
                        l.append(token + " {}".format(''.join(tokens[i])))
                        skip = True
                except:
                    pass

                continue

            if self.is_flag_emoji(token):
                try:
                    l.append(flag.ISO3166[flag.dflagize(token)[1:3]])
                    continue
                except:
                    continue

            if token in self.corona_words:
                l.append("covid")
                continue

            if self.cleaning(token, tokens, l):
                size = len(tokens)
                continue

            l.append(self.regrex_pattern.sub(r'', token))


        text_tokens_without_stopwords = [w for w in l if w.lower() not in self.stop_words]
        #print(text_tokens_without_stopwords)
        return text_tokens_without_stopwords

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
        # print(tokenized_text)

        for i, term in enumerate(tokenized_text):
            if term not in term_dict.keys():
                term_dict[term] = [1, [i]]
            else:
                term_dict[term][0] += 1
                term_dict[term][1].append(i)


        # print(term_dict)
        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document

    def remove_emoji(self, text):
        regrex_pattern = re.compile(pattern="["
                                            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                            "\U0001F300-\U0001F5FF"  # symbols & pictographs
                                            "\U0001F600-\U0001F64F"  # emoticons
                                            "\U0001F680-\U0001F6FF"  # transport & map symbols
                                            "\U0001F700-\U0001F77F"  # alchemical symbols
                                            "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
                                            "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                                            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                                            "\U0001FA00-\U0001FA6F"  # Chess Symbols
                                            "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                                            "\U00002702-\U000027B0"  # Dingbats
                                            "\U000024C2-\U0001F251"
                                            "]+", flags=re.UNICODE)

        try:
            l.append(flag.ISO3166[flag.dflagize(text)[1:3]])
        except:
            pass


    def is_flag_emoji(self, c):
        return "\U0001F1E6\U0001F1E8" <= c <= "\U0001F1FF\U0001F1FC" or c in [
                "\U0001F3F4\U000e0067\U000e0062\U000e0065\U000e006e\U000e0067\U000e007f",
                "\U0001F3F4\U000e0067\U000e0062\U000e0073\U000e0063\U000e0074\U000e007f",
                "\U0001F3F4\U000e0067\U000e0062\U000e0077\U000e006c\U000e0073\U000e007f"]


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
    p.parse_sentence("This shit 🇺🇸 right here 😷🖕😷 making #COVID-19 me wanna work out 🤔🤔🤔h")
    #p.parse_sentence("@bisping Wear a mask 😷 u donkeys 👊🏼🇨🇦👊🏼")
    #p.parse_sentence('1 Million 35 3/4 cases 1000000 55,442')
    #print(p.check_if_term_is_fraction('3/4'))

