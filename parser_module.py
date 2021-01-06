from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re
from stemmer import Stemmer
import flag

symbols = ['+', '-', '*', '=', '', '//', '#', '.', ',', ':', '!', '?', '•', '|', '||', '~', '$', '%','&',
        '(', ')', '{', '}', '[', ']',
        '.', '..', '...', '....', '.....',
        '’', "'", '”', '"', "''", "``", '^',
        ':', ';','.%']
stop_words = stopwords.words('english')
stop_words.extend(['',' ','rt', "'m", "'ve", "'s", "n't"])
stop_words.extend(['http', 'https', 'www', 'com', 'co'])
stop_words.extend(symbols)
percent = ['%', 'percent', 'percentage']
corona_words = ['corona','coronavirus', 'covid', 'covid-19', 'covid_19', 'covid19','coronavirusrelated']
sizes = {'thousand':1000, 'million':1000000, 'billion':1000000000}
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

def isSymbol(term):
    for ch in term:
        if ch not in symbols:
            return False
    return True

def containOneOrLessLetter(token):
    eng_letter = re.compile(r'[a-z]')
    counter = 0
    for ch in token:
        if counter > 1: return False
        if eng_letter.match(ch):
            counter += 1
    return counter <= 1

def parse_hashtage(token, l, tokens):
    temp = ''
    if len(token) <= 1 or isContainOnlySymbols(token) or not token.isascii():
        return
    elif token.lower() in corona_words:
        l.append('#covid')
        l.append('covid')
    elif isNumber(token):
        l.append('#'+parse_number(token))
        l.append(parse_number(token))
    else:
        if '_' in token:
            temp = token.split('_')
        else:
            temp = re.findall('[^a-z]*[^A-Z][^A-Z]*', token)
        for j in temp:
            tokens.append(j.lower())
        l.append('#' + token.replace('_', '').lower())

def parst_tag(token, l):
    if len(token) <= 1 or isContainOnlySymbols(token) or not token.isascii():
        return False
    if token.lower() in corona_words:
        l.append('@covid')
        l.append('covid')
    else:
        l.append('@'+token.lower())

def containSymbols(token):
    for ch in token:
        if isSymbol(ch):
            return True
    return False

def isContainOnlySymbols(token):
    for ch in token:
        if not isSymbol(ch):
            return False
    return True

def cleanSymbols(token):
    newToken = ''
    for ch in token:
        if not isSymbol(ch):
            newToken += ch
    return newToken

def cleaning(token, tokens, l):
    if len(token) <= 1 or isContainOnlySymbols(token) or not token.isascii():
        return
    if '.' in token:
        tokens.extend(token.split('.'))
    # elif '=' in token:
    #     tokens.extend(token.split('='))
    elif '/' in token:
        tokens.extend(token.split('/'))
    elif isWord(token):
        l.append(token)
    # else:
    #     l.append(cleanSymbols(token))

def is_flag_emoji(c):
    return "\U0001F1E6\U0001F1E8" <= c <= "\U0001F1FF\U0001F1FC" or c in [
        "\U0001F3F4\U000e0067\U000e0062\U000e0065\U000e006e\U000e0067\U000e007f",
        "\U0001F3F4\U000e0067\U000e0062\U000e0073\U000e0063\U000e0074\U000e007f",
        "\U0001F3F4\U000e0067\U000e0062\U000e0077\U000e006c\U000e0073\U000e007f"]

def isNumber(token):
    if check_if_term_is_fraction(token):
        return True
    term = token.replace('.', '', 1).replace(',', '')
    return term.isascii() and term.isdigit()

def clean_number(token):
    value = token.replace(',','')
    return value

def num_format(num, round_to=3):
    if(num < 1000):
        return num
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num = round(num / 1000.0, round_to)
    if num <= int(num):
        round_to = 0
    if (magnitude > 3):
        magnitude = 3
    ans = '{:.{}f}{}'.format(round(num, round_to), round_to, ['', 'K', 'M', 'B'][magnitude])
    return ans

def containNumber(token):
    pass

def isWord(token):
    word = cleanSymbols(token)
    for ch in word:
        if isNumber(ch): return False
    return True

def parse_number(clear_number, size=''):
    if int(float(clear_number)) < float(clear_number):
        num = float(clear_number)
    else:
        num = int(float(clear_number))
    size = size.lower()
    if size in sizes.keys():
        size = sizes.get(size)
        num *= size
        return str(num_format(num))
    else:
        return str(num_format(num))

def getOriginalTweetId(url):
    tokens = url.replace('"','').replace('}','').replace("'",'').replace(']','').split('/')
    i = len(tokens) - 1
    tweet_id = tokens[i]
    return tweet_id

def check_if_term_is_fraction(term):
    values = term.split('/')
    return len(values) == 2 and all(i.isdigit() for i in values)


class Parse:
    def __init__(self):
        pass

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        l = []
        tokens = word_tokenize(text)
        print(tokens)
        skip = 0
        i = -1 # index of token in tokens list
        for token in tokens:
            i += 1
            if skip:
                skip -= 1
            # CORONA TERMS:
            elif token.lower() in corona_words:
                l.append('covid')
            elif is_flag_emoji(token):
                try:
                    l.append(flag.ISO3166[flag.dflagize(token)[1:3]])
                except:
                    continue
            # HASHTAGS:
            elif token == '#' and i+1 < len(tokens):
                parse_hashtage(tokens[i+1], l, tokens)
                skip += 1
            # TAGS:
            elif token == '@' and i+1 < len(tokens):
                parst_tag(tokens[i+1], l)
                skip = True
            # Size AS A WORD:
            elif token.lower() in sizes.keys():
                l.append(parse_number('1', token))
            elif check_if_term_is_fraction(token):
                if i < len(tokens)-1 and tokens[i+1].lower() in percent:
                    l.append(token + '%')
                    skip += 1
                else:
                    l.append(token)
            # NUMBERS:
            elif isNumber(token):
                token = clean_number(token)
                if (i < len(tokens) - 2) and (tokens[i+1].lower() in sizes.keys()) and (tokens[i+2].lower() in percent):
                    l.append(parse_number(token, tokens[i+1]) + '%')
                    skip += 2
                elif (i < len(tokens) - 1) and tokens[i+1].lower() in percent:
                    l.append(parse_number(token) + '%')
                    skip += 1
                elif (i < len(tokens) - 1) and tokens[i+1].lower() in sizes.keys():
                    l.append(parse_number(token, tokens[i+1]))
                    skip += 1
                elif (i < len(tokens) - 1) and check_if_term_is_fraction(tokens[i+1]):
                    l.append(token +' '+ tokens[i+1])
                    skip += 1
                else:
                    l.append(parse_number(token))
            # OTHER TOKENS:
            else:
                cleaning(token, tokens, l)

        text_tokens_without_stopwords = [w for w in l if w.lower() not in stop_words]
        print(text_tokens_without_stopwords)
        return text_tokens_without_stopwords

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-presenting the tweet.
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

        doc_length = len(tokenized_text)  # after text operations.

        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document



if __name__ == '__main__':
    p = Parse()
    # p.parse_sentence('204 35.66 35 3/4 35.75')
    # t = '@projectlincoln Yesterday new covid cases Denmark 10.0 Norway 11.0 Sweden 57.0 Germany 298.0 United States of America 55.442K'
    # t = 'https://www.instagram.com/p/CD7fAPWs3WM/?igshid=o9kf0ugp1l8x'
    # t = 'CD7fAPWs3WM/?igshid=o9kf0ugp1l8x'
    t = 'RT Rt rT rt'
    p.parse_sentence(t)