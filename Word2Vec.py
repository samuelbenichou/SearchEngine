import requests
import time


class Word2VecScrapper(object):

    def __init__(self, ):
        self.url = "http://bionlp-www.utu.fi/wv_demo/nearest"
        self.data = {'form[1][name]': 'topn', 'form[0][name]': 'word', 'model_name': "English GoogleNews Negative300", }
        self.local_cache = {}

    def get_top_n_dictionary(self, word_list_input, n=3):
        ret_dict = {}

        for word in word_list_input:
            if word in self.local_cache:
                ret_dict[word] = self.local_cache[word]
            else:
                ret_dict[word] = self.get_top_n(word, n)

        return ret_dict

    def get_top_n(self, word, n=3):
        self.data['form[1][value]'] = str(n)
        self.data['form[0][value]'] = word

        if word in self.local_cache:
            return self.local_cache[word]

        # Post the request to the server
        r = requests.post(self.url, self.data)

        # Check request status
        if r.status_code != "200" and r.reason != "OK":
            return None

        # Get the text from the response
        response_text = r.text

        # Given word is not in the dictionary
        if "is not in the vocabulary" in response_text:
            return None

        response_text = response_text.replace(r'{"tbl": "<div class=\"w2vresultblock bg-info\">\n\n', "") \
            .replace(r"\n", "").replace("</div>", " ").replace("</br>", " ").replace('"}', "")
        response_text = response_text.split()
        self.local_cache[word] = response_text
        return response_text


if __name__ == "__main__":
    start_time = time.time()  # starting time

    w2v = Word2VecScrapper()
    word_dict = w2v.get_top_n_dictionary(
        ["covid", "case"])
    print(word_dict)
    elapsed_time = time.time() - start_time
    print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
