from gensim.scripts.glove2word2vec import glove2word2vec
from gensim.models import KeyedVectors
import utils

class GloVeMethod:


    def __init__(self, glove_input_file='../../../../glove.twitter.27B.25d.txt'):
        self.glove_input_file = glove_input_file
        self.word2vec_output_file = 'glove.twitter.27B.25d.txt.word2vec'
        glove2word2vec(glove_input_file, self.word2vec_output_file)

    def get_filename(self):
        filename = '../../../../glove.twitter.27B.25d.txt'

    def most_similar(self, query):
        glove_input_file = 'glove.twitter.27B.25d.txt'
        word2vec_output_file = 'glove.twitter.27B.25d.txt.word2vec'
        glove2word2vec(glove_input_file, word2vec_output_file)
        model = KeyedVectors.load_word2vec_format(self.word2vec_output_file, binary=False)
        result = []
        for term in query:
            try:
                if term[0] == "@" or term[0] == "#":
                    continue
                else:
                    result.extend([model.most_similar(term)[0], model.most_similar(term)[1], model.most_similar(term)[2]])
            except:
                pass
        return result
        #print(model.most_similar(query))
        #print(model.most_similar(query)[0])
        #print(model.most_similar(query)[0][0])

if __name__ == '__main__':
    #g = GloVeMethod('glove.twitter.27B.25d.txt')
    #g.most_similar('america')
    #glove_input_file = 'glove.6B.100d.txt'
    #word2vec_output_file = 'glove.6B.100d.txt.word2vec'
    #glove2word2vec(glove_input_file, word2vec_output_file)

    # load the Stanford GloVe model
    #filename = 'glove.6B.100d.txt.word2vec'
    #model = KeyedVectors.load_word2vec_format(filename, binary=False)
    # calculate: (king - man) + woman = ?
    # print(model.most_similar('obama'))
    # print(model.most_similar('banana'))
    # print(model.most_similar(negative='banana'))
    # result = model.most_similar(positive=['woman', 'king'], negative=['man'], topn=1)
    # print(result)
    #People should NOT wear masks while exercising
    inverted_index = utils.load_obj('inverted_index')
    print(inverted_index)
