from search_engine_interface import search_engine_interface
from searcher import Searcher
from configuration import ConfigClass
from spellchecker import SpellChecker



class SearchEngine(search_engine_interface):
    ##############################################
    ###########      SpellChecker      ###########
    ##############################################

    def __init__(self):
        super().__init__()


    def search(self, query):
        query_as_list = self.splellChecker(query)
        searcher = Searcher(self._parser, self._indexer, model=self._model)
        print(query_as_list)
        n_relevant, ranked_doc_ids = searcher.search(query_as_list,5)
        print(ranked_doc_ids)
        return n_relevant, ranked_doc_ids


    def splellChecker(self, query):
        # https://stackoverflow.com/questions/13928155/spell-checker-for-python#:~:text=The%20best%20way%20for%20spell,The%20fastest%20one%20is%20SymSpell.&text=This%20library%20is%20based%20on%20Peter%20Norvig's%20implementation.
        result = []
        spell = SpellChecker()
        for term in self._parser.parse_sentence(query):
            # find those words that may be misspelled
            misspelled = spell.unknown(term)
            if len(misspelled) == 0 or term == 'covid':
                result.append(term)
            else:
                # Get the one `most likely` answer
                result.append(spell.correction(term))
        return result


if __name__ == '__main__':
    s = SearchEngine()
    s.build_index_from_parquet("/Users/samuel/Desktop/Corpus/test")
    s.search("Coronavirus is less dangerous than the flu")
    #'bioweapon'
    #s.splellChecker("Coronavirus is less dangerous than the flu ")
