<img src="https://cdn.searchenginejournal.com/wp-content/uploads/2018/10/14-Great-Search-Engines-You-Can-Use-Instead-of-Google-760x400.png"  align="center" />


# Tweet Search Engine

Description 

The dataset contained 10 million tweets about the corona subject.
The primary goal was, given a query to retrieve the most relevant tweets from the corpus, obviously at the best runtime as possible.
Therefore, I used Multithreading programming model throughout the different preprocess parts in order to improve the runtime.


Preprocess:
-------------
- reader , Read the tweets from the dataset
- parser_module , Parse the tweets according to the rules of the class
- stemmer , Stemm the tweets using Porter's stemming
- indexer , Create and store the posting file om the disk

Model:
-------------
- ranker , I implemented multiple models to rank tweets supply by GloVe, Word2Vec, WordNet, SpellChecker and Thesaurus.
- searcher , return the relevant tweets


## 💡 Prerequisite
[Python 3.7](https://www.python.org/downloads/release/python-3711/)


## 🛠️ Installation

### With Github

```bash
git clone https://github.com/samuelbenichou/SearchEngine.git
cd SearchEngine/
python3 setup.py install
```
