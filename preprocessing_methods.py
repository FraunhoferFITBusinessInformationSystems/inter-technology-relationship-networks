"""This module defines methods required in the pre-processing phase.

"""

# standard library imports
import string

# related third party imports
from nltk.corpus import wordnet, stopwords
from nltk.tokenize import MWETokenizer
from nltk.tokenize import sent_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.tag.perceptron import PerceptronTagger

# local application/library specific imports
from nodes import *


class NLP:
    def __init__(self, nodes):
        self.mwe_tokenizer = MWETokenizer(self._build_mwe(nodes.nodelist))
        self.lemmatizer = WordNetLemmatizer()
        self.tagger = PerceptronTagger()
        self.stop_words = set(stopwords.words("english"))

    @staticmethod
    def _build_mwe(nodelist):
        """Builds multi word expressions based on synonyms of nodes in nodelist.

        Parameters
        ----------
        nodelist: list(Node)

        Returns
        -------
        multi_word_expressions : list(str)
        """

        multi_word_expressions = []
        # iterate through synonyms in nodelist and append joined n-grams
        # (separator = "_") to above list.
        for node in nodelist:
            for idx in range(len(node.synonyms)):
                mwe = [word_tokenize(node.synonyms[idx].lower())]
                multi_word_expressions.append(tuple(mwe[0]))
        return multi_word_expressions

    def tokenize_into_words(self, text_input):
        """Splits strings into list of words with regard to multi word expressions
        generated from nodelist.

        Parameters
        ----------
        text_input : str

        Returns
        -------
        tokenized_string : list(str)
        """
        tokenized_string = self.mwe_tokenizer.tokenize(word_tokenize(text_input.lower()))
        return tokenized_string

    @staticmethod
    def tokenize_into_sentences(text_input):
        """Splits strings into list of sentences.

        Parameters
        ----------
        text_input: str

        Returns
        -------
        tokenized_string : list(str)

        """

        tokenized_string = sent_tokenize(text_input)
        return tokenized_string

    @staticmethod
    def get_wordnet_pos(treebank_tag):
        """Used to translate part of speech tags (wordnet vs. nltk).

        Parameters
        ----------
        treebank_tag : str

        Returns
        -------
        wordnet.* : str

        References
        -------
        https://stackoverflow.com/questions/15586721/wordnet-lemmatization-and-pos-tagging-in-python

        """

        if treebank_tag.startswith('J'):
            return wordnet.ADJ
        elif treebank_tag.startswith('V'):
            return wordnet.VERB
        elif treebank_tag.startswith('N'):
            return wordnet.NOUN
        elif treebank_tag.startswith('R'):
            return wordnet.ADV
        else:
            return wordnet.NOUN  # Alternatively: "" or None.

    def pos_tag_words(self, text_input):
        """Assigns part of speech tags to tokenized words.

        Parameters
        ----------
        text_input : str

        Returns
        -------
        pos_tagged_words : list(str)
        """

        sentences = sent_tokenize(text_input)
        pos_tagged_words = []
        for sentence in sentences:
            sentence_pos = self.tagger.tag(self.mwe_tokenizer.tokenize(word_tokenize(sentence)))
            for t in sentence_pos:
                pos_tagged_words.append(t)
        return pos_tagged_words

    def lemmatize_words(self, text_input):
        """Identifies the lemma of each word based on specified lemmatizer
        and part of speech tagger.

        Parameters
        ----------
        text_input : str

        Returns
        -------
        lemmatized_words : list(str)
        """

        lemmatized_words = []
        for w in self.pos_tag_words(text_input):
            if w[0] not in string.punctuation:
                lemmatized_word = self.lemmatizer.lemmatize(word=w[0],
                                                            pos=self.get_wordnet_pos(w[1]))
                lemmatized_words.append(lemmatized_word.lower())
        return lemmatized_words

    def list_based_stopword_removal(self, text_input):
        """Removes stopwords based on specified stopword list.

        Parameters
        ----------
        text_input : list(str)

        Returns
        -------
        filtered_words . list(str)
        """

        filtered_words = []
        for word in text_input:
            if word not in self.stop_words:
                filtered_words.append(word)
        return filtered_words

    def list_based_stopword_removal_for_pos_tagged_words(self, text_input):
        """Removes stopwords based on specified stopword list
        (specified for pos_tagged_words)

        Parameters
        ----------
        text_input : str

        Returns
        -------
        filtered_words : list(str)
        """

        filtered_words = []
        for word in text_input:
            if word[0] not in self.stop_words:
                filtered_words.append(word)
        return filtered_words
