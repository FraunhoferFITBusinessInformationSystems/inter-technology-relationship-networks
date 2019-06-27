""""This module defines the classes to manage nodes.

"""

# standard library imports
# None

# related third party imports
from nltk import word_tokenize

# local application/library specific imports
# None


class Node:
    def __init__(self, name, synonyms):
        """Defines a node in an network

        Parameters
        ----------
        name : str
            The name of the node.
        synonyms : list(str)
            A list of synonyms, that are used for searching in assets.
        """
        self.name = name
        self.syndict = {}
        self.synonyms = synonyms
        self.assets = {}

        # fill tokenized synonym dictionary
        for syn in synonyms:
            mwe = [word_tokenize(syn.lower())]
            self.syndict['_'.join(mwe[0])] = None

    def node_matches_word(self, word):
        if self.syndict.__contains__(word):
            return True
        else:
            return False

    def node_matches_raw_text(self, text):
        text_to_compare = text.lower()
        for syn in self.synonyms:
            if syn.lower() in text_to_compare:
                return True
        return False


class Nodes:
    def __init__(self,):
        self.nodelist = []
        self.nodes = {}

        # dictionary to get the node by any synonym
        self.node_synonyms = {}

    def add_node(self, node):
        """Adds a new node to the nodelist

        Parameters
        ----------
        node : Node

        Returns
        -------

        """
        self.nodelist.append(node)
        self.nodes[node.name] = node
        for syn in node.syndict.keys():
                self.node_synonyms[syn] = node

    def get_node_by_synonym(self, word):
        """Return the node, that matches the given word

        Parameters
        ----------
        word : str

        Returns
        -------
        node : Node
        """
        return self.node_synonyms.get(word, None)

    def is_node_in_text(self, words):
        """Returns true if the given list of words contains a synonym of any node

        Parameters
        ----------
        words : list(str)

        Returns
        -------
        : bool
        """
        for word in words:
            if self.get_node_by_synonym(word) is not None:
                return True
        return False

    def enrich_with_assets(self, assetlist):
        """Associate the nodes in the nodelist with the preprocessed assets
        This is necessary to speed up the subsequent edge and node algorithms

        Parameters
        ----------
        assetlist: list(Asset)

        Returns
        -------

        """
        for asset in assetlist:
            if len(asset.node_names) > 0:
                for name in asset.node_names:
                    node = self.nodes[name]
                    if asset.year not in node.assets:
                        node.assets[asset.year] = []
                    node.assets[asset.year].append(asset)
