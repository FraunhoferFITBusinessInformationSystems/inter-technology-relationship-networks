""""This module defines the classes to manage nodes.

"""

# standard library imports
import os
# None

# related third party imports
from nltk import word_tokenize

# local application/library specific imports
from assets import *
# None


class Node:
    def __init__(self, name, query):
        """Defines a node in an network based on a boolean query

        Parameters
        ----------
        name : str
            The name of the node.
        query : str
            a query with search strings combined with AND, OR, (, ).
        """
        self.name = name
        self.nodes = None
        self.syndict = {}
        self.synonyms = []

        # asset counts per year
        self.asset_count = {}
        self.query = query
        self.expression = None

        words = query.split('"')
        for i in range(1, len(words), 2):
            self.synonyms.append(words[i])
        self.build_query_expression()

        # fill tokenized synonym dictionary
        for syn in self.synonyms:
            mwe = [word_tokenize(syn.lower())]
            self.syndict['_'.join(mwe[0])] = None

        # try to evaluate expression to check correct expression
        try:
            self.node_matches_found_synonyms(self.synonyms)
        except Exception as e:
            print("Node " + self.name + ": incorrect query '" + self.query + "'")
            print("     " + str(e))
            raise

    def build_query_expression(self):
        self.expression = self.query

        # replace string constants in query with code to evaluate the condition based on the tokenized value
        for syn in self.synonyms:
            mwe = [word_tokenize(syn.lower())]
            token_syn = '_'.join(mwe[0])
            self.expression = self.expression.replace('"' + syn + '"', "'" + token_syn + "' in words ")
        self.expression = self.expression.replace(' OR ', ' or ')
        self.expression = self.expression.replace(' AND ', ' and ')
        self.expression = self.expression.replace(' NOT ', ' not ')
        return self.expression

    def node_matches_found_synonyms(self, words):
        # starttime = perf_counter()
        exp_as_func = eval('lambda words: ' + self.expression)
        result = exp_as_func(words)
        # print("NodeEvalDuration:" + str (perf_counter() - starttime))
        return result

    def node_matches_raw_text(self, text):
        text_to_compare = text.lower()
        for syn in self.synonyms:
            if syn.lower() in text_to_compare:
                return True
        return False

    def get_words(self, year):
        try:
            for asset in self.get_assets(year):
                for word in asset.words_to_analyze():
                    yield word
        except KeyError:
            return
        return

    def get_assets(self, year):
        with open(self._pickle_file_path(year), 'rb') as fp:
            try:
                while True:
                    asset = pickle.load(fp)
                    yield asset
            except EOFError:
                return

    def get_words_cumulative(self, year_cum):
        for year in self.asset_count:
            if year <= year_cum:
                for asset in self.get_assets(year):
                    for word in asset.words_to_analyze():
                        yield word
        return

    def _pickle_file_name(self):
        return "AssetWords_" + self.name.replace("/", "_") + "_"

    def _pickle_file_path(self, year):
        return self.nodes.asset_tmp_dir + self._pickle_file_name() + str(year)

    def _update_counts(self, asset):
        if asset.year not in self.asset_count:
            self.asset_count[asset.year] = 0
        self.asset_count[asset.year] = self.asset_count[asset.year] + 1

    def append_asset(self, asset):
        internal_asset = AssetWords(asset.year, asset)
        with open(self._pickle_file_path(asset.year), 'ab+') as fp:
            pickle.dump(internal_asset, fp)
        self._update_counts(asset)

    def remove_assets(self):
        self.asset_count = {}

    def read_assets(self):
        start = timeit.default_timer()
        node_filename = self._pickle_file_name()
        self.asset_count = {}
        for file in os.scandir(self.nodes.asset_tmp_dir):
            if file.name.startswith(node_filename):
                year = int(file.name[-4:])
                for asset in self.get_assets(year):
                    self._update_counts(asset)
        stop = timeit.default_timer()
        runtime = stop - start
        count = 0
        for year in self.asset_count:
            count = count + self.asset_count[year]
        print(self.name + ': finished reading ' + str(count) + ' assets from disk. Duration: ' + str(runtime))

class Nodes:
    def __init__(self, asset_tmp_dir):
        self.nodelist = []
        self.nodes = {}

        # directory to temporary save assets words
        self.asset_tmp_dir = asset_tmp_dir + "perNode/"
        if not os.path.exists(self.asset_tmp_dir):
            os.makedirs(self.asset_tmp_dir)

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

        # keep reference to class containing all nodes
        node.nodes = self

        for syn in node.syndict.keys():
            if syn not in self.node_synonyms:
                self.node_synonyms[syn] = []
            self.node_synonyms[syn].append(node)

    def get_nodes_by_synonym(self, word):
        """Return the nodes, that contains a condition with the given word

        Parameters
        ----------
        word : str

        Returns
        -------
        nodes : Nodes
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
            if self.get_nodes_by_synonym(word) is not None:
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
                    node.append_asset(asset)

    def remove_assets(self):
        for file in os.scandir(self.asset_tmp_dir):
            os.unlink(file.path)
        for node in self.nodelist:
            node.remove_assets()

    def read_assets(self):
        for node in self.nodelist:
            node.read_assets()

    def get_years(self):
        """Creates a list of years.

        Determines the earliest as well as the latest year in the given assetlist and
        returns a list of years with the minimum and maximum values as boundaries.

        Parameters
        ----------
        Returns
        -------
        range_of_years : list(int)
        """

        years = []
        for node in self.nodelist:
            for year in node.asset_count:
                if year not in years:
                    years.append(year)
        min_year = min(int(i) for i in years)
        max_year = max(int(i) for i in years)
        range_of_years = range(min_year, max_year+1)
        return range_of_years
