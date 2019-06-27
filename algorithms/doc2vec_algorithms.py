"""This module defines methods to analyze relation characteristics.

"""

# standard library imports
import collections

# related third party imports
from gensim import models

# local application/library specific imports
from algorithms.algorithm import *
from assets import get_years


class Doc2vecIterator(collections.Iterator):
    def __init__(self, nodelist, cumulative, year):
        self.node_index = 0
        self.asset_index = 0
        self.cumulative = cumulative
        self.year = year
        self.nodelist = nodelist
        self.node = self.nodelist[self.node_index]
        self.assets = self.get_assets()

    @staticmethod
    def get_assets_from_node_cumulative(node, year_cum):
        for year in node.assets:
            if year <= year_cum:
                for asset in node.assets[year]:
                    yield asset
        return

    @staticmethod
    def get_assets_from_node(node, year):
        for asset in node.assets[year]:
            yield asset
        return

    def get_assets(self):
        if self.cumulative:
            return list(self.get_assets_from_node_cumulative(self.node, self.year))
        else:
            return list(self.get_assets_from_node(self.node, self.year))

    def __iter__(self):
        return self

    def next(self):
        while self.asset_index == len(self.assets) and self.node_index < len(self.nodelist):
            # switch to next node
            self.node_index = self.node_index + 1
            if self.node_index < len(self.nodelist):
                self.node = self.nodelist[self.node_index]
                self.assets = self.get_assets()
                self.asset_index = 0
        if self.asset_index < len(self.assets):
            wordlist = self.assets[self.asset_index].words_to_analyze()
            self.asset_index = self.asset_index + 1
            return models.doc2vec.TaggedDocument(words=wordlist, tags=['%s' % self.node.name])
        else:
            # Iterators must raise when done, else considered broken
            raise StopIteration

    __next__ = next  # Python 3 compatibility


class Doc2Vec(Algorithm):
    def __init__(self, window=10, epochs=10, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'Doc2Vec', cumulative, logfile_path)
        self.window = window
        self.epochs = epochs

    def run(self, asset_list, nodes, results, years=None):
        """ Runs the Bag of Words Algorithm.

        Parameters
        ----------
        asset_list : list of Asset
        nodes : Nodes
            Specifies the nodes to be analyzed (input parameter).
        results : results.Results object
            Specifies the results object (output parameter) to which results should be added.
        years : list of integer
            if years is None all years from assets in the asset_list are evaluated.

        Returns
        ----------
        None

        """
        self.start_timer()
        if years is None:
            years = get_years(asset_list)
        nodelist = nodes.nodelist
        for year in years:
            model = models.Doc2Vec(window=self.window, alpha=0.025, min_alpha=0.025)  # use fixed learning rate
            model.build_vocab(Doc2vecIterator(nodelist, self.cumulative, year))
            for epoch in range(self.epochs):
                print('Train Doc2Vec: epoch=' + str(epoch) + ' Year=' + str(year))
                model.train(Doc2vecIterator(nodelist, self.cumulative, year), total_examples=model.corpus_count,
                            epochs=self.epochs)
                model.alpha -= 0.002  # decrease the learning rate
                model.min_alpha = model.alpha  # fix the learning rate, no decay

            results.begin_bulk_insert()
            for i in range(0, len(nodelist)):
                node_1 = nodelist[i]
                for a in range(i + 1, len(nodelist)):
                    node_2 = nodelist[a]
                    if node_1.name in model.docvecs and node_2.name in model.docvecs:
                        similarity = model.docvecs.similarity(node_1.name, node_2.name)
                        results.add_edge_value(year, node_1, self.alg_name, node_2, similarity)
                    else:
                        results.add_edge_value(year, node_1, self.alg_name, node_2, 0)

            results.end_bulk_insert()
        self.stop_timer_and_log('window: ' + str(self.window) + '  epochs: ' + str(self.epochs))
