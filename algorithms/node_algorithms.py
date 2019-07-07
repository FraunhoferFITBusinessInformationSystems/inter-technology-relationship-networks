"""This module defines methods to analyze node characteristics.

"""

# standard library imports
from datetime import datetime

# related third party imports
import pandas as pd

from pytrends.request import TrendReq

# local application/library specific imports
from algorithms.algorithm import *


class WordInAssetOccurrence(Algorithm):
    def __init__(self, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'WordInAssetOccurrence', cumulative, logfile_path)

    def run(self, nodes, results, years=None):
        """ Runs the word in asset occurrence algorithm.

        Parameters
        ----------
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

        # If algorithm is cumulative all existing years have to be evaluated, but the cumulation has to be
        # done only for the years specified as arguments
        years_to_evaluate = years
        if years_to_evaluate is None or self.cumulative:
            years_to_evaluate = nodes.get_years()

        if years is None:
            needed_result_years = {year: 0 for year in years_to_evaluate}
        else:
            needed_result_years = {year: 0 for year in years}

        nodelist = nodes.nodelist
        count = {}
        for node in nodelist:
            for year in years_to_evaluate:
                try:
                    asset_count = node.asset_count[year]
                except KeyError:
                    asset_count = 0
                count[(year, node)] = asset_count

        result_count = count
        if self.cumulative:
            result_count = self.cumulate_count(count, years_to_evaluate)

        results.begin_bulk_insert()
        for idx in result_count:
            if idx[0] in needed_result_years:
                results.add_node_value(idx[0], idx[1], self.alg_name, result_count[idx])
        results.end_bulk_insert()
        self.stop_timer_and_log()

    @staticmethod
    def cumulate_count(counts, years):
        """ Build aggregated results: the value of an year includes all prior years

        Parameters
        ----------
        counts : dict((year, node))
        years : list(int)

        Returns
        -------
        count_cumulated : dict((year, node1, node2))
        """
        count_cumulated = {}
        for idx in counts:
            for year in years:
                if idx[0] <= year:
                    if (year, idx[1]) in count_cumulated:
                        count_cumulated[(year, idx[1])] = count_cumulated[(year, idx[1])] + counts[idx]
                    else:
                        count_cumulated[(year, idx[1])] = counts[idx]
        return count_cumulated
