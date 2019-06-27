"""This module defines methods to analyze node characteristics.

"""

# standard library imports
from datetime import datetime

# related third party imports
import pandas as pd

from pytrends.request import TrendReq

# local application/library specific imports
import credentials
from assets import get_years
from algorithms.algorithm import *


class GoogleTrends(Algorithm):
    def __init__(self, logfile_path=None):
        Algorithm.__init__(self, 'GoogleTrends', False, logfile_path)

    def run(self, asset_list, nodes, results, years=None):
        """ Gets the individual interest over time (google trends) by using the pytrends library.

        Parameters
        ----------
        asset_list : list of Asset
        nodes : Nodes
            Specifies the nodes to be analyzed (input parameter).
        results : results.Results object
            Specifies the results object (output parameter) to which results should be added.
        years : list of integer
            This parameter is not supported

        Returns
        ----------
        None

        """
        self.start_timer()
        google_username = credentials.Google["username"]
        google_password = credentials.Google["password"]
        trend = TrendReq(google_username, google_password, custom_useragent='My Pytrends Script')
        # trend = TrendReq()
        alg_name = 'individual_gtrend'

        for node in nodes.nodelist:
            suggestions_dict = trend.suggestions(keyword=node.name)
            query = node.name
            for suggestion in suggestions_dict:
                if suggestion['type'] == \
                        "Thema" and (suggestion['title'] == node.name or suggestion['title'] in node.synonyms):
                    query = suggestion['title']
                    break
            trend.build_payload(kw_list=[str(query)], cat=0, timeframe="all")
            print("GoogleTrendQuery:", query)
            interest_over_time_df = trend.interest_over_time()
            if len(interest_over_time_df) > 0:          # check if google found any results
                interest_by_year_df = interest_over_time_df.groupby(pd.TimeGrouper(freq='12M', closed='left')).mean()
                for row in interest_by_year_df.itertuples():
                    results.add_node_value(int(row[0].year), node, alg_name, row[1])
            else:
                actual_date = datetime.now()
                for year in range(2004, actual_date.year):
                    results.add_node_value(year, node, alg_name, 0)

        self.stop_timer_and_log()


class WordInAssetOccurrence(Algorithm):
    def __init__(self, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'WordInAssetOccurrence', cumulative, logfile_path)

    def run(self, asset_list, nodes, results, years=None):
        """ Runs the word in asset occurrence algorithm.

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

        # If algorithm is cumulative all existing years have to be evaluated, but the cumulation has to be
        # done only for the years specified as arguments
        years_to_evaluate = years
        if years_to_evaluate is None or self.cumulative:
            years_to_evaluate = get_years(asset_list)

        if years is None:
            needed_result_years = {year: 0 for year in years_to_evaluate}
        else:
            needed_result_years = {year: 0 for year in years}

        nodelist = nodes.nodelist
        count = {}
        for node in nodelist:
            for year in years_to_evaluate:
                try:
                    asset_count = len(node.assets[year])
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
