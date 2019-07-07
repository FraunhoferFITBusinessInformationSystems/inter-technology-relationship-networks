"""This module defines the node co occurrence algorithm.

"""

# standard library imports
# None

# related third party imports
# None

# local application/library specific imports
from algorithms.algorithm import *


class NodeCoOccurrence(Algorithm):
    def __init__(self, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'node_cooc', cumulative, logfile_path)

    def run(self, nodes, results, years=None):
        """ Runs the Word Co-Occurrence Algorithm.

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
        count = {}
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

        # create empty result dict
        for node in nodelist:
            for year in years_to_evaluate:
                for other_node in nodelist:
                    count[(year, node, other_node)] = 0

        # creates results for each year
        for node in nodelist:
            for year in years_to_evaluate:
                if year not in node.asset_count:
                    continue
                for asset in node.get_assets(year):
                    if asset.node_names is None:
                        asset.find_nodes(nodes)
                    for other_node_name in asset.node_names:
                        if other_node_name != node.name:
                            count[(year, node, nodes.nodes[other_node_name])] = \
                                count[(year, node, nodes.nodes[other_node_name])] + 1
        result_count = count
        if self.cumulative:
            result_count = self.cumulate_count(count, years_to_evaluate)

        results.begin_bulk_insert()
        for idx in result_count:
            if idx[0] in needed_result_years:
                results.add_edge_value(idx[0], idx[1], self.alg_name, idx[2], result_count[idx])
        results.end_bulk_insert()

        self.stop_timer_and_log()

    @staticmethod
    def cumulate_count(counts, years):
        """ Build aggregated results: the value of an year includes all prior years

        Parameters
        ----------
        counts : dict((year, node1, node2))
        years : list(int)

        Returns
        -------
        count_cumulated : dict((year, node1, node2))
        """
        count_cumulated = {}
        for idx in counts:
            for year in years:
                if idx[0] <= year:
                    if (year, idx[1], idx[2]) in count_cumulated:
                        count_cumulated[(year, idx[1], idx[2])] = count_cumulated[(year, idx[1], idx[2])] + counts[idx]
                    else:
                        count_cumulated[(year, idx[1], idx[2])] = counts[idx]
        return count_cumulated
