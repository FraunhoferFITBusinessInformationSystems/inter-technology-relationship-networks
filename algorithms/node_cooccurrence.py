"""This module defines the node co occurrence algorithm.

"""

# standard library imports
# None

# related third party imports
# None

# local application/library specific imports
from algorithms.algorithm import *
from assets import get_years


class NodeCoOccurrence(Algorithm):
    def __init__(self, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'node_cooc', cumulative, logfile_path)

    def run(self, asset_list, nodes, results, years=None):
        """ Runs the Word Co-Occurrence Algorithm.

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
        count = {}
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
        # creates results for each year
        for i in range(0, len(nodelist)):
            node_1 = nodelist[i]
            for a in range(i+1, len(nodelist)):
                node_2 = nodelist[a]
                for year in years_to_evaluate:
                    count[(year, node_1, node_2)] = 0
                    try:
                        for asset in node_1.assets[year]:
                            if asset in node_2.assets[year]:
                                if (year, node_1, node_2) in count:
                                    count[(year, node_1, node_2)] = count[(year, node_1, node_2)] + 1
                                else:
                                    count[(year, node_1, node_2)] = 1
                    except KeyError:
                        continue
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
