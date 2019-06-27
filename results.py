"""This module defines the class results and associated methods.

"""

# standard library imports
import pandas as pd
import numpy as np

# related third party imports
# None

# local application/library specific imports
# None


class Results:
    def __init__(self):
        self.bulk_mode = False
        self.new_edge_values = []
        self.new_node_values = []
        self.df = pd.DataFrame(columns=['Year', 'Node', 'AlgName', 'NodeValue', 'EdgeToNode', 'EdgeValue'])

    def add_node_value(self, year, node, alg_name, node_value):
        """Add a node value to the results data frame

        Parameters
        ----------
        year : int
        node : Node
        alg_name : str
        node_value : float

        Returns
        -------

        """
        if self.bulk_mode:
            self.new_node_values.append({'Year': year, 'Node': node.name, 'AlgName': alg_name, 'NodeValue': node_value,
                                         'EdgeToNode': '', 'EdgeValue': np.nan})
        else:
            self.df.loc[len(self.df)] = [year, node.name, alg_name, node_value, '', np.nan]

    def add_edge_value(self, year, node, alg_name, edge_to_node, edge_value):
        """Add an edge value to the results data frame

        Parameters
        ----------
        year : int
        node : Node
        alg_name : str
        edge_to_node : Node
        edge_value : float

        Returns
        -------

        """
        if self.bulk_mode:
            self.new_edge_values.append({'Year': year, 'Node': node.name, 'AlgName': alg_name, 'NodeValue': np.nan,
                                         'EdgeToNode': edge_to_node.name, 'EdgeValue': edge_value})
        else:
            self.df.loc[len(self.df)] = [year, node.name, alg_name, np.nan, edge_to_node.name, edge_value]

    def begin_bulk_insert(self):
        """Speeds up inserting of new records into the results data frame
        New records are buffered until end_bulk_insert is called
        Returns
        -------

        """
        self.new_edge_values = []
        self.new_node_values = []
        self.bulk_mode = True

    def end_bulk_insert(self):
        """Speeds up inserting of new records into the results data frame
        Use together with begin_bulk_insert
        Returns
        -------

        """
        if len(self.new_edge_values) > 0:
            self.df = self.df.append(pd.DataFrame(self.new_edge_values))
        if len(self.new_node_values) > 0:
            self.df = self.df.append(pd.DataFrame(self.new_node_values))
        self.new_edge_values = []
        self.new_node_values = []
        self.bulk_mode = False

    def save_to_csv(self, result_file):
        """Save the content of the results data frame to a csv formatted file

        Parameters
        ----------
        result_file : str
            path of the file

        Returns
        -------

        """
        self.df.to_csv(result_file, columns=['Year', 'AlgName', 'Node', 'NodeValue', 'EdgeToNode', 'EdgeValue'],
                       sep=";", header=True, decimal=",")

    def add_cumulative_node_results(self, nodelist, alg_name):
        """Add cumulative results for node values

        Parameters
        ----------
        nodelist : list(Node)
        alg_name : str

        Returns
        -------
        df : DataFrame
        """
        for node in nodelist:
            df_c = self.df.loc[(self.df['AlgName'] == alg_name) & (self.df['Node'] == node.name)]
            df_c = df_c.sort_values('Year', ascending=True)
            df_c['cum_sum'] = df_c['NodeValue'].cumsum()
            df_c = df_c.drop('NodeValue', 1)
            df_c = df_c.rename(columns={'cum_sum': 'NodeValue'})
            df_c['AlgName'] = 'c_' + df_c['AlgName'].astype(str)
            self.df = self.df.append(df_c)[['Year', 'Node', 'AlgName', 'NodeValue', 'EdgeToNode', 'EdgeValue']]
        return self.df

    def add_cumulative_edge_results(self, nodelist, alg_name):
        """Add cumulative results for edge values

        Parameters
        ----------
        nodelist : list(Node)
        alg_name : str

        Returns
        -------
        df : DataFrame
        """
        for node in nodelist:
            df_c = self.df.loc[(self.df['AlgName'] == alg_name) & (self.df['Node'] == node.name)]
            if len(df_c) > 0:
                df_c = df_c.sort_values(['Node', 'EdgeToNode', 'Year'], ascending=True)
                df_c['cumsum'] = df_c.groupby(by=['Node', 'EdgeToNode'])['EdgeValue'].transform(pd.Series.cumsum)
                df_c = df_c.drop('EdgeValue', 1)
                df_c = df_c.rename(columns={'cumsum': 'EdgeValue'})
                df_c['AlgName'] = 'c_' + df_c['AlgName'].astype(str)
                self.df = self.df.append(df_c)[['Year', 'Node', 'AlgName', 'NodeValue', 'EdgeToNode', 'EdgeValue']]
        return self.df
