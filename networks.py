"""This module defines the class Network and associated methods.

"""

# standard library imports
# None

# related third party imports
import networkx as nx


# local application/library specific imports
# None


class Network:
    def __init__(self):
        self.graph = self.df_for_graph = self.node_attribute = None

    def generate_multi_graph(self, nodelist, results, years, data_dir, edge_algorithms,
                             node_algorithms=None):
        """Generates a multi graph and writes it as an 'graph_year.gexf' file
        (gephi format) to disk.

        Parameters
        ----------
        nodelist : list(networks.Node object)
            Specifies the nodes (i.e., technologies) to be included in the network(s)
            and provides information about these nodes.
        results : results.Results object
            Specifies the results object from which data is used to build the network(s).
        years : list(str)
            Specifies the years for which networks are generated.
        data_dir : str
            Specifies the data directory to which the network are saved to.
        edge_algorithms : list(str)
            Specifies which edge values are included in the network file.
        node_algorithms : list(str)
            Specifies which edge values are included in the network file.

        Returns
        -------
        None

        """
        df = results.df
        for year in years:
            self.graph = nx.MultiGraph(time=year)
            for node in nodelist:
                self.graph.add_node(node.name, name=node.name)
                if node_algorithms is not None:
                    for node_algorithm in node_algorithms:
                        self.node_attribute = {}
                        df_for_graph = df.loc[(df['Node'] == node.name) &
                                              (df['Year'] == year) &
                                              (df['AlgName'] == node_algorithm)]
                        print(df_for_graph)
                        self.node_attribute[node.name] = \
                            int(df_for_graph.iloc[0]['NodeValue'])
                        nx.set_node_attributes(self.graph, node_algorithm,
                                               self.node_attribute)
                else:
                    pass
                for edge_algorithm in edge_algorithms:
                    df_for_graph = df.loc[(df['Node'] == node.name) &
                                          (df['Year'] == year) &
                                          (df['AlgName'] == edge_algorithm)]
                    print(df_for_graph)
                    if len(df_for_graph) > 0:
                        for i in range(0, len(df_for_graph)):
                            self.graph.add_edge(node.name,
                                                df_for_graph.iloc[i]['EdgeToNode'],
                                                key=edge_algorithm,
                                                weight=float(df_for_graph.iloc[i]
                                                             ['EdgeValue']))
            print(nx.info(self.graph))
            file_name = "graph_" + str(year) + ".gexf"
            file_directory = data_dir + file_name
            nx.write_gexf(self.graph, file_directory)
