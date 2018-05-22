if __name__ == '__main__':

    all_nodes = [   {'lfs': {'size': 2, 'path': 'abc'}, 'uid': 1},
                    {'lfs': {'size': 2, 'path': 'abc'}, 'uid': 2},
                    {'lfs': {'size': 2, 'path': 'abc'}, 'uid': 3},
                    {'lfs': {'size': 2, 'path': 'abc'}, 'uid': 4}]
    slots = {'nodes':[{'lfs': {'size': 2, 'path': 'abc'}, 'uid': 2}]}

    for nodes in slots['nodes']:

        # Find the entry in the the slots list

        # TODO: [Optimization] Assuming 'uid' is the ID of the node, it
        #       seems a bit wasteful to have to look at all of the nodes
        #       available for use if at most one node can have that uid.
        #       Maybe it would be worthwhile to simply keep a list of nodes
        #       that we would read, and keep a dictionary that maps the uid
        #       of the node to the location on the list?

        node = (n for n in all_nodes if n['uid'] == nodes['uid']).next()
        # for n in self.nodes:
        #     if n['uid'] == nodes['uid']:
        #         node = n
        #         break
        assert(node)

        print '---Before---'
        print 'slot nodes: ', nodes
        print 'all nodes: ', all_nodes
        print 'selected node: ',node

        node['lfs']['size']=0

        print '---After---'
        print 'slot nodes: ', nodes
        print 'all nodes: ', all_nodes
        print 'selected node: ',node
