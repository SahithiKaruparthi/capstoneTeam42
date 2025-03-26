import numpy as np
import hashlib
import time
from time import sleep

class PRSketch:
    def __init__(self, width, depth, pattern_length, conflict_limit=3):
        """
        Initializes the PR-Sketch data structure with given parameters.
        This involves setting up a three-dimensional matrix to store rank, weight, and conflict lists,
        ensuring efficient graph compression and query operations. Each cell in the matrix represents
        a combination of hashed source-destination pairs across multiple hash functions. The rank
        maintains a hierarchical ordering of node importance, weight tracks edge significance,
        and the conflict list helps manage hash collisions while preserving data integrity.
        Randomized hash functions are used for pattern generation, ensuring diverse mappings
        and reducing chances of hash collisions. Proper initialization of these structures is
        crucial for the PR-Sketch's ability to approximate graph structures efficiently and
        handle streaming data effectively.
        """
        self.width = width  
        self.depth = depth  
        self.pattern_length = pattern_length
        self.conflict_limit = conflict_limit
        
        self.gM = np.zeros((width, width, depth), dtype=[('rank', 'i4'), ('weight', 'f4'), ('list', 'O')])
        for i in range(width):
            for j in range(width):
                for k in range(depth):
                    self.gM[i, j, k]['list'] = []
        
        self.hash_funcs = [lambda x, seed=i: int(hashlib.md5((str(x) + str(seed)).encode()).hexdigest(), 16) % width for i in range(depth)]

    def pattern_hash(self, node):
        """
        Generates a hash pattern for the given node using multiple hash functions.
        The node is passed through several pre-defined hash functions, each returning
        a unique mapping to ensure a spread across the hash space. This method is
        crucial for indexing and accessing nodes efficiently within PR-Sketch, helping
        to balance computational cost and collision avoidance. By distributing nodes
        in a structured yet randomized manner, this function improves query accuracy
        and facilitates better compression in the PR-Sketch framework.
        """
        return [h(node) for h in self.hash_funcs]
    
    def rank_hash(self, node):
        """
        Computes a rank permutation for a node, determining its relative importance.
        This ranking is derived from a seeded random permutation of pattern positions,
        ensuring a stable but diverse ranking for different nodes. The ranking plays a
        crucial role in conflict resolution, prioritizing higher-ranked nodes for
        storage while discarding lower-ranked ones when space constraints arise. This
        method enhances PR-Sketch's effectiveness by helping to distinguish significant
        nodes from less important ones in an evolving graph structure.
        """
        np.random.seed(hash(node) % 1000)
        return np.random.permutation(self.pattern_length)
    
    def update(self, source, dest, weight=1.0):
        """
        Updates the PR-Sketch with a new edge between the source and destination nodes.
        The function first converts the nodes into strings, then generates their pattern
        hashes and rank values. These values are used to determine where to store the
        edge in the hash matrix. If the new rank is higher than the existing rank,
        it replaces the old entry; otherwise, it either accumulates weight or adds the
        edge to a conflict list. This method enables PR-Sketch to dynamically adjust
        to incoming graph data while maintaining an efficient representation of the
        most significant edges.
        """
        source = str(source)
        dest = str(dest)

        src_pattern = self.pattern_hash(source)
        dest_pattern = self.pattern_hash(dest)
        src_rank = self.rank_hash(source) 
        dest_rank = self.rank_hash(dest)

        for i in range(self.depth):
            x, y = src_pattern[i], dest_pattern[i]
            rank_val = min(src_rank[i], dest_rank[i])
            cell = self.gM[x, y, i]

            if rank_val > cell['rank']:
                cell['rank'] = rank_val
                cell['weight'] = weight
                cell['list'] = [(source, dest)]
            elif rank_val == cell['rank']:
                cell['weight'] += weight
            else:
                if len(cell['list']) < self.conflict_limit:
                    cell['list'].append((source, dest))
    
    def edge_query(self, source, dest):
        """
        Estimates the weight of an edge between the given source and destination.
        The function uses the stored hash patterns and ranks to look up the edge
        in the hash matrix. If an exact match is found, it returns the minimum
        recorded weight. Otherwise, it returns zero, indicating the absence of
        a strong connection. This query method allows efficient weight estimation
        without needing the full adjacency list, making it ideal for compressed
        graph representations.
        """
        src_pattern = self.pattern_hash(source)
        dest_pattern = self.pattern_hash(dest)
        src_rank = self.rank_hash(source)
        dest_rank = self.rank_hash(dest)
        
        min_weight = float('inf')
        found = False
        
        for i in range(self.depth):
            x, y = src_pattern[i], dest_pattern[i]
            cell = self.gM[x, y, i]
            
            if min(src_rank[i], dest_rank[i]) == cell['rank']:
                found = True
                min_weight = min(min_weight, cell['weight'])
        
        return min_weight if found else 0.0
    
    def reachability_query(self, source, dest):
        """
        Checks whether there exists a path from the source node to the destination.
        This method performs a breadth-first search over the compressed edge lists,
        exploring reachable nodes until the target is found. It efficiently determines
        connectivity without requiring explicit storage of all paths, making it well-
        suited for handling large dynamic graphs. This feature is essential for
        understanding network structures and detecting potential pathways within
        PR-Sketch's summarized representation.
        """
        source = str(source)
        dest = str(dest)
        
        visited = set()
        queue = [source]

        while queue:
            node = queue.pop(0)
            if node == dest:
                return True
            if node in visited:
                continue
            visited.add(node)
            for i in range(self.depth):
                for x in range(self.width):
                    for y in range(self.width):
                        cell = self.gM[x, y, i]
                        for pair in cell['list']:
                            if len(pair) >= 2 and pair[0] == node:
                                queue.append(pair[1])
        return False
    
            
    def stream_edges(self, file_path):
        """Simulates real-time graph streaming from a dataset."""
        with open(file_path, 'r') as file:
            for line in file:
                if line.startswith("#"):
                    continue  # Skip comments
                parts = line.strip().split()
                if len(parts) == 2:
                    source, dest = parts[0], parts[1]
                    self.update(source, dest, weight=1.0)
                    print(f"Streamed Edge: {source} -> {dest}")
                    time.sleep(0.05)  # Simulate real-time delay

    def run_queries(self, queries):
        """Runs queries in parallel while streaming edges."""
        while True:
            for (source, dest) in queries:
                edge_weight = self.edge_query(source, dest)
                reachability = self.reachability_query(source, dest)
                print(f"Edge Query ({source} -> {dest}): {edge_weight}")
                print(f"Reachability Query ({source} -> {dest}): {reachability}")
            time.sleep(2)