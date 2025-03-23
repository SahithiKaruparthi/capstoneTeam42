import numpy as np
import hashlib

class PRSketch:
    def __init__(self, width, depth, pattern_length, conflict_limit=3):
        self.width = width  # Width of the hash table
        self.depth = depth  # Number of hash functions
        self.pattern_length = pattern_length
        self.conflict_limit = conflict_limit
        
        # 3D hash table storing rank, weight, and conflict list
        self.gM = np.zeros((width, width, depth), dtype=[('rank', 'i4'), ('weight', 'f4'), ('list', 'O')])
        for i in range(width):
            for j in range(width):
                for k in range(depth):
                    self.gM[i, j, k]['list'] = []
        
        # Randomized hash functions
        self.hash_funcs = [lambda x, seed=i: int(hashlib.md5((str(x) + str(seed)).encode()).hexdigest(), 16) % width for i in range(depth)]

    def pattern_hash(self, node):
        """Generate pattern for a node using multiple hash functions."""
        return [h(node) for h in self.hash_funcs]
    
    def rank_hash(self, node):
        """Generate rank values for a node."""
        np.random.seed(hash(node) % 1000)
        return np.random.permutation(self.pattern_length)
    
    def update(self, source, dest, weight=1.0):
        """Update the PR-Sketch with a new edge."""
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
        """Estimate the weight of an edge."""
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
        """Check if there is a reachable path from source to dest."""
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