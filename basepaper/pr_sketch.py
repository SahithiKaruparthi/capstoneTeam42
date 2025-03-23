import hashlib
import random
from collections import defaultdict

class PRSketch:
    def __init__(self, table_size=1000, pattern_length=2):
        """
        Initialize PR-Sketch with given table size and pattern length.
        :param table_size: Number of hash slots
        :param pattern_length: Number of hash locations per book
        """
        self.table_size = table_size  # Width of the hash table
        self.pattern_length = pattern_length  # Number of pattern locations
        self.hash_table = defaultdict(list)  # Store (book, weight) at each location
        self.pattern_map = {}  # Store patterns for each ASIN
        self.rank_map = {}  # Store rank vectors for each ASIN
        # Add graph structure to track neighbors
        self.graph = defaultdict(set)  # For tracking neighbors directly

    def _hash_function(self, key, seed=0):
        """
        Generate a hash value for a given key (ASIN).
        :param key: The book ASIN
        :param seed: Different seeds produce different hash values
        :return: Hash value within the table size
        """
        hash_val = int(hashlib.md5((key + str(seed)).encode()).hexdigest(), 16)
        return hash_val % self.table_size

    def generate_pattern(self, asin):
        """
        Generate & store a pattern for an ASIN (Ensures consistency).
        :param asin: The book's ASIN
        :return: List of pattern locations
        """
        if asin not in self.pattern_map:
            self.pattern_map[asin] = [self._hash_function(asin, i) for i in range(self.pattern_length)]
        return self.pattern_map[asin]

    def generate_rank(self, asin):
        """
        Generate & store a rank vector for an ASIN (Ensures consistency).
        :param asin: The book's ASIN
        :return: Rank vector of length pattern_length
        """
        if asin not in self.rank_map:
            rank_vector = list(range(1, self.pattern_length + 1))
            random.shuffle(rank_vector)  # Shuffle to randomize ranking
            self.rank_map[asin] = rank_vector
        return self.rank_map[asin]

    def add_edge(self, asin1, asin2, weight=1):
        """
        Store an edge (asin1 → asin2) in the sketch.
        :param asin1: Source book ASIN
        :param asin2: Destination book ASIN
        :param weight: Strength of similarity
        """
        pattern1 = self.generate_pattern(asin1)
        pattern2 = self.generate_pattern(asin2)
        rank1 = self.rank_map.get(asin1, self.generate_rank(asin1))
        rank2 = self.rank_map.get(asin2, self.generate_rank(asin2))

        # Update graph structure for reachability queries
        self.graph[asin1].add(asin2)

        for i in range(self.pattern_length):
            loc = (pattern1[i], pattern2[i])
            rank = (rank1[i], rank2[i])

            if loc in self.hash_table:
                existing_rank = self.hash_table[loc][1]
                if rank > existing_rank:  # Keep the edge with higher rank
                    self.hash_table[loc] = [(asin1, asin2), rank, weight]
            else:
                self.hash_table[loc] = [(asin1, asin2), rank, weight]

    def edge_query(self, asin1, asin2):
        """
        Query edge weight (similarity score) between two books.
        :param asin1: Source book ASIN
        :param asin2: Destination book ASIN
        :return: Estimated edge weight
        """
        pattern1 = self.generate_pattern(asin1)
        pattern2 = self.generate_pattern(asin2)

        min_weight = float('inf')
        found = False

        for i in range(self.pattern_length):
            loc = (pattern1[i], pattern2[i])
            if loc in self.hash_table:
                found = True
                min_weight = min(min_weight, self.hash_table[loc][2])

        return min_weight if found else 0  # Return 0 if edge is not found

    def reachability_query(self, asin1, asin2, visited=None):
        """
        Check if asin1 can reach asin2 using DFS.
        :param asin1: Start book ASIN
        :param asin2: Target book ASIN
        :param visited: Set to track visited nodes
        :return: True if reachable, False otherwise
        """
        if visited is None:
            visited = set()
        
        if asin1 == asin2:
            return True

        visited.add(asin1)
        
        # Use the graph structure to find neighbors directly
        for neighbor in self.graph[asin1]:
            if neighbor not in visited:
                if self.reachability_query(neighbor, asin2, visited):
                    return True
                    
        return False