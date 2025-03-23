import threading
from basepaper.pr_sketch.pr_sketch import PRSketch, stream_edges, run_queries

if __name__ == "__main__":
    # Define input parameters directly in the script
    width = 1000
    depth = 5
    pattern_length = 8
    conflict_limit = 3
    file_path = "/home/pes2ug22cs632/capstoneTeam42/basepaper/dataset/web-NotreDame.txt"
    queries = [(0, 5), (5, 10)]  # List of queries as (source, destination) tuples
    
    # Initialize PR-Sketch
    pr_sketch = PRSketch(width=width, depth=depth, pattern_length=pattern_length, conflict_limit=conflict_limit)
    
    # Start the query processing thread
    threading.Thread(target=run_queries, args=(pr_sketch, queries), daemon=True).start()
    
    # Start streaming edges
    stream_edges(file_path, pr_sketch)
