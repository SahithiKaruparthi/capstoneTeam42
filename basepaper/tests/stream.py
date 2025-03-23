import threading
import test_setup
from pr_sketch.pr_sketch import PRSketch
import time



if __name__ == "__main__":
    # Define input parameters directly in the script
    width = 1000
    depth = 5
    pattern_length = 8
    conflict_limit = 3
    file_path = "basepaper/dataset/web-NotreDame.txt"
    queries = [(0, 5), (5, 10)]  # List of queries as (source, destination) tuples
    
    # Initialize PR-Sketch
    pr_sketch = PRSketch(width=width, depth=depth, pattern_length=pattern_length, conflict_limit=conflict_limit)

    # Start streaming edges
    stream_thread = threading.Thread(target=pr_sketch.stream_edges, args=(file_path,), daemon=True)
    stream_thread.start()

    time.sleep(2)
    
    # Start the query processing thread
    query_thread = threading.Thread(target=pr_sketch.run_queries, args=(queries,), daemon=True)
    query_thread.start()

    
    stream_thread.join()
    query_thread.join()