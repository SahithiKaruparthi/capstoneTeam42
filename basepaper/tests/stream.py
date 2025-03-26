import threading  
# Import threading module for concurrent execution.
# This allows us to run multiple tasks in parallel.
# Here, we use it to handle streaming and querying simultaneously.

import test_setup  
# Import test setup module.
# This is assumed to contain necessary configurations or utilities.
# It might help in initializing dependencies for PRSketch.

from pr_sketch.pr_sketch import PRSketch  
# Import PRSketch class from the pr_sketch module.
# PRSketch is the core data structure we are using.
# It enables efficient pattern-based graph compression.

import time  
# Import time module to introduce delays where needed.
# Used here to ensure queries run after some data has been processed.
# Helps prevent race conditions in multi-threaded execution.

if __name__ == "__main__":  
# This ensures the script runs only when executed directly.
# Prevents accidental execution when imported as a module.
# A standard practice in Python scripts.

    width = 1000  
    depth = 5  
    pattern_length = 8  
    conflict_limit = 3  

    # Defines the width of the PRSketch data structure.
    # Affects the number of entries available for sketching.
    # Higher width generally improves accuracy but increases memory use.

    # Defines the depth of PRSketch.
    # Represents the number of hash functions used.
    # Greater depth reduces collisions but increases computation cost.

    # Specifies the length of patterns stored in PRSketch.
    # Longer patterns capture more complex structures.
    # However, they also increase storage and processing requirements.

    # Sets the conflict limit for PRSketch.
    # Determines how many hash collisions are tolerated.
    # Beyond this, conflicting entries may be discarded or replaced.

    file_path = "/home/pes2ug22cs632/capstoneTeam42/basepaper/dataset/web-NotreDame.txt"  
    # Defines the file path to the input graph dataset.
    # This dataset contains edges representing web page links.
    # Used to simulate real-world graph compression scenarios.

    queries = [(0, 5), (5, 10)]  
    # Specifies queries as (source, destination) tuples.
    # Each query checks connectivity between two nodes.
    # These queries will run after some edges have been streamed.

    pr_sketch = PRSketch(width=width, depth=depth, pattern_length=pattern_length, conflict_limit=conflict_limit)  

    # Initializes the PRSketch instance with specified parameters.
    # This sets up the data structure for streaming and querying.
    # Ensures the system is ready to process incoming edges efficiently.

    stream_thread = threading.Thread(target=pr_sketch.stream_edges, args=(file_path,), daemon=True)  
    stream_thread.start()  
    # Creates a separate thread for streaming edges.
    # This allows edge processing to happen in the background.
    # The stream_edges method continuously feeds data from the file.

    # Starts the streaming thread.
    # Begins processing the graph dataset asynchronously.
    # This ensures data is available for querying.

    time.sleep(2)  
    # Introduces a delay before starting queries.
    # Ensures that some edges have already been processed.
    # Prevents premature querying on an empty dataset.

    query_thread = threading.Thread(target=pr_sketch.run_queries, args=(queries,), daemon=True)  
    query_thread.start()  
    # Creates another thread for query processing.
    # This runs in parallel with edge streaming.
    # Allows real-time querying while new data is added.

    # Starts the query processing thread.
    # Executes queries on the PRSketch data structure.
    # These queries check if paths exist between specified nodes.

    stream_thread.join()  
    query_thread.join()  
    # Waits for the streaming thread to complete.
    # Ensures all edges are processed before proceeding.
    # Prevents premature termination of the program.

    # Waits for the query thread to complete.
    # Ensures all queries finish execution.
    # Prevents abrupt script termination before results are obtained.
