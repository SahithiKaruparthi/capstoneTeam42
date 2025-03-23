import test_setup
from pr_sketch.pr_sketch import PRSketch  # Now you can import normally

def load_book_data(pr_sketch, book_data):
    """Loads book dataset into PR-Sketch."""
    for book in book_data:
        source = book["ASIN"]
        for similar_book in book["similar"]:
            pr_sketch.update(source, similar_book, weight=1.0)

# Initialize PR-Sketch
pr_sketch = PRSketch(width=1000, depth=5, pattern_length=8)

# Sample book dataset
book_data = [
    {"ASIN": "0827229534", "similar": ["0804215715", "156101074X", "0738700797"]},
    {"ASIN": "0738700797", "similar": ["0738700827", "1567184960"]},
    {"ASIN": "0486287785", "similar": []}
]

# Load data into PR-Sketch
load_book_data(pr_sketch, book_data)

# Run queries
print(f"Edge Query (0827229534 -> 0738700797): {pr_sketch.edge_query('0827229534', '0738700797')}")
print(f"Reachability Query (0827229534 -> 1567184960): {'Yes' if pr_sketch.reachability_query('0827229534', '1567184960') else 'No'}")