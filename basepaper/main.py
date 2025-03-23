from pr_sketch import PRSketch

# Sample Dataset (Manually Extracted)
book_data = [
    {"ASIN": "0827229534", "similar": ["0804215715", "156101074X", "0738700797"]},
    {"ASIN": "0738700797", "similar": ["0738700827", "1567184960"]},
    {"ASIN": "0486287785", "similar": []}
]

def main():
    # Initialize PR-Sketch
    pr_sketch = PRSketch(table_size=1000, pattern_length=2)

    # Add Book Relationships to PR-Sketch
    for book in book_data:
        asin1 = book["ASIN"]
        for asin2 in book["similar"]:
            pr_sketch.add_edge(asin1, asin2, weight=1)

    # Perform Edge Query
    asin1, asin2 = "0827229534", "0804215715"
    print(f"Edge Query ({asin1} → {asin2}):", pr_sketch.edge_query(asin1, asin2))

    # Perform Reachability Query
    asin1, asin2 = "0827229534", "1567184960"
    print(f"Reachability Query ({asin1} → {asin2}):", pr_sketch.reachability_query(asin1, asin2))

if __name__ == "__main__":
    main()
