from collections import defaultdict

class HashMap():
    """
    Hash map implementation
    """

    def __init__(self):
        self.hash_table = defaultdict(list)
    
    def insert(self, key, value):
        self.hash_table[key].append(value)

    def get(self, key):
        return self.hash_table[key]