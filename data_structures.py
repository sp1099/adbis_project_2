from collections import defaultdict

class HashMap():

    def __init__(self):
        self.hash_table = defaultdict(list)
    
    def insert(self, key, value):
        self.hash_table[key].append(value)

    def get(self, key):
        return self.hash_table[key]
    
class PropertyTable():

    def __init__(self, table={}):
        self.table = []
        for subject, objects in table.items():
            for object in objects:
                self.table.append({"subject": subject,
                                    "object": object[0],
                                    "data": object[1]
                                    })

    def insert(self, subject, object, data=[]):
        self.table.append({"subject": subject,
                            "object": object,
                            "data": data
                            })
        
    def print_table(self, length=10):
        for items in self.table[:length]:
            print(items)
        print()