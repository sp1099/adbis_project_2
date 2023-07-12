from collections import defaultdict

from data_structures import PropertyTable

class DataPreprocessor():
    
    def __init__(self, data_path, properties):
        self.data_path = data_path
        self.properties = properties
        self.partion_tables = {property: PropertyTable() for property in properties}


    def partition_data(self):

        with open(self.data_path, 'r') as data_file:
            for line in data_file:
                line = line.strip('\n.')
                subject, property, *object = line.split()
                object = ' '.join(object)
                subject = self.remove_prefix(subject)
                property = self.remove_prefix(property)
                object = self.remove_prefix(object)

                if property in self.partion_tables:
                    self.partion_tables[property].insert(subject, object)

        # print(self.partion_tables)

    def remove_prefix(self, string):
        if string[-1] == '>':
            string = string[:-1]
            for index, char in enumerate(string[::-1]):
                if not char.isdigit() and not char.isalpha():
                    return string[-index:]
        elif string[-1] == '"':
            return string
        else:
            return string[string.find(':') + 1:]


if __name__ == '__main__':
    data_path = 'data/100k.txt'
    properties = ['follows', 'friendOf', 'likes', 'hasReview']
    data_preprocessor = DataPreprocessor(data_path, properties)
    data_preprocessor.partition_data()