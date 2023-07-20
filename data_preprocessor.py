from collections import defaultdict

from data_structures import PropertyTable

class DataPreprocessor():
    
    def __init__(self, data_path, properties):
        self.data_path = data_path
        self.properties = properties
        self.partion_tables_int = {property_name: defaultdict(list) for property_name in properties}
        self.rdf_dict = defaultdict()
        self.rdf_dict_reversed = defaultdict()

        self.partition_data()
        self.reverse_dict()


    def partition_data(self):

        with open(self.data_path, 'r') as data_file:
            for line in data_file:
                line = line.strip('\n.')
                subject, property, *object = line.split()
                object = ' '.join(object)
                subject = self.remove_prefix(subject)
                property = self.remove_prefix(property)
                object = self.remove_prefix(object)

                if property in self.properties:
                    self.add_to_dict(subject, object)
                    subject_int, object_int = self.rdf_dict[subject], self.rdf_dict[object]
                    self.partion_tables_int[property][object_int].append(subject_int)

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
        
    def add_to_dict(self, subject, object):
        if subject not in self.rdf_dict:
            self.rdf_dict[subject] = len(self.rdf_dict) + 1
        if object not in self.rdf_dict:
            self.rdf_dict[object] = len(self.rdf_dict) + 1

    def reverse_dict(self):
        for key, value in self.rdf_dict.items():
            self.rdf_dict_reversed[value] = key


if __name__ == '__main__':
    data_path = 'data/100k.txt'
    properties = ['follows', 'friendOf', 'likes', 'hasReview']
    data_preprocessor = DataPreprocessor(data_path, properties)
    data_preprocessor.partition_data()