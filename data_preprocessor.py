from collections import defaultdict

from data_structures import PropertyTable

class DataPreprocessor():
    
    def __init__(self, data_path, properties):
        """
        Initialize the data preprocessor and preprocess the data
        
        Parameters
        ----------
        data_path : str
            Path to the data file
        properties : list
            List of relevant properties

        """
        self.data_path = data_path
        self.properties = properties
        # property_tables_int contains for each property a list of objects and for each object a list of subjects
        self.property_tables_int = {property_name: defaultdict(list) for property_name in properties}

        # for each unique subject and for each unique object in the data, the rdf_dict has a unique index
        self.rdf_dict = defaultdict()

        # the rdf_dict_reversed has for each index the corresponding subject or object
        # this is used to get the subject or object strings from the respective index
        self.rdf_dict_reversed = defaultdict()

        # preprocess the data by partitioning the data into different property tables
        self.partition_data()
        # fill rdf_dict_reversed so that it contains the indices as keys and the corresponding subjects and objects as values
        self.reverse_dict()


    def partition_data(self):
        """
        Partition the data (triples) into different property tables
        Property tables are here represented as a dictionary with the property name as key and a dictionary as value
        The dictionary as value contains the objects as keys and a list of subjects as values
        """

        # open the data file and read line by line
        with open(self.data_path, 'r') as data_file:
            for line in data_file:
                # remove the newline character
                line = line.strip('\n.')

                # split the line into subject, property and object
                subject, property, *object = line.split()
                # join the object list into a string seperated by space
                object = ' '.join(object)

                # remove the prefix of the subject, property and object
                subject = self.remove_prefix(subject)
                property = self.remove_prefix(property)
                object = self.remove_prefix(object)

                # check if the property is in the list of relevant properties
                if property in self.properties:
                    # add the subject and object of the triple to the corresponding property table
                    self.add_to_dict(subject, object)
                    # get the corresponding indices of the subject and object
                    subject_int, object_int = self.rdf_dict[subject], self.rdf_dict[object]

                    # add the subject to the list of subjects of the object in the corresponding property table
                    self.property_tables_int[property][object_int].append(subject_int)

        # print(self.partion_tables)

    def remove_prefix(self, string):
        """
        Remove the prefix of the given string from the data and return the string without the prefix

        Parameters
        ----------
        string : str
            String from the data

        Returns
        -------
        str
            String without the prefix
        """

        # check if the string is from the watdiv.10M.nt dataset (subject and object are surrounded by < and >)
        if string[-1] == '>':
            string = string[:-1]

            # traverse the string from the end
            for index, char in enumerate(string[::-1]):
                # when the first non-digit and non-alphabet character is found, return the string from this index
                if not char.isdigit() and not char.isalpha():
                    return string[-index:]
        
        # the data is from the 100k.txt dataset
        # check if the string is a value (surrounded by ")
        elif string[-1] == '"':
            return string
        else:
            # return the string after the first occurence of :
            return string[string.find(':') + 1:]
        
    def add_to_dict(self, subject, object):
        """
        Add the subject and object to the rdf_dict if they are not already in the dictionary

        Parameters
        ----------
        subject : str
            Subject of the triple
        object : str
            Object of the triple
        """
        if subject not in self.rdf_dict:
            self.rdf_dict[subject] = len(self.rdf_dict) + 1
        if object not in self.rdf_dict:
            self.rdf_dict[object] = len(self.rdf_dict) + 1

    def reverse_dict(self):
        """
        Fill the rdf_dict_reversed so that it contains the indices as keys and the corresponding subjects or objects as values
        """
        for key, value in self.rdf_dict.items():
            self.rdf_dict_reversed[value] = key


if __name__ == '__main__':
    data_path = 'data/100k.txt'
    properties = ['follows', 'friendOf', 'likes', 'hasReview']
    data_preprocessor = DataPreprocessor(data_path, properties)
    data_preprocessor.partition_data()