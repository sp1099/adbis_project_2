from collections import defaultdict
import time
import sys

from data_structures import HashMap
from data_preprocessor import DataPreprocessor

class JoinAlgorithm():
    def __init__(self, algorithm_type, preprocessor : DataPreprocessor, output_path, use_yannakakis):
        """
        Initialize the join algorithm

        Parameters
        ----------
        algorithm_type : str
            Type of the join algorithm
        preprocessor : DataPreprocessor
            Preprocessor that contains the property tables
        output_path : str   
            Path to the output file
        use_yannakakis : bool
            If True, yannakakis algorithm is used
        """
        self.algorithm_type = algorithm_type
        self.output_path = output_path
        self.property_tables_int = preprocessor.property_tables_int
        self.rdf_dict = preprocessor.rdf_dict_reversed

        # map the objects to the subjects of the property tables
        self.map_objects_to_subjects(use_yannakakis)

    def run(self):
        print("Start running hash join")
        start_time = time.time()

        if self.algorithm_type == "hash_join":
            self.hash_join()
        elif self.algorithm_type == "sort_merge_join":
            self.sort_merge_join()
        else:
            raise ValueError("Algorithm type not supported")
        
        end_time = time.time()
        print("Finish running")
        print("Time: ", end_time - start_time)
        return end_time - start_time

    def get_info(self):
        """
        Get information about the size of property tables
        Calling this method only returns valid results after map_objects_to_subjects() was called (in init)

        Returns
        -------
        size : dict
            Dictionary that contains the size (size of subjects and objects) of each property table
        """
        # get all subjects in the property table follows
        subjects_of_follows = set(subject for subject_lists in self.property_tables_int['follows'].values() for subject in subject_lists)
        number_of_subjects_follows = len(subjects_of_follows)

        # return the size of each property table (number of subjects and objects using subjects_of_...)
        return {
            'follows': (number_of_subjects_follows, len(self.objects_of_follows)),
            'friendOf': (len(self.subjects_of_friendOf), sum(len(objects) for objects in self.subjects_of_friendOf.values())),
            'likes': (len(self.subjects_of_likes), sum(len(objects) for objects in self.subjects_of_likes.values())),
            'hasReview': (len(self.subjects_of_hasReview), sum(len(objects) for objects in self.subjects_of_hasReview.values())),
        }

    def map_objects_to_subjects(self, use_yannakakis):
        """
        Create a dictionary for each property table so that it contains the subjects as keys and a list of objects as values
        This is done for the property tables follows, likes, friendOf and hasReview
        The property table hasReview is not handled specially with yannakakis because it is the last property table in the query

        Parameters
        ----------
        use_yannakakis : bool
            If True, the property tables of a join are already filtered based on if the object of the first property table of the join is present as a subject in the second property table of the join
            This is done to reduce the size of the property tables and therefore the memory usage and time
        """

        # build the property table for hasReview
        self.subjects_of_hasReview = defaultdict(set)
        for obj, subjects in self.property_tables_int['hasReview'].items():
            for subj in subjects:
                self.subjects_of_hasReview[subj].add(obj)

        # build the property table for likes considering the relation
        # likes.object = hasReview.subject
        self.subjects_of_likes = defaultdict(set)
        for obj, subjects in self.property_tables_int['likes'].items():
            if use_yannakakis:
                # check if the object of likes is a subject of hasReview (all done for the indices)
                if obj in self.subjects_of_hasReview:
                    # if yes, add these subjects of likes as keys to the dictionary and the object as value
                    for subj in subjects:
                        self.subjects_of_likes[subj].add(obj)
            else:
                for subj in subjects:
                    self.subjects_of_likes[subj].add(obj)

        # build the property table for friendOf considering the relation
        # friendOf.object = likes.subject
        self.subjects_of_friendOf = defaultdict(set)
        for obj, subjects in self.property_tables_int['friendOf'].items():
            if use_yannakakis:
                if obj in self.subjects_of_likes:
                    for subj in subjects:
                        self.subjects_of_friendOf[subj].add(obj)
            else:
                for subj in subjects:
                    self.subjects_of_friendOf[subj].add(obj)

        # build the property table for follows considering the relation
        # friendOf.subject = follows.object
        self.objects_of_follows = set()
        for obj, subjects in self.property_tables_int['follows'].items():
            if use_yannakakis:
                if obj in self.subjects_of_friendOf:
                    self.objects_of_follows.add(obj)
            else:
                self.objects_of_follows.add(obj)

        print("hasReview:", len(self.subjects_of_hasReview))
        print("likes:", len(self.subjects_of_likes))
        print("friendOf:", len(self.subjects_of_friendOf))
        print("follows:", len(self.objects_of_follows))


    def hash_join(self):
        """
        Hash join for the query of the assignment
        """
        print("Start running hash join")
        start_time = time.time()
        # first join: follows.object = friendOf.subject
        objects_of_friendsOf = self.hash_join_single(self.objects_of_follows, self.subjects_of_friendOf)
        # second join: first join resulting objects = likes.subject
        objects_of_likes = self.hash_join_single(objects_of_friendsOf, self.subjects_of_likes)
        # third join: second join resulting objects = hasReview.subject
        objects_of_hasReview = self.hash_join_single(objects_of_likes, self.subjects_of_hasReview)
        end_time = time.time()
        print("Finish running")
        print("Time: ", end_time - start_time)
        print("Size in GB: ", sys.getsizeof(objects_of_hasReview) / 1000 / 1000 / 1000)

        # TODO: UNCOMMENT THIS LINE TO WRITE THE RESULTS TO A FILE
        # self.collect_results(objects_of_hasReview)

    def hash_join_single(self, objects_from_left_join_table, subjects_objects_of_right_join_table):
        """
        Hash join for a single property table

        Parameters
        ----------
        objects : set
            Property table that should be joined and will be used as hash table
        subjects_objects : dict
            Property table that should be joined

        Returns
        -------
        result : set
            Result of the hash join

        """

        # create a hash table for the property table given in objects
        hash_table = HashMap()
        for object in objects_from_left_join_table:
            # calculate the hash of the object
            hash = self.hash_function(object)
            # insert the object with hash into the hash table
            hash_table.insert(hash, object)

        # probe hash table
        result = set()
        # iterate over the subjects and objects of the property table (subjects_objects) that should be joined
        for subject, objects_from_right_join_table in subjects_objects_of_right_join_table.items():
            # calculate the hash of the subject
            hash = self.hash_function(subject)
            # get the objects of the hash table with the same hash
            hashed_objects = hash_table.get(hash)
            # if the actual subject index is in the returned list associated with the hash (because of hash collisions)
            if subject in hashed_objects:
                # store the objects of the right join table in the result that are given by the join
                result.update(objects_from_right_join_table)

        # return the result of the hash join (only the objects)
        return result
    
    def hash_function(self, integer):
        """
        Hash function for the hash join (taken from: https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key)

        Parameters
        ----------
        integer : int
            Integer that should be hashed

        Returns
        -------
        integer : int
            Hash of the integer

        """
        integer = ((integer >> 16) ^ integer) * 0x45d9f3b
        integer = ((integer >> 16) ^ integer) * 0x45d9f3b
        integer = (integer >> 16) ^ integer
        return integer


    def sort_merge_join(self):
        """
        Sort merge join for the query of the assignment
        First sort the property tables of the join and then merge them
        """
        print("Start running sort merge join")
        start_time = time.time()

        # join follows.object = friendOf.subject
        objects_of_friendsOf = self.merge(sorted(self.objects_of_follows), 
                                            sorted(self.subjects_of_friendOf.items(), key=lambda item: item[0]))

        # join previous join result object = likes.subject
        objects_of_likes = self.merge(sorted(objects_of_friendsOf), 
                                            sorted(self.subjects_of_likes.items(), key=lambda item: item[0]))

        # join previous join result object = hasReview.subject
        objects_of_hasReview = self.merge(sorted(objects_of_likes), 
                                                sorted(self.subjects_of_hasReview.items(), key=lambda item: item[0]))
        end_time = time.time()
        print("Finish running")
        print("Time: ", end_time - start_time)
        print("Size in GB: ", sys.getsizeof(objects_of_hasReview) / 1000 / 1000 / 1000)

        # TODO: UNCOMMENT THIS LINE TO WRITE THE RESULTS TO A FILE
        # self.collect_results(objects_of_hasReview)

    def merge(self, objects, subjects_objects):
        """
        Merge two sorted lists

        Parameters
        ----------
        objects : list
            Sorted list of objects
        subjects_objects : list
            Sorted list of tuples of subjects and objects

        Returns
        -------
        result : set
            Result of the merge
        """
        result = set()
        index_left_table, index_right_table = 0, 0

        # iterate over the sorted lists and compare the subject and object integer encodings
        while index_left_table < len(objects) and index_right_table < len(subjects_objects):
            # get the subject and object integer encoding of the left and right table given the table indices
            subj, obj = objects[index_left_table], subjects_objects[index_right_table][0]

            # check if the subject and object integer encodings are equal
            if subj == obj:
                # if yes, add the objects of the right table to the result
                result.update(subjects_objects[index_right_table][1])
                # increase both table indices
                index_left_table += 1
                index_right_table += 1

            # check if subject integer encoding is larger than object integer encoding --> increase right table index
            elif subj > obj:
                index_right_table += 1

            # increase left table index
            else:
                index_left_table += 1

        return result
    
    def collect_results(self, objects_of_hasReview):
        """
        Find the corresponding subjects of the given objects that are results of the join(s)
        Write the results to a file as requested by the query in the assignment

        Parameters
        ----------
        objects_of_hasReview : set
            Resulting objects of the join(s)
        """

        # traverse the join results backwards to find the corresponding subjects
        # this saves a lot of memory because the join results are much smaller than the property tables
        generator = (
            (follows_subject, follows_object, friendOf_object, likes_object, hasReview_object)
            for hasReview_object in objects_of_hasReview 
            for likes_object in self.property_tables_int['hasReview'][hasReview_object] 
            for friendOf_object in self.property_tables_int['likes'][likes_object] 
            for follows_object in self.property_tables_int['friendOf'][friendOf_object] 
            for follows_subject in self.property_tables_int['follows'][follows_object] 
        )

        # write the results to a file
        with open(self.output_path, 'w') as self.output:
            for element in generator:
                result = " ".join([self.rdf_dict[singlet] for singlet in element])
                self.output.write(result + '\n')