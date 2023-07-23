from collections import defaultdict
import time
import sys

from data_structures import HashMap, PropertyTable

class JoinAlgorithm():
    def __init__(self, algorithm_type, preprocessor, output_path, use_yannakis):
        self.algorithm_type = algorithm_type
        self.output_path = output_path
        self.partition_tables_int = preprocessor.partion_tables_int
        self.rdf_dict = preprocessor.rdf_dict_reversed

        self.reverse_index(use_yannakis)

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


    def reverse_index(self, use_yannakis):
        # reverse the index of oject_of_hasReview

        self.subjects_of_hasReview = defaultdict(set)
        for obj, subjects in self.partition_tables_int['hasReview'].items():
            for subj in subjects:
                self.subjects_of_hasReview[subj].add(obj)

        # reverse the index of object_of_likes considering the relation
        # hasReview.subject = likes.object

        self.subjects_of_likes = defaultdict(set)
        for obj, subjects in self.partition_tables_int['likes'].items():
            if use_yannakis:
                if obj in self.subjects_of_hasReview:
                    for subj in subjects:
                        self.subjects_of_likes[subj].add(obj)
            else:
                for subj in subjects:
                    self.subjects_of_likes[subj].add(obj)

        # reverse the index of object_of_friendOf considering the relation
        # likes.subject = friendOf.object

        self.subjects_of_friendOf = defaultdict(set)
        for obj, subjects in self.partition_tables_int['friendOf'].items():
            if use_yannakis:
                if obj in self.subjects_of_likes:
                    for subj in subjects:
                        self.subjects_of_friendOf[subj].add(obj)
            else:
                for subj in subjects:
                    self.subjects_of_friendOf[subj].add(obj)

        # get the surviving objects of follows considering the relation
        # friendOf.subject = follows.object

        self.objects_of_follows = set()
        for obj, subjects in self.partition_tables_int['follows'].items():
            if use_yannakis:
                if obj in self.subjects_of_friendOf:
                    self.objects_of_follows.add(obj)
            else:
                self.objects_of_follows.add(obj)


    def hash_join(self):
        print("Start running hash join")
        start_time = time.time()
        objects_of_friendsOf = self.hash_join_single(self.objects_of_follows, self.subjects_of_friendOf)
        objects_of_likes = self.hash_join_single(objects_of_friendsOf, self.subjects_of_likes)
        objects_of_hasReview = self.hash_join_single(objects_of_likes, self.subjects_of_hasReview)
        end_time = time.time()
        print("Finish running")
        print("Time: ", end_time - start_time)
        print("Size in GB: ", sys.getsizeof(objects_of_hasReview) / 1000 / 1000 / 1000)

        # self.collect_results(objects_of_hasReview)

    def hash_join_single(self, objects, subjects_objects):
        # build hash table
        hash_table = HashMap()
        for object in objects:
            hash = self.hash_function(object)
            hash_table.insert(hash, object)

        # probe hash table
        result = set()
        for subject, objects in subjects_objects.items():
            hash = self.hash_function(subject)
            hashed_objects = hash_table.get(hash)
            if subject in hashed_objects:
                result.update(objects)

        return result
    
    def hash_function(self, integer):
        integer = ((integer >> 16) ^ integer) * 0x45d9f3b;
        integer = ((integer >> 16) ^ integer) * 0x45d9f3b;
        integer = (integer >> 16) ^ integer;
        return integer;


    def sort_merge_join(self):
        print("Start running sort merge join")
        start_time = time.time()
        objects_of_friendsOf = self.merge(sorted(self.objects_of_follows), 
                                            sorted(self.subjects_of_friendOf.items(), key=lambda item: item[0]))

        # objects_of_friendsOf = subject of likes
        objects_of_likes = self.merge(sorted(objects_of_friendsOf), 
                                            sorted(self.subjects_of_likes.items(), key=lambda item: item[0]))

        # objects_of_likes = subjects_of_hasReview
        objects_of_hasReview = self.merge(sorted(objects_of_likes), 
                                                sorted(self.subjects_of_hasReview.items(), key=lambda item: item[0]))
        end_time = time.time()
        print("Finish running")
        print("Time: ", end_time - start_time)
        print("Size in GB: ", sys.getsizeof(objects_of_hasReview) / 1000 / 1000 / 1000)

        # self.collect_results(objects_of_hasReview)

    def merge(self, objects, subjects_objects):
        result = set()
        i, j = 0, 0

        while i < len(objects) and j < len(subjects_objects):

            subj, obj = objects[i], subjects_objects[j][0]
            if subj == obj:
                result.update(subjects_objects[j][1])
                i += 1
                j += 1

            elif subj > obj:
                j += 1

            else:
                i += 1

        return result
    
    def collect_results(self, objects_of_hasReview):
        generator = (
            (followsSubject, followsObject, friendOfObject, likesObject, hasReviewObject)
            for hasReviewObject in objects_of_hasReview 
            for likesObject in self.partition_tables_int['hasReview'][hasReviewObject] 
            for friendOfObject in self.partition_tables_int['likes'][likesObject] 
            for followsObject in self.partition_tables_int['friendOf'][friendOfObject] 
            for followsSubject in self.partition_tables_int['follows'][followsObject] 
        )


        with open(self.output_path, 'w') as self.output:
            for element in generator:
                result = " ".join([self.rdf_dict[singlet] for singlet in element])
                self.output.write(result + '\n')