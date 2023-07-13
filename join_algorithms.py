from collections import defaultdict
import time
import sys

from data_structures import HashMap, PropertyTable

class JoinAlgorithm():

    def __init__(self, algorithm_type, preprocessor, output_path):
        self.algorithm_type = algorithm_type
        self.preprocessor = preprocessor
        self.output_path = output_path
        self.partion_tables_string = preprocessor.partion_tables_string
        self.partition_tables_int = preprocessor.partion_tables_int
        self.rdf_dict = preprocessor.rdf_dict_reversed

        self.algorithm = None
        if self.algorithm_type == "hash_join":
            self.algorithm = HashJoin(self.partition_tables, self.output_path)
        elif self.algorithm_type == "sort_merge_join":
            self.algorithm = SortMergeJoin(self.partition_tables_int, self.rdf_dict, self.output_path)

    def run(self):
        self.algorithm.run()


class HashJoin():

    def __init__(self, partition_tables, output_path):
        self.partition_tables = partition_tables
        self.output_path = output_path
        self.join_direction = "forward"

    def run(self):
        # start_time = time.time()
        # output = self.join(self.partition_tables["follows"], "object", self.partition_tables["friendOf"], "subject", out_data_list=[("table1", "subject"), ("table2", "subject"), ("table2", "object")])
        # output = self.join(output, "object", self.partition_tables["likes"], "subject", out_data_list=[("table2", "object")])
        # output = self.join(output, "object", self.partition_tables["hasReview"], "subject", out_data_list=[("table2", "object")])
        # end_time = time.time()
        # print("Time: ", end_time - start_time)
        # print("Size in GB: ", sys.getsizeof(output.table) / 1000 / 1000 / 1000)
        # print(len(output.table))

        # with open("test_forward.txt", 'w') as output_file:
        #     for items in output.table:
        #         output_file.write(",".join(items["data"]) + '\n')


        self.join_direction = "backward"
        start_time = time.time()
        output = self.join(self.partition_tables["likes"], "object", self.partition_tables["hasReview"], "subject", out_data_list=[("table2", "object"), ("table1", "object")])
        output = self.join(self.partition_tables["friendOf"], "object", output, "subject", out_data_list=[("table1", "object")])
        output = self.join(self.partition_tables["follows"], "object", output, "subject", out_data_list=[("table1", "object"), ("table1", "subject")], batch_size=500000)
        end_time = time.time()
        print("Time: ", end_time - start_time)
        print("Size in GB: ", sys.getsizeof(output.table) / 1000 / 1000 / 1000)
        print(len(output.table))

        # with open("test_backwards.txt", 'w') as output_file:
        #     for items in output.table:
        #         output_file.write(",".join(items["data"]) + '\n')

    def join(self, table1, key1, table2, key2, out_data_list, batch_size=None):
        hash_table = self.build(table1, key1)
        print(len(hash_table.hash_table))
        output_data = self.probe(hash_table, key1, table2, key2, out_data_list, batch_size)
        output_data = PropertyTable(output_data)
        return output_data

    def build(self, table, key):
        hash_table = HashMap()
        for items in table.table:
            hash_key = items[key]
            hash = self.hash_function(hash_key)
            hash_table.insert(hash, (items["subject"], items["object"], items["data"]))

        return hash_table
    
    def probe(self, hash_table, key1, table2, key2, out_data_list, batch_size=None):
        output_data = {}
        i = 0

        for items in table2.table:
            i += 1
            sys.stdout.write('\r')
            sys.stdout.write(f"{i}/{len(table2.table)}")
            sys.stdout.flush()
            hash_key = items[key2]
            hash = self.hash_function(hash_key)
            for subject1, object1, data1 in hash_table.get(hash):
                left_compare = subject1 if key1 == "subject" else object1
                if left_compare == items[key2]:
                    out_data = []
                    for output in out_data_list:
                        if output[0] == "table1":
                            if output[1] == "subject":
                                out_data.append(subject1)
                            else:
                                out_data.append(object1)
                        else:
                            out_data.append(items[output[1]])

                    if output_data.get(subject1) is not None:
                        if self.join_direction == "forward":
                            output_data[subject1].append((items["object"], data1 + out_data))
                        else:
                            output_data[subject1].append((items["object"], items["data"] + out_data))
                    else:
                        if self.join_direction == "forward":
                            output_data[subject1] = [(items["object"], data1 + out_data)]
                        else:
                            output_data[subject1] = [(items["object"], items["data"] + out_data)]
                    
                    if batch_size is not None and (i % batch_size == 0 or i == len(table2.table)):
                        with open("test_on_fly_write.txt", 'a+') as output_file:
                            for values in output_data.values():
                                for value in values:
                                    output_file.write(",".join(value[1]) + '\n')
                        output_data = {}

        return output_data
    
    def hash_function(self, string):
        hash_value = 0
        p, m = 31, 10**9 + 7
        length = len(string)
        p_pow = 1
        for i in range(length):
            hash_value = (hash_value + (1 + ord(string[i]) - ord('a')) * p_pow) % m
            p_pow = (p_pow * p) % m
        return hash_value




class SortMergeJoin():

    def __init__(self, partition_tables_int, rdf_dict, output_path):
        self.partition_tables_int = partition_tables_int
        self.rdf_dict = rdf_dict
        self.output_path = output_path
        self.reverse_index()

    def reverse_index(self):
        # reverse the index of oject_of_hasReview

        self.subjects_of_hasReview = defaultdict(set)
        for obj, subjects in self.partition_tables_int['hasReview'].items():
            for subj in subjects:
                self.subjects_of_hasReview[subj].add(obj)

        # reverse the index of object_of_likes considering the relation
        # hasReview.subject = likes.object

        self.subjects_of_likes = defaultdict(set)
        for obj, subjects in self.partition_tables_int['likes'].items():
            for subj in subjects:
                self.subjects_of_likes[subj].add(obj)

        # reverse the index of object_of_friendOf considering the relation
        # likes.subject = friendOf.object

        self.subjects_of_friendOf = defaultdict(set)
        for obj, subjects in self.partition_tables_int['friendOf'].items():
            for subj in subjects:
                self.subjects_of_friendOf[subj].add(obj)

        # get the surviving objects of follows considering the relation
        # friendOf.subject = follows.object

        self.objects_of_follows = set()
        for obj, subjects in self.partition_tables_int['follows'].items():
            self.objects_of_follows.add(obj)


    def run(self):
        print("Start running")
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
        print(len(objects_of_hasReview))

        self.collect_results(objects_of_hasReview)


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

