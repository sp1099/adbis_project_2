from collections import defaultdict
import time
import sys

from data_structures import HashMap, PropertyTable

class JoinAlgorithm():

    def __init__(self, algorithm_type, partition_tables, output_path):
        self.algorithm_type = algorithm_type
        self.partition_tables = partition_tables
        self.output_path = output_path

        self.algorithm = None
        if self.algorithm_type == "hash_join":
            self.algorithm = HashJoin(self.partition_tables, self.output_path)
        elif self.algorithm_type == "sort_merge_join":
            self.algorithm = SortMergeJoin(self.partition_tables, self.output_path)

    def run(self):
        self.algorithm.run()


class HashJoin():

    def __init__(self, partition_tables, output_path):
        self.partition_tables = partition_tables
        self.output_path = output_path
        self.join_direction = "forward"

    def run(self):
        # # implemet hash join
        # start_time = time.time()
        # # build phase
        # # tables follows.object = friendOf.subject
        # hash_table_follows = defaultdict(list)
        # for subject, objects in self.partition_tables["follows"].items():
        #     for object in objects:
        #         object_hash = self.hash_function(object)
        #         hash_table_follows[object_hash].append((subject, object))
        
        # # tables friendOf.object = likes.subject
        # hash_table_friendOf = defaultdict(list)
        # for subject, objects in self.partition_tables["friendOf"].items():
        #     for object in objects:
        #         object_hash = self.hash_function(object)
        #         hash_table_friendOf[object_hash].append((subject, object))

        # # tables likes.object = hasReview.subject
        # hash_table_likes = defaultdict(list)
        # for subject, objects in self.partition_tables["likes"].items():
        #     for object in objects:
        #         object_hash = self.hash_function(object)
        #         hash_table_likes[object_hash].append((subject, object))

        # print(len(hash_table_follows))
        # print(len(hash_table_friendOf))
        # print(len(hash_table_likes))


        # # probe phase
        # # test likes.object = hasReview.subject
        # likes_output_data = []
        # for subject, objects in self.partition_tables["hasReview"].items():
        #     subject_hash = self.hash_function(subject)
        #     for tuple in hash_table_likes[subject_hash]:
        #         if tuple[1] == subject:
        #             for object in objects:
        #                 likes_output_data.append((tuple[0], tuple[1], object))
        # likes_output_data = set(likes_output_data)
        # print(len(likes_output_data))
        # # test friendOf.object = likes.subject
        # friendOf_output_data = []
        # for i, (likes_subject, likes_object, hasReview_object) in enumerate(likes_output_data):
        #     sys.stdout.write('\r')
        #     sys.stdout.write(f"{i}/{len(likes_output_data)}")
        #     sys.stdout.flush()
        #     subject_hash = self.hash_function(likes_subject)
        #     for tuple in hash_table_friendOf[subject_hash]:
        #         if tuple[1] == likes_subject:
        #             friendOf_output_data.append((tuple[0], tuple[1], likes_object, hasReview_object))
        # friendOf_output_data = set(friendOf_output_data)
        # print(len(friendOf_output_data))
        # # test follows.object = friendOf.subject
        # follows_output_data = []
        # for i, (friendOf_subject, freindOf_object, likes_object, hasReview_object) in enumerate(friendOf_output_data):
        #     sys.stdout.write('\r')
        #     sys.stdout.write(f"{i}/{len(friendOf_output_data)}")
        #     sys.stdout.flush()
        #     subject_hash = self.hash_function(friendOf_subject)
        #     for tuple in hash_table_follows[subject_hash]:
        #         if tuple[1] == friendOf_subject:
        #             follows_output_data.append((tuple[0], tuple[1], freindOf_object, likes_object, hasReview_object))
        # follows_output_data = set(follows_output_data)
        # print(len(follows_output_data))
        # end_time = time.time()
        # print("Time: ", end_time - start_time)
        # print("Size in GB: ", sys.getsizeof(follows_output_data) / 1000 / 1000 / 1000)
        # # write output
        # with open(self.output_path, 'w') as output_file:
        #     for tuple in follows_output_data:
        #         output_file.write(",".join(tuple) + '\n')

        start_time = time.time()
        output = self.join(self.partition_tables["follows"], "object", self.partition_tables["friendOf"], "subject", out_data_list=[("table1", "subject"), ("table2", "subject"), ("table2", "object")])
        output = self.join(output, "object", self.partition_tables["likes"], "subject", out_data_list=[("table2", "object")])
        output = self.join(output, "object", self.partition_tables["hasReview"], "subject", out_data_list=[("table2", "object")])
        end_time = time.time()
        print("Time: ", end_time - start_time)
        print("Size in GB: ", sys.getsizeof(output.table) / 1000 / 1000 / 1000)
        print(len(output.table))

        with open("test_forward.txt", 'w') as output_file:
            for items in output.table:
                output_file.write(",".join(items["data"]) + '\n')


        self.join_direction = "backward"
        start_time = time.time()
        output = self.join(self.partition_tables["likes"], "object", self.partition_tables["hasReview"], "subject", out_data_list=[("table2", "object"), ("table1", "object")])
        output = self.join(self.partition_tables["friendOf"], "object", output, "subject", out_data_list=[("table1", "object")])
        output = self.join(self.partition_tables["follows"], "object", output, "subject", out_data_list=[("table1", "object"), ("table1", "subject")])
        end_time = time.time()
        print("Time: ", end_time - start_time)
        print("Size in GB: ", sys.getsizeof(output.table) / 1000 / 1000 / 1000)
        print(len(output.table))

        with open("test_backwards.txt", 'w') as output_file:
            for items in output.table:
                output_file.write(",".join(items["data"]) + '\n')

    def join(self, table1, key1, table2, key2, out_data_list):
        # # build phase
        # hash_table = defaultdict(list)
        # for subject, objects in table1.items():
        #     for object in objects:
        #         if key1 == "subject":
        #             hash = self.hash_function(subject)
        #             hash_table[hash].append((subject, object))
        #         else:
        #             hash = self.hash_function(object)
        #             hash_table[hash].append((subject, object))
        hash_table = self.build(table1, key1)
        print(len(hash_table.hash_table))
        output_data = self.probe(hash_table, key1, table2, key2, out_data_list)
        output_data = PropertyTable(output_data)
        return output_data

    def build(self, table, key):
        hash_table = HashMap()
        for items in table.table:
            hash_key = items[key]
            hash = self.hash_function(hash_key)
            hash_table.insert(hash, (items["subject"], items["object"], items["data"]))

        return hash_table
    
    def probe(self, hash_table, key1, table2, key2, out_data_list):
        output_data = {}

        for items in table2.table:
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

        return output_data

        # # probe phase
        # output_data = []
        # for subject, objects in table2.items():
        #     for object in objects:
        #         if key2 == "subject":
        #             hash = self.hash_function(subject)
        #             for tuple in hash_table[hash]:
        #                 if tuple[1] == subject:
        #                     output_data.append((tuple[0], tuple[1], object))
        #         else:
        #             hash = self.hash_function(object)
        #             for tuple in hash_table[hash]:
        #                 if tuple[1] == object:
        #                     output_data.append((tuple[0], tuple[1], subject))



    def hash_function(self, string):
        hash = 0
        for char in string:
            hash += ord(char)**3
        hash = hash % 1000
        return hash




class SortMergeJoin():

    def __init__(self, partition_tables, output_path):
        self.partition_tables = partition_tables
        self.output_path = output_path

    def run(self):
        pass


