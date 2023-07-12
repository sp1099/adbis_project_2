from data_preprocessor import DataPreprocessor
from join_algorithms import JoinAlgorithm


if __name__ == '__main__':

    data_path = 'data/100k.txt'
    # data_path = 'data/watdiv.10M.nt'
    # data_path = 'data/test.txt'
    properties = ['follows', 'friendOf', 'likes', 'hasReview']
    data_preprocessor = DataPreprocessor(data_path, properties)
    data_preprocessor.partition_data()
    partition_tables = data_preprocessor.partion_tables

    algorithm_type = "hash_join"
    output_path = "output.txt"

    join_algorithm = JoinAlgorithm(algorithm_type, partition_tables, output_path)
    join_algorithm.run()
