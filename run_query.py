from data_preprocessor import DataPreprocessor
from join_algorithms import JoinAlgorithm
from collections import defaultdict

if __name__ == '__main__':

    # data_path = 'data/test.txt'
    data_path = 'data/100k.txt'
    # data_path = 'data/watdiv.10M.nt'

    # define relevant properties of the data triples
    properties = ['follows', 'friendOf', 'likes', 'hasReview']

    # preprocess the data
    data_preprocessor = DataPreprocessor(data_path, properties)

    # create a dictionary to store the information about the join algorithms (size and runtime)
    join_information = defaultdict()

    # run the join algorithms
    for algorithm_type in ['hash_join', 'sort_merge_join']:
        for use_yannakis in [True, False]:
            output_path = f"output/{algorithm_type}_{'yannakis' if use_yannakis else 'no_yannakis'}.txt"
            print(f"Running {algorithm_type} with Yannakis: {use_yannakis}")
            join_algorithm = JoinAlgorithm(algorithm_type, data_preprocessor, output_path, use_yannakis)
            size_info = join_algorithm.get_info()
            runtime = join_algorithm.run()
            join_information[f"{algorithm_type}_{'yannakis' if use_yannakis else 'no_yannakis'}"] = (size_info, runtime)

    # plot the join information
    for algorithm_type in ['hash_join', 'sort_merge_join']:
        for use_yannakis in [True, False]:
            size_info, runtime = join_information[f"{algorithm_type}_{'yannakis' if use_yannakis else 'no_yannakis'}"]
            print(f"Algorithm: {algorithm_type} with Yannakis: {use_yannakis}")
            print(f"Size info: {size_info}")
            print(f"Runtime: {runtime}")
            print()
