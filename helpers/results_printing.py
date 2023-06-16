import sys


def direct_stdout_to_file_if_path_specified(print_method):
    def wrapper(*args, **kwargs):
        if kwargs.get("output_file_path"):
            original_stdout = sys.stdout
            with open(kwargs.get("output_file_path"), 'a') as f:
                sys.stdout = f
                print_method(*args, **kwargs)
                sys.stdout = original_stdout
        else:
            print_method(*args, **kwargs)
    return wrapper


@direct_stdout_to_file_if_path_specified
def print_results(results: str, output_file_path=None):
    print(results)