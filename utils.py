import os
import re
from typing import List

def split_text_into_words(text: str) -> List[str]:
    """
    Splits a given text into words, removing any non-alphanumeric characters
    and converting to lowercase.
    """
    return re.findall(r'\b\w+\b', text.lower())

def get_bucket_id(word: str, num_buckets: int) -> int:
    """
    Determines the bucket ID for a word based on the first letter of the word
    and the total number of buckets. Uses modulo operation to ensure the bucket
    ID is within the range of available buckets.
    """
    return ord(word[0]) % num_buckets

def write_to_intermediate_file(task_id: int, bucket_id: int, words: List[str]):
    """
    Writes words to an intermediate file named according to the map task ID and
    the bucket ID. Each word is written on a new line.
    """
    filename = os.path.join("./intermediate_files", f"mr-{task_id}-{bucket_id}")
    with open(filename, 'a+') as file:
        for word in words:
            file.write(f"{word}\n")

def read_intermediate_files(task_id: int, num_map_tasks: int) -> List[str]:
    """
    Reads all intermediate files for a given bucket ID, aggregating words from
    each file into a single list.
    """
    words = []
    root_path = "./intermediate_files"
    for file in os.listdir(root_path):
        file_path = os.path.join(root_path,file)
        filename_split = file.split("-")
        file_bucket_id = filename_split[-1]
        if int(task_id) == int(file_bucket_id):
            print(f"Task ID :{task_id} match with file :{file}")
            if os.path.exists(file_path):
                with open(file_path, 'r') as file:
                    words.extend(file.read().splitlines())

    
    # for task_id in range(num_map_tasks):
    #     print(f"--> read_intermediate_files : task_id = {task_id}")
    #     filename = os.path.join("./intermediate_files", f"mr-{task_id}-{bucket_id}")
    #     print(f"--> read_intermediate_files : filename = {filename}")
    #     if os.path.exists(filename):
    #         with open(filename, 'r') as file:
    #             words.extend(file.read().splitlines())
    return words

def count_words(words: List[str]) -> dict:
    """
    Counts the occurrence of each word in a list, returning a dictionary where
    keys are words and values are the counts.
    """
    word_count = {}
    for word in words:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
    return word_count

def write_final_output(bucket_id: int, word_counts: dict):
    """
    Writes the final output file for a reduce task, containing word counts.
    """
    filename = os.path.join("./output_files", f"out-{bucket_id}")
    with open(filename, 'w') as file:
        for word, count in word_counts.items():
            file.write(f"{word} {count}\n")
