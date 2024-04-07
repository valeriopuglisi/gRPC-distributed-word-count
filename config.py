# config.py

# Configuration for the distributed map-reduce word count program

# Server (Driver) Configuration
DRIVER_HOST = 'localhost'
DRIVER_PORT = 50051  # Default port for gRPC. Change if using REST or another gRPC service.

# Path to the directory containing input text files
INPUT_FILES_DIR = './input_files'

# Path to the directory for storing intermediate files generated by map tasks
INTERMEDIATE_FILES_DIR = './intermediate_files'

# Path to the directory for storing final output files generated by reduce tasks
OUTPUT_FILES_DIR = './output_files'

# Number of map tasks to divide the input files into
# This can be adjusted based on the number of input files or desired level of parallelism
NUM_MAP_TASKS = 1

# Number of reduce tasks to aggregate the intermediate results
# This should be less than or equal to the number of unique first letters in words across all input files
NUM_REDUCE_TASKS = 1  # Assuming English alphabet

# Communication Protocol
# This can be either 'gRPC' or 'REST'. Change according to your preference and implementation.
COMMUNICATION_PROTOCOL = 'gRPC'

# Additional configuration parameters can be added here as needed.
# For example, you might want to add authentication for the server-client communication,
# or specify timeouts for network operations.

