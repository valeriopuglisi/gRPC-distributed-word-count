# Distributed Map-Reduce Word Count

This project implements a distributed map-reduce system to solve the word count problem using Python. It utilizes either gRPC or REST API for server-client communication, focusing on processing a set of text files to produce a count of how many times each word occurs across all files.

## Project Structure

- `utils.py`: Contains utility functions for processing text, including splitting text into words, determining bucket IDs, and reading/writing intermediate and final output files.
- `config.py`: Configuration settings for the distributed map-reduce system, including server details, file paths, and task configurations.
- `driver.py`: The driver (server) part of the system, responsible for distributing tasks to workers and aggregating results.
- `worker.py`: The worker (client) part of the system, responsible for executing map and reduce tasks assigned by the driver.
- `README.md`: This file, providing an overview and instructions for the project.

## Configuration

Before running the system, ensure the configuration in `config.py` matches your setup. Important settings include:

- `DRIVER_HOST` and `DRIVER_PORT`: The host and port where the driver will run.
- `INPUT_FILES_DIR`, `INTERMEDIATE_FILES_DIR`, `OUTPUT_FILES_DIR`: Directories for input, intermediate, and output files.
- `NUM_MAP_TASKS` and `NUM_REDUCE_TASKS`: The number of map and reduce tasks to divide the work into.
- `COMMUNICATION_PROTOCOL`: Set to either 'gRPC' or 'REST' based on your preference.

## Running the System

1. **Start the Driver**: Run `driver.py` to start the driver process. It will wait for workers to connect and request tasks.

   ```
   python driver.py
   ```

2. **Start Worker(s)**: In separate terminal windows, start one or more worker processes by running `worker.py`. Workers will connect to the driver, request tasks, and process them.

   ```
   python worker.py
   ```

Workers can be started before or after the driver, but they will only begin processing tasks once the driver is running.

## Design Overview

- The driver divides the total work among map and reduce tasks based on the configuration and available input files.
- Workers request tasks from the driver and perform either map or reduce operations depending on the task type assigned.
- Map tasks involve reading input files, splitting text into words, and distributing words into buckets based on their first letter.
- Reduce tasks aggregate words from the same bucket across all map tasks, counting occurrences, and writing the final output.
- Communication between the driver and workers is handled over the network using the specified protocol (gRPC or REST).

## Testing

The project includes basic tests to verify the functionality of utility functions and the correct operation of map and reduce tasks. Run the tests to ensure the system is working as expected.

## Conclusion

This distributed map-reduce system demonstrates a basic implementation of the word count problem in a distributed environment. It showcases the use of Python for network communication and parallel processing of large datasets.

