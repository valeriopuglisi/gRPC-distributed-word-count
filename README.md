# MapReduce Framework Documentation

This documentation provides an overview of the MapReduce framework implemented in `driver.py` and `worker.py` files, detailing the logic, implementation, and methods of both components within a distributed computing environment.

## Overview

The MapReduce framework is designed to process large sets of data by dividing the task into smaller sub-tasks, processing them in parallel across multiple worker nodes, and then aggregating the results. This framework consists of two main components: the Driver and the Worker.


Execution of the MapReduce framework requires initial configuration specified in the `config.py` file. This file contains basic settings for the operation of the driver and workers, including the server host and port, paths to directories for input, intermediate, and output files, and the number of Map and Reduce tasks to execute. 

To run the driver, it is necessary to specify any additional configuration parameters via the command line. The `driver.py` file uses an argument parser to allow the user to override the default settings defined in `config.py`. Accepted arguments include the driver server host and port, paths to directories for input, intermediate, and output files, and the number of Map and Reduce tasks. 
### Execution Example:

To start the driver with custom configurations, use the following command:
python driver.py --host=localhost --port=50051 --input_dir=./input --intermediate_dir=./intermediate --output_dir=./output --map_tasks=2 --reduce_tasks=2


and in another terminal run the worker with the following command:
python worker.py

or simply run the worker with the default configuration written in `config.py`:
python worker.py in one or more terminals and python driver.py in another terminal.



### Driver (`driver.py`)

The Driver is responsible for orchestrating the MapReduce tasks. It performs several key functions:

1. **Initialization**: Sets up the server, logging, and configuration parameters based on the provided arguments or default settings.
2. **Task Distribution**: Divides the input data into smaller chunks, assigns Map tasks to available Worker nodes, and once Map tasks are completed, assigns Reduce tasks.
3. **Task Monitoring**: Keeps track of the status of each task and worker. If a worker does not update its task status within a specified interval, the task is reassigned. In addition, if a worker disconnects or fails to communicate with the Driver, the system will detect this and reassign the task to another available worker to ensure the task's completion.
4. **Result Aggregation**: Once all Reduce tasks are completed, the Driver aggregates the results and concludes the MapReduce job.

The Driver uses gRPC for communication with Worker nodes, allowing for efficient and reliable task assignment and status updates.

#### Key Methods in `driver.py`

- `__init__`: Initializes the Driver service with default configurations and starts logging.
- `__log_config`: Logs the current configuration settings.
- `__get_input_files`: Retrieves the list of input files to be processed.
- `__add_task`: Adds a new task to the task list.
- `__calculate_tasks`: Calculates and assigns tasks based on the input files.
- `AssignTask`: Assigns a task to a worker and updates the task status.
- `UpdateTaskStatus`: Updates the status of a task based on the worker's progress.
- `serve_driver`: Starts the gRPC server and listens for worker connections.

### Worker (`worker.py`)

The Worker nodes are responsible for executing the tasks assigned by the Driver. Each Worker:

1. **Requests Tasks**: Communicates with the Driver to request a new task.
2. **Performs Tasks**: Depending on the task type (Map or Reduce), the Worker processes the assigned data chunk. For Map tasks, it processes the input data and generates intermediate key-value pairs. For Reduce tasks, it aggregates the results based on the intermediate data.
3. **Updates Task Status**: Notifies the Driver of the task's progress and completion. If the Worker fails to update the Driver due to disconnection or any communication failure, the Driver will attempt to reassign the task to ensure its completion.
4. **Handles Task Completion**: Once a task is completed, the Worker requests the next task until there are no more tasks to process.

Workers use utility functions for data processing, such as splitting text into words, counting word occurrences, and writing results to files.

#### Key Methods in `worker.py`

- `__init__`: Initializes the Worker service with default configurations.
- `RequestTask`: Requests a new task from the Driver.
- `PerformTask`: Performs the assigned task, either Map or Reduce, based on the task type.
- `UpdateTaskStatusDriver`: Updates the Driver with the current status of the task.
- `perform_map_task`: Processes the input file for a Map task.
- `perform_reduce_task`: Aggregates the results for a Reduce task.

## Communication

The framework utilizes gRPC for communication between the Driver and Worker nodes. This choice ensures efficient data transfer and task management in a distributed system. The `libs` directory contains the protobuf definitions (`driver_pb2.py`, `worker_pb2.py`, `task_pb2.py`) and the gRPC service stubs and servicers (`driver_pb2_grpc.py`, `worker_pb2_grpc.py`) that facilitate this communication.

## Conclusion

This MapReduce framework efficiently processes large datasets by distributing tasks across multiple Worker nodes, managed by a central Driver. Through the use of gRPC for inter-component communication, the framework ensures reliability and scalability in processing tasks in parallel, making it suitable for a wide range of data processing applications.

