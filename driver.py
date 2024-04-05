import grpc
import os
from concurrent import futures
import time
from typing import List

# Assuming gRPC is chosen based on the config snippet provided
import driver_pb2
import driver_pb2_grpc

from config import (
    DRIVER_HOST,
    DRIVER_PORT,
    INPUT_FILES_DIR,
    NUM_MAP_TASKS,
    NUM_REDUCE_TASKS,
    COMMUNICATION_PROTOCOL,
)

class MapReduceDriverServicer(driver_pb2_grpc.MapReduceDriverServicer):
    def __init__(self):
        self.map_tasks_assigned = 0
        self.reduce_tasks_assigned = 0
        self.map_tasks_completed = 0
        self.reduce_tasks_completed = 0
        self.input_files = self._get_input_files()
        self.total_map_tasks = min(NUM_MAP_TASKS, len(self.input_files))
        self.total_reduce_tasks = NUM_REDUCE_TASKS
        print(f"Input files: ")
        for file in self.input_files:
            print(file)
        
    
    def _get_input_files(self) -> List[str]:
        """
        Retrieves the list of input files from the specified directory.
        """
        return [os.path.join(INPUT_FILES_DIR, f) for f in os.listdir(INPUT_FILES_DIR) if os.path.isfile(os.path.join(INPUT_FILES_DIR, f))]

    def AssignTask(self, request, context):
        """
        Assigns a map or reduce task to a worker.
        """
        

        if self.map_tasks_assigned < self.total_map_tasks:
            task = driver_pb2.Task(
                type=driver_pb2.Task.MAP,
                id=self.map_tasks_assigned,
                filename=self.input_files[self.map_tasks_assigned]
            )
            assignResponse = driver_pb2.AssignTaskResponse(task=task)
            self.map_tasks_assigned += 1
            print(f"self.map_tasks_assigned:{self.map_tasks_assigned}")
            print(f"self.total_map_tasks:{self.total_map_tasks}")
            
            return assignResponse
        
        elif self.reduce_tasks_assigned < NUM_REDUCE_TASKS:
            task = driver_pb2.Task(
                type=driver_pb2.Task.REDUCE,
                id=self.reduce_tasks_assigned
            )
            assignResponse = driver_pb2.AssignTaskResponse(task=task)
            self.reduce_tasks_assigned += 1
            print(f"reduce_tasks_assigned:{self.reduce_tasks_assigned}")
            print(f"NUM_REDUCE_TASKS:{NUM_REDUCE_TASKS}")
            return assignResponse
        else:
            return driver_pb2.AssignTaskResponse(task=driver_pb2.Task(type=driver_pb2.Task.NONE))

    def CompleteTask(self, request, context):
        """
        Marks a task as completed.
        """
        if request.task.type == driver_pb2.Task.MAP:
            self.map_tasks_completed += 1
            return driver_pb2.CompleteTaskResponse(ack=True)
        elif request.task.type == driver_pb2.Task.REDUCE:
            self.reduce_tasks_completed += 1
            return driver_pb2.CompleteTaskResponse(ack=True)

        # Check if all tasks are completed
        if self.map_tasks_completed == self.total_map_tasks and self.reduce_tasks_completed == NUM_REDUCE_TASKS:
            print("All tasks completed. Shutting down the driver.")
            # This is a simple way to stop the server, but in a real application, you might want to use a more graceful shutdown mechanism.
            os._exit(0)

        return driver_pb2.Empty()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    driver_pb2_grpc.add_MapReduceDriverServicer_to_server(MapReduceDriverServicer(), server)
    server.add_insecure_port(f'{DRIVER_HOST}:{DRIVER_PORT}')
    server.start()
    print(f"Driver server started on {DRIVER_HOST}:{DRIVER_PORT}. Waiting for workers...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
