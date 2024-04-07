import grpc
import os
from concurrent import futures
import time
from typing import List
import config
import numpy
import pandas as pd
import argparse
# Assuming gRPC is chosen based on the config snippet provided
import driver_pb2
import driver_pb2_grpc
import worker_pb2
import worker_pb2_grpc
import task_pb2
import task_pb2_grpc
import threading

import warnings

# Disabilita i FutureWarning di pandas
warnings.simplefilter(action='ignore', category=FutureWarning)


from config import (
    DRIVER_HOST,
    DRIVER_PORT,
    INPUT_FILES_DIR,
    NUM_MAP_TASKS,
    NUM_REDUCE_TASKS,
    COMMUNICATION_PROTOCOL,
)

import logging

# Configurazione del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MapReduceDriverService(driver_pb2_grpc.MapReduceDriverServicer):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # in a loop log the configuration variables
        self.__log_config()
        self.map_tasks_assigned = 0
        self.reduce_tasks_assigned = 0
        self.map_tasks_completed = 0
        self.reduce_tasks_completed = 0
        self.input_files = self._get_input_files()
        self.total_map_tasks = min(NUM_MAP_TASKS, len(self.input_files))
        self.total_reduce_tasks = NUM_REDUCE_TASKS

        # create pandas dataframe for workers status
        self.workers = pd.DataFrame(columns=["worker_id", "worker_status", "worker_host", "worker_port","task_type", "task_id", "task_status"])
        
        self._calculate_tasks()
        
    
    def __log_config(self):
        self.logger.info("Driver server started, with config: ")
        self.logger.info(f"DRIVER_HOST: {DRIVER_HOST}")
        self.logger.info(f"DRIVER_PORT: {DRIVER_PORT}")
        self.logger.info(f"INPUT_FILES_DIR: {INPUT_FILES_DIR}")
        self.logger.info(f"NUM_MAP_TASKS: {NUM_MAP_TASKS}")
        self.logger.info(f"NUM_REDUCE_TASKS: {NUM_REDUCE_TASKS}")
        self.logger.info(f"COMMUNICATION_PROTOCOL: {COMMUNICATION_PROTOCOL}")


    def _get_input_files(self) -> List[str]:
        """
        Retrieves the list of input files from the specified directory.
        """
        self.logger.info("Retrieving input files from directory: %s", INPUT_FILES_DIR)
        files = [os.path.join(INPUT_FILES_DIR, f) for f in os.listdir(INPUT_FILES_DIR) if os.path.isfile(os.path.join(INPUT_FILES_DIR, f))]
        self.logger.info("Input files: %s", files)
        return files
    
    def _calculate_tasks(self):
        for i in range(self.total_map_tasks):
            map_task = task_pb2.Task(
                type=task_pb2.TaskType.MAP,
                status = task_pb2.TaskStatus.UNASSIGNED,
                id=i,
                filename=self.input_files[i]
            )
            self.workers = self.workers._append({
                "worker_id": None, 
                "worker_status": None,
                "worker_host": None, 
                "worker_port": None,
                "task_type": map_task.type, 
                "task_id": map_task.id, 
                "task_status": map_task.status, 
                }, 
                ignore_index=True)
        for i in range(self.total_reduce_tasks):
            reduce_task = task_pb2.Task(
                type=task_pb2.TaskType.REDUCE,
                status = task_pb2.TaskStatus.UNASSIGNED,
                id=i
            )
            self.workers = self.workers._append({
                "worker_id": None, 
                "worker_status": None,
                "worker_host": None, 
                "worker_port": None,
                "task_type": reduce_task.type, 
                "task_id": reduce_task.id, 
                "task_status": reduce_task.status, 
                }, 
                ignore_index=True)
        self.workers.to_csv("workers.csv", index=False)

    def AssignTask(self, request, context):
        """
        Assigns a map or reduce task to a worker.
        """
        # get worker_id from request
        worker_id = request.worker.id
        worker_host = request.worker.host
        worker_port = request.worker.port

        self.logger.info(f"Assigning task to worker ID {worker_id} , host {worker_host} , port {worker_port}")
        # check if all map tasks are completed
        unassigned_map_tasks = self.workers[(self.workers["task_type"] == task_pb2.TaskType.MAP) & (self.workers["task_status"] == task_pb2.TaskStatus.UNASSIGNED)]
        unassigned_reduce_tasks = self.workers[(self.workers["task_type"] == task_pb2.TaskType.REDUCE) & (self.workers["task_status"] == task_pb2.TaskStatus.UNASSIGNED)]
        
        if len(unassigned_map_tasks) > 0:
            # get the first map or reduce task that is not completed
            unassigned_map_tasks = unassigned_map_tasks.iloc[0]
            # convert numpy float64 to int
            task_id =  int(unassigned_map_tasks["task_id"].astype(numpy.int64))
            task_type = int(unassigned_map_tasks["task_type"].astype(numpy.int64))

            task = task_pb2.Task(
                type=task_pb2.TaskType.MAP,
                id=task_id,
                filename=self.input_files[task_id]
            )

            # update workers dataframe with relative task using task_id and task_type
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "task_status"] = task_pb2.TaskStatus.IN_PROGRESS
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "worker_id"] = worker_id
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "worker_status"] = "OK"
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "worker_host"] = worker_host
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "worker_port"] = worker_port
            self.workers.to_csv("workers.csv", index=False)
            assignResponse = driver_pb2.AssignTaskResponse(task=task)
            self.logger.info("Assigned MAP task ID %s to worker: %s", task.id, worker_id)
            return assignResponse
        
        elif len(unassigned_reduce_tasks) > 0:
            unassigned_reduce_tasks = unassigned_reduce_tasks.iloc[0]
            # convert numpy float64 to int
            task_id =  int(unassigned_reduce_tasks["task_id"].astype(numpy.int64))
            task_type = int(unassigned_reduce_tasks["task_type"].astype(numpy.int64))
            task = task_pb2.Task(
                type=task_pb2.TaskType.REDUCE,
                id=task_id,
            )
            # update workers dataframe with relative task using task_id and task_type
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "task_status"] = task_pb2.TaskStatus.IN_PROGRESS
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "worker_id"] = worker_id
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "worker_status"] = worker_pb2.WorkerStatus.IN_PROGRESS
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "worker_host"] = worker_host
            self.workers.loc[
                (self.workers["task_id"] == task_id) &
                (self.workers["task_type"] == task_type), 
                "worker_port"] = worker_port
            self.workers.to_csv("workers.csv", index=False)
            assignResponse = driver_pb2.AssignTaskResponse(task=task)
            self.logger.info("Assigned REDUCE task ID %s to worker: %s", task.id, worker_id)
            return assignResponse
        
        elif all(self.workers["task_status"] == task_pb2.TaskStatus.COMPLETED):
            self.logger.info("All tasks completed")
            driver_pb2.AssignTaskResponse(task=task_pb2.Task(type=task_pb2.TaskType.NONE))
            if all(self.workers["worker_status"] == worker_pb2.WorkerStatus.COMPLETED):
                self.logger.info("All Worker closed process ... Exit")
                os._exit(0)
            
        


    def CompleteTask(self, request, context):
        """
        Marks a task as completed.
        """
        if request.task.type == task_pb2.TaskType.MAP:
            self.logger.info("MAP task ID: %s completed ", request.task.id)
            self.map_tasks_completed += 1


        elif request.task.type == task_pb2.TaskType.REDUCE:
            self.logger.info("REDUCE task ID: %s completed ", request.task.id)
            self.reduce_tasks_completed += 1
        
        # search the worker in the workers dataframe
        worker = self.workers[(self.workers["task_id"] == request.task.id) & (self.workers["task_type"] == request.task.type)]
        # update the status of the worker
        self.workers.loc[worker.index, "worker_status"] = worker_pb2.WorkerStatus.IDLE
        self.workers.loc[worker.index, "task_status"] = task_pb2.TaskStatus.COMPLETED
        self.workers.to_csv("workers.csv", index=False)

        return driver_pb2.CompleteTaskResponse(ack=True)

    def UpdateTaskStatus(self, request, context):
        """
        Aggiorna lo stato di un task assegnato a un worker e avvia un timer di 60 secondi.
        Se il worker non aggiorna lo stato entro questo tempo, il task viene settato come UNASSIGNED.
        """

        self.logger.info("UpdateTaskStatus: Aggiornato lo stato del worker %s a %s", request.worker.id, request.worker.status)
        self.workers.loc[self.workers["worker_id"] == request.worker.id, "worker_status"] = request.worker.status

        # Cerca il worker e il task corrispondente nella lista dei workers
        worker = self.workers[(self.workers["worker_id"] == request.worker.id) & 
                              (self.workers["task_id"] == request.task.id) & 
                              (self.workers["task_type"] == request.task.type)]
        if not worker.empty:
            # Aggiorna lo stato del task nel dataframe dei workers
            self.workers.loc[worker.index, "task_status"] = request.task.status
            self.workers.loc[worker.index, "worker_status"] = request.worker.status
            self.workers.to_csv("workers.csv", index=False)
            self.logger.info("Aggiornato lo stato del task ID %s a %s", request.task.id, request.task.status)
            
            # Avvia il timer
            timer = threading.Timer(10.0, self.set_task_unassigned, [request.task.id])
            timer.start()
            
            return driver_pb2.UpdateTaskStatusResponse(ack=True)
        
    
    def set_task_unassigned(self, task_id):
        """
        Imposta lo stato di un task come UNASSIGNED se il worker non aggiorna lo stato entro 60 secondi.
        """
        # Cerca il task corrispondente nella lista dei workers
        task = self.workers[self.workers["task_id"] == task_id]
        if not task.empty and task["task_status"].iloc[0] != task_pb2.TaskStatus.COMPLETED:
            # Imposta lo stato del task come UNASSIGNED
            self.workers.loc[task.index, "task_status"] = task_pb2.TaskStatus.UNASSIGNED
            self.workers.to_csv("workers.csv", index=False)
            self.logger.info("Impostato lo stato del task ID %s come UNASSIGNED dopo 60 secondi senza aggiornamenti", task_id)
  
def serve_driver(driver):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    driver_pb2_grpc.add_MapReduceDriverServicer_to_server(driver, server)
    server.add_insecure_port(f'{DRIVER_HOST}:{DRIVER_PORT}')
    server.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    

    # Parsing input arguments
    parser = argparse.ArgumentParser(description='MapReduce Driver Configuration')
    parser.add_argument('--driver_host', type=str, default=config.DRIVER_HOST, help='Host for the driver server')
    parser.add_argument('--driver_port', type=int, default=config.DRIVER_PORT, help='Port for the driver server')
    parser.add_argument('--input_files_dir', type=str, default=config.INPUT_FILES_DIR, help='Directory containing input text files')
    parser.add_argument('--intermediate_files_dir', type=str, default=config.INTERMEDIATE_FILES_DIR, help='Directory for storing intermediate files')
    parser.add_argument('--output_files_dir', type=str, default=config.OUTPUT_FILES_DIR, help='Directory for storing output files')
    parser.add_argument('--num_map_tasks', type=int, default=config.NUM_MAP_TASKS, help='Number of map tasks')
    parser.add_argument('--num_reduce_tasks', type=int, default=config.NUM_REDUCE_TASKS, help='Number of reduce tasks')
    parser.add_argument('--communication_protocol', type=str, default=config.COMMUNICATION_PROTOCOL, help='Communication protocol (gRPC or REST)')

    args = parser.parse_args()

    # Prima di aggiornare le variabili in config.py, controlliamo quali parametri sono stati effettivamente passati dall'utente
    if args.driver_host is not None:
        config.DRIVER_HOST = args.driver_host
    if args.driver_port is not None:
        config.DRIVER_PORT = args.driver_port
    if args.input_files_dir is not None:
        config.INPUT_FILES_DIR = args.input_files_dir
    if args.intermediate_files_dir is not None:
        config.INTERMEDIATE_FILES_DIR = args.intermediate_files_dir
    if args.output_files_dir is not None:
        config.OUTPUT_FILES_DIR = args.output_files_dir
    if args.num_map_tasks is not None:
        config.NUM_MAP_TASKS = args.num_map_tasks
    if args.num_reduce_tasks is not None:
        config.NUM_REDUCE_TASKS = args.num_reduce_tasks
    if args.communication_protocol is not None:
        config.COMMUNICATION_PROTOCOL = args.communication_protocol
    # log the new configuration


    driver = MapReduceDriverService()
    serve_driver(driver=driver)