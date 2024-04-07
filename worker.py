import grpc
import os
import sys
from concurrent import futures
import time
import threading
import task_pb2
import task_pb2_grpc
import worker_pb2
import worker_pb2_grpc
import driver_pb2
import driver_pb2_grpc
from config import DRIVER_HOST, DRIVER_PORT
from utils import split_text_into_words, get_bucket_id, write_to_intermediate_file, read_intermediate_files, count_words, write_final_output
import config

# add logging
import logging 
import socket

import warnings

# Disabilita i FutureWarning di pandas
warnings.simplefilter(action='ignore', category=FutureWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WorkerService():
    def __init__(self, driver_host = config.DRIVER_HOST, driver_port = config.DRIVER_PORT, worker_host=config.DRIVER_HOST, worker_port=None):
        self.task : task_pb2.Task = task_pb2.Task(
            id=0,
            type=task_pb2.TaskType.NONE,
            status=task_pb2.TaskStatus.UNASSIGNED,
            filename=""
        )

        self.worker : worker_pb2.Worker = worker_pb2.Worker(
            id=str(os.getpid()),
            host=worker_host,
            port=worker_port,
            status=worker_pb2.WorkerStatus.IDLE,
        )

        self.driver_host = driver_host
        self.driver_port = driver_port

        
        self.input_file = None
        self.intermediate_file = None
        self.output_file = None
        self.num_buckets = config.NUM_REDUCE_TASKS
       

    def RequestTask(self, worker_host, worker_port):
        # Communicate with the driver to get a task
        logging.info("Requesting Task to Driver ...")
        channel = grpc.insecure_channel(f'{self.driver_host}:{self.driver_port}')
        stub = driver_pb2_grpc.MapReduceDriverStub(channel)
        response = stub.AssignTask(driver_pb2.AssignTaskRequest(worker=self.worker))
        self.task = response.task
        self.input_file = response.task.filename
        self.worker.status = worker_pb2.WorkerStatus.IN_PROGRESS
        logging.info(f"Received task ID: {self.task.id} of TYPE {self.task.type}")

    def RequestCompleteTask(self):
        logging.info(f"CompleteTaskRequest: ID {self.task.id} TYPE: {self.task.type}")
        channel = grpc.insecure_channel(f'{self.driver_host}:{self.driver_port}')
        stub = driver_pb2_grpc.MapReduceDriverStub(channel)
        response = stub.CompleteTask(driver_pb2.CompleteTaskRequest(task=self.task))
        logging.info(f"CompleteTaskResponse: {response}")
        self.worker.status = worker_pb2.WorkerStatus.IDLE

    def PerformTask(self):
        
        logging.info(f"PerformTask : Task ID {self.task.id} TYPE: {self.task.type}")
        if self.task.type == task_pb2.TaskType.MAP:
            self.task.status = task_pb2.TaskStatus.IN_PROGRESS
            self.worker.status = worker_pb2.WorkerStatus.IN_PROGRESS
            self.perform_map_task()
            self.task.status = task_pb2.TaskStatus.COMPLETED
            self.worker.status = worker_pb2.WorkerStatus.IDLE
        
        elif self.task.type == task_pb2.TaskType.REDUCE:
            self.task.status = task_pb2.TaskStatus.IN_PROGRESS
            self.worker.status = worker_pb2.WorkerStatus.IN_PROGRESS
            self.perform_reduce_task()
            self.task.status = task_pb2.TaskStatus.COMPLETED
            self.worker.status = worker_pb2.WorkerStatus.IDLE

        elif self.task.type == task_pb2.TaskType.NONE:
            logging.info("No task available ...Exit")
            self.worker.status = worker_pb2.WorkerStatus.COMPLETED
            self.task.status = task_pb2.TaskStatus.COMPLETED
            self.UpdateTaskStatusDriver()
            sys._exit(0)

    def UpdateTaskStatusDriver(self):
        logging.info(f"UpdateTaskStatus : Updating status for task ID: {self.task.id} TYPE: {self.task.type}")
        channel = grpc.insecure_channel(f'{self.driver_host}:{self.driver_port}')
        stub = driver_pb2_grpc.MapReduceDriverStub(channel)
        try:
            request = driver_pb2.UpdateTaskStatusRequest(worker=self.worker, task=self.task)
            response = stub.UpdateTaskStatus(request)
            if response.ack:
                logging.info("Driver accepted status update.")
            
        except grpc.RpcError as e:
            logging.error(f"Error during status update: {e}")


    def perform_map_task(self):
        logging.info(f"Performing MAP Task ID :{self.task.id} on FILE: {self.input_file}")
        # Assuming the input files are named sequentially as 'input0.txt', 'input1.txt', ..., 'inputN.txt'
        input_file_path = self.input_file
        if not os.path.exists(input_file_path):
            print(f"No input file found for map task {self.task_id}")
            return
        with open(input_file_path, 'r') as file:
            text = file.read()
        
        words = split_text_into_words(text)
        for word in words:
            bucket_id = get_bucket_id(word, self.num_buckets)
            write_to_intermediate_file(self.task.id, bucket_id, [word])   
        self.RequestCompleteTask()
        

    def perform_reduce_task(self):
        print(f"Performing REDUCE TASK ID:{self.task.id}")
        words = read_intermediate_files(self.task.id, config.NUM_MAP_TASKS)
        word_counts = count_words(words)
        write_final_output(self.task.id, word_counts)
        self.RequestCompleteTask()


def get_first_free_port(starting_port=1024, max_port=65535):
    """
    Trova la prima porta libera disponibile sul sistema, partendo da una porta iniziale.
    :param starting_port: Porta da cui iniziare la ricerca.
    :param max_port: Massima porta da considerare nella ricerca.
    :return: La prima porta libera trovata.
    """
    for port in range(starting_port, max_port + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(('localhost', port)) != 0:
                return port
    raise IOError("Nessuna porta libera trovata.")


def start_server(worker):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        worker_pb2_grpc.add_WorkerServiceServicer_to_server(worker, server)
        server.add_insecure_port(f'{worker.worker_host}:{worker.worker_port}')
        server.start()
        logging.info(f"WORKER ID: {worker.worker_id} server started on {worker.worker_host}:{worker.worker_port}. Waiting for DRIVER...")
        while(True):
            time.sleep(1)


def request_and_perform_tasks(worker):
    while True:
        try:
            worker.RequestTask(worker_host=worker.worker.host, worker_port=worker.worker.port)
            if worker.task.type is None:
                time.sleep(1)
            else:
                worker.PerformTask()
                logging.info(f"Completed task TYPE:{worker.task.type}  ID:{worker.task.id} ")
                time.sleep(1)
        except grpc.RpcError as e:
            logging.info(f"Driver not available ... Reconnecting in 3 sec")
            time.sleep(3)
            continue


def update_status_to_driver(worker):
    while True:
        worker.UpdateTaskStatusDriver()
        time.sleep(3)
        if worker.worker.status == worker_pb2.WorkerStatus.COMPLETED:
            logging.info("Worker status: COMPLETED ... Exiting")
            os._exit(0)

if __name__ == '__main__':

    worker_port = get_first_free_port()
    worker = WorkerService(config.DRIVER_HOST, config.DRIVER_PORT, config.DRIVER_HOST, worker_port)
    
    # add information about worker to logger
    logging.info(f"Worker ID: {worker.worker.id} started on {worker.worker.host}:{worker.worker.port}")
    

    thread_tasks = threading.Thread(target=request_and_perform_tasks, args=(worker,))
    thread_status = threading.Thread(target=update_status_to_driver, args=(worker,))
    thread_tasks.start()
    thread_status.start()

    
    # Attendi che entrambi i thread completino prima di procedere
    thread_tasks.join()
    thread_status.join()

    print("Tutti i thread completati")
