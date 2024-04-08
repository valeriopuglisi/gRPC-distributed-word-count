import unittest
from unittest.mock import patch, MagicMock
import driver
import libs.driver_pb2 as driver_pb2
import libs.task_pb2 as task_pb2
import libs.worker_pb2 as worker_pb2
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import config

class TestMapReduceDriverService(unittest.TestCase):


    
    def setUp(self):
        # Initialize the MapReduceDriverService before each test
        self.driver_service = driver.MapReduceDriverService()


    def test___add_task(self):
        # Create an empty DataFrame to simulate initial worker status
        initial_df = pd.DataFrame(columns=[
            "worker_id", 
            "worker_status", 
            "task_type", 
            "task_id", 
            "task_status"])
        # Copy the empty DataFrame to the driver service's workers attribute
        self.driver_service.workers = initial_df.copy()

        # Call the private method __add_task to add a MAP task with task_id 0
        self.driver_service._MapReduceDriverService__add_task(task_pb2.TaskType.MAP, 0,'file1.txt' )
        # Create an expected DataFrame with the added task
        expected_df = initial_df._append({
            "worker_id": None,
            "worker_status": None,
            "task_type": task_pb2.TaskType.MAP, 
            "task_id": 0, 
            "task_status": task_pb2.TaskStatus.UNASSIGNED,
            "filename": 'file1.txt'
        }, ignore_index=True)
        # Assert that the workers DataFrame matches the expected DataFrame
        assert_frame_equal(self.driver_service.workers, expected_df)
    
    @patch('driver.MapReduceDriverService.__add_task')
    def test___calculate_tasks(self, mock_add_task):
        # Set the total number of map and reduce tasks
        self.driver_service.total_map_tasks = 2
        # Call the private method __calculate_tasks
        self.driver_service._MapReduceDriverService__calculate_tasks()
        # Assert that the mock __add_task method was called 4 times (2 map tasks + 2 reduce tasks)
        self.assertEqual(mock_add_task.call_count, 4)

    @patch('driver.pd.DataFrame.to_csv')
    def test___set_task_unassigned(self, mock_to_csv):
        # Set up the workers DataFrame with tasks in different statuses
        self.driver_service.workers = pd.DataFrame({
            "task_type": [task_pb2.TaskType.MAP, task_pb2.TaskType.REDUCE],
            "task_status": [task_pb2.TaskStatus.IN_PROGRESS, task_pb2.TaskStatus.COMPLETED],
            "task_id": [0, 1] # Added missing task_id column to match the expected DataFrame structure
        })
        # Call the private method __set_task_unassigned for task_id 0 and task_type MAP
        self.driver_service._MapReduceDriverService__set_task_unassigned(0, task_pb2.TaskType.MAP)        # Assert that the task status was updated to UNASSIGNED
        self.assertEqual(self.driver_service.workers.loc[0, "task_status"], task_pb2.TaskStatus.UNASSIGNED)
        # Assert that the to_csv method was called once
        mock_to_csv.assert_called_once()
    @patch('driver.pd.DataFrame.to_csv')
    def test__update_worker_status(self, mock_to_csv):
        # Set up the workers DataFrame with a task ready for status update
        self.driver_service.workers = pd.DataFrame({
            "task_id": [0],
            "task_type": [task_pb2.TaskType.MAP],
            "worker_id": [None],
            "task_status": [task_pb2.TaskStatus.UNASSIGNED],
            "worker_status": [None]
        })
        # Call the _update_worker_status method to update the worker status for task_id 0 and task_type MAP
        self.driver_service._update_worker_status(0, task_pb2.TaskType.MAP, 'worker1', task_pb2.TaskStatus.IN_PROGRESS)
        # Assert that the worker_id, task_status, and worker_status fields were updated correctly
        self.assertEqual(self.driver_service.workers.loc[0, "worker_id"], 'worker1')
        self.assertEqual(self.driver_service.workers.loc[0, "task_status"], task_pb2.TaskStatus.IN_PROGRESS)
        self.assertEqual(self.driver_service.workers.loc[0, "worker_status"], worker_pb2.WorkerStatus.IN_PROGRESS)
        mock_to_csv.assert_called_once()

    @patch('driver.MapReduceDriverService._update_worker_status')
    @patch('driver.pd.DataFrame.loc')
    def test_AssignTask(self, mock_df_loc, mock_update_worker_status):
        # Test setup
        self.driver_service.workers = pd.DataFrame({
            "worker_id": [None, None],
            "worker_status": [None, None],
            "task_type": [task_pb2.TaskType.MAP, task_pb2.TaskType.REDUCE],
            "task_id": [0, 1],
            "task_status": [task_pb2.TaskStatus.UNASSIGNED, task_pb2.TaskStatus.UNASSIGNED],
            "filename": ['file1.txt', '']
        })
        request = driver_pb2.AssignTaskRequest(worker=worker_pb2.Worker(id='worker1'))
        context = MagicMock()

        # Execute the AssignTask method
        response = self.driver_service.AssignTask(request, context)

        # Verify that the _update_worker_status method was called with the correct parameters
        mock_update_worker_status.assert_called_once_with(0, task_pb2.TaskType.MAP, 'worker1', task_pb2.TaskStatus.IN_PROGRESS)

        # Verify that the response contains the correctly assigned task
        self.assertEqual(response.task.id, 0)
        self.assertEqual(response.task.type, task_pb2.TaskType.MAP)
        self.assertEqual(response.task.filename, './input_files/pg-sherlock_holmes.txt')
        self.assertEqual(response.task.status, task_pb2.TaskStatus.UNASSIGNED)

if __name__ == '__main__':
    unittest.main()
