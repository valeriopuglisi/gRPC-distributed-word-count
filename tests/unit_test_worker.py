import unittest
from unittest.mock import patch, MagicMock
import worker
import worker_pb2
import driver_pb2
import task_pb2
import os

class TestWorkerService(unittest.TestCase):

    def setUp(self):
        self.worker_service = worker.WorkerService()

    @patch('worker.grpc.insecure_channel')
    @patch('worker.driver_pb2_grpc.MapReduceDriverStub')
    def test_RequestTask(self, mock_stub, mock_channel):
        mock_channel.return_value = MagicMock()
        mock_stub.return_value = MagicMock()
        mock_stub.return_value.AssignTask.return_value = driver_pb2.AssignTaskResponse(
            task=task_pb2.Task(
                id=1,
                type=task_pb2.TaskType.MAP,
                status=task_pb2.TaskStatus.UNASSIGNED,
                filename="input_file.txt"
            )
        )
        self.worker_service.RequestTask()
        self.assertEqual(self.worker_service.task.id, 1)
        self.assertEqual(self.worker_service.task.type, task_pb2.TaskType.MAP)
        self.assertEqual(self.worker_service.task.status, task_pb2.TaskStatus.UNASSIGNED)
        self.assertEqual(self.worker_service.task.filename, "input_file.txt")
        self.assertEqual(self.worker_service.worker.status, worker_pb2.WorkerStatus.IN_PROGRESS)

    @patch('worker.grpc.insecure_channel')
    @patch('worker.driver_pb2_grpc.MapReduceDriverStub')
    def test_UpdateTaskStatusDriver(self, mock_stub, mock_channel):
        mock_channel.return_value = MagicMock()
        mock_stub.return_value = MagicMock()
        mock_stub.return_value.UpdateTaskStatus.return_value = driver_pb2.UpdateTaskStatusResponse(ack=True)
        self.worker_service.task.id = 1
        self.worker_service.task.type = task_pb2.TaskType.MAP
        self.worker_service.task.status = task_pb2.TaskStatus.COMPLETED
        self.worker_service.worker.status = worker_pb2.WorkerStatus.COMPLETED
        self.worker_service.UpdateTaskStatusDriver()
        mock_stub.return_value.UpdateTaskStatus.assert_called_once()
        self.assertTrue(mock_stub.return_value.UpdateTaskStatus.called)

    @patch('worker.os.path.exists')
    @patch('worker.open', new_callable=unittest.mock.mock_open, read_data='word1 word2 word3')
    @patch('worker.write_to_intermediate_file')
    def test_perform_map_task(self, mock_write_to_intermediate_file, mock_open, mock_path_exists):
        mock_path_exists.return_value = True
        self.worker_service.task.id = 1
        self.worker_service.input_file = "input_file.txt"
        self.worker_service.perform_map_task()
        mock_write_to_intermediate_file.assert_called()
        self.assertTrue(mock_write_to_intermediate_file.called)

    @patch('worker.read_intermediate_files', return_value=['word1', 'word2', 'word3'])
    @patch('worker.count_words', return_value={'word1': 1, 'word2': 1, 'word3': 1})
    @patch('worker.write_final_output')
    def test_perform_reduce_task(self, mock_write_final_output, mock_count_words, mock_read_intermediate_files):
        self.worker_service.task.id = 1
        self.worker_service.perform_reduce_task()
        mock_write_final_output.assert_called()
        self.assertTrue(mock_write_final_output.called)

if __name__ == '__main__':
    unittest.main()

