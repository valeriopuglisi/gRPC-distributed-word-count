import unittest
from unittest.mock import patch, MagicMock
import driver
from driver import MapReduceDriverServicer
import driver_pb2

class TestMapReduceDriverServicer(unittest.TestCase):
    def setUp(self):
        self.servicer = MapReduceDriverServicer()

    @patch('os.listdir')
    @patch('os.path.isfile')
    @patch('os.path.join')
    def test_get_input_files(self, mock_join, mock_isfile, mock_listdir):
        mock_listdir.return_value = ['file1.txt', 'file2.txt']
        mock_isfile.return_value = True
        mock_join.side_effect = lambda x, y: f"{x}/{y}"

        expected_files = ['input_dir/file1.txt', 'input_dir/file2.txt']
        driver.INPUT_FILES_DIR = 'input_dir'
        files = self.servicer._get_input_files()

        self.assertEqual(files, expected_files)

    @patch.object(MapReduceDriverServicer, '_get_input_files')
    def test_assign_task_map(self, mock_get_input_files):
        mock_get_input_files.return_value = ['file1.txt', 'file2.txt']
        self.servicer.total_map_tasks = 2

        request = MagicMock(worker_id="worker1")
        context = MagicMock()

        response = self.servicer.AssignTask(request, context)

        self.assertEqual(response.task.type, driver_pb2.Task.MAP)
        self.assertEqual(response.task.filename, 'file1.txt')
        self.assertEqual(self.servicer.map_tasks_assigned, 1)

    @patch.object(MapReduceDriverServicer, '_get_input_files')
    def test_assign_task_reduce(self, mock_get_input_files):
        mock_get_input_files.return_value = ['file1.txt', 'file2.txt']
        self.servicer.total_map_tasks = 2
        self.servicer.map_tasks_assigned = 2
        self.servicer.total_reduce_tasks = 1

        request = MagicMock(worker_id="worker1")
        context = MagicMock()

        response = self.servicer.AssignTask(request, context)

        self.assertEqual(response.task.type, driver_pb2.Task.REDUCE)
        self.assertEqual(self.servicer.reduce_tasks_assigned, 1)

    def test_complete_task_map(self):
        self.servicer.total_map_tasks = 1
        self.servicer.map_tasks_assigned = 1

        request = MagicMock(task=driver_pb2.Task(type=driver_pb2.Task.MAP, id=0))
        context = MagicMock()

        response = self.servicer.CompleteTask(request, context)

        self.assertTrue(response.ack)
        self.assertEqual(self.servicer.map_tasks_completed, 1)

    def test_complete_task_reduce(self):
        self.servicer.total_reduce_tasks = 1
        self.servicer.reduce_tasks_assigned = 1

        request = MagicMock(task=driver_pb2.Task(type=driver_pb2.Task.REDUCE, id=0))
        context = MagicMock()

        response = self.servicer.CompleteTask(request, context)

        self.assertTrue(response.ack)
        self.assertEqual(self.servicer.reduce_tasks_completed, 1)

if __name__ == '__main__':
    unittest.main()
