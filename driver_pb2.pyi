import task_pb2 as _task_pb2
import worker_pb2 as _worker_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AssignTaskRequest(_message.Message):
    __slots__ = ("worker",)
    WORKER_FIELD_NUMBER: _ClassVar[int]
    worker: _worker_pb2.Worker
    def __init__(self, worker: _Optional[_Union[_worker_pb2.Worker, _Mapping]] = ...) -> None: ...

class AssignTaskResponse(_message.Message):
    __slots__ = ("task", "error")
    TASK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    task: _task_pb2.Task
    error: str
    def __init__(self, task: _Optional[_Union[_task_pb2.Task, _Mapping]] = ..., error: _Optional[str] = ...) -> None: ...

class UpdateTaskStatusRequest(_message.Message):
    __slots__ = ("worker", "task")
    WORKER_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    worker: _worker_pb2.Worker
    task: _task_pb2.Task
    def __init__(self, worker: _Optional[_Union[_worker_pb2.Worker, _Mapping]] = ..., task: _Optional[_Union[_task_pb2.Task, _Mapping]] = ...) -> None: ...

class UpdateTaskStatusResponse(_message.Message):
    __slots__ = ("ack",)
    ACK_FIELD_NUMBER: _ClassVar[int]
    ack: bool
    def __init__(self, ack: bool = ...) -> None: ...

class CompleteTaskRequest(_message.Message):
    __slots__ = ("task", "worker_id")
    TASK_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    task: _task_pb2.Task
    worker_id: str
    def __init__(self, task: _Optional[_Union[_task_pb2.Task, _Mapping]] = ..., worker_id: _Optional[str] = ...) -> None: ...

class CompleteTaskResponse(_message.Message):
    __slots__ = ("ack",)
    ACK_FIELD_NUMBER: _ClassVar[int]
    ack: bool
    def __init__(self, ack: bool = ...) -> None: ...
