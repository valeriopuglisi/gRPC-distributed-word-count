from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Task(_message.Message):
    __slots__ = ("type", "id", "filename")
    class TaskType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NONE: _ClassVar[Task.TaskType]
        MAP: _ClassVar[Task.TaskType]
        REDUCE: _ClassVar[Task.TaskType]
    NONE: Task.TaskType
    MAP: Task.TaskType
    REDUCE: Task.TaskType
    TYPE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    type: Task.TaskType
    id: int
    filename: str
    def __init__(self, type: _Optional[_Union[Task.TaskType, str]] = ..., id: _Optional[int] = ..., filename: _Optional[str] = ...) -> None: ...

class AssignTaskRequest(_message.Message):
    __slots__ = ("worker_id",)
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    def __init__(self, worker_id: _Optional[str] = ...) -> None: ...

class AssignTaskResponse(_message.Message):
    __slots__ = ("task", "error")
    TASK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    task: Task
    error: str
    def __init__(self, task: _Optional[_Union[Task, _Mapping]] = ..., error: _Optional[str] = ...) -> None: ...

class CompleteTaskRequest(_message.Message):
    __slots__ = ("task", "worker_id")
    TASK_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    task: Task
    worker_id: str
    def __init__(self, task: _Optional[_Union[Task, _Mapping]] = ..., worker_id: _Optional[str] = ...) -> None: ...

class CompleteTaskResponse(_message.Message):
    __slots__ = ("ack",)
    ACK_FIELD_NUMBER: _ClassVar[int]
    ack: bool
    def __init__(self, ack: bool = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
