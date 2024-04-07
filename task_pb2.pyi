from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TaskType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NONE: _ClassVar[TaskType]
    MAP: _ClassVar[TaskType]
    REDUCE: _ClassVar[TaskType]

class TaskStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNASSIGNED: _ClassVar[TaskStatus]
    IN_PROGRESS: _ClassVar[TaskStatus]
    COMPLETED: _ClassVar[TaskStatus]
    FAILED: _ClassVar[TaskStatus]
NONE: TaskType
MAP: TaskType
REDUCE: TaskType
UNASSIGNED: TaskStatus
IN_PROGRESS: TaskStatus
COMPLETED: TaskStatus
FAILED: TaskStatus

class Task(_message.Message):
    __slots__ = ("id", "type", "status", "filename")
    ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    id: int
    type: TaskType
    status: TaskStatus
    filename: str
    def __init__(self, id: _Optional[int] = ..., type: _Optional[_Union[TaskType, str]] = ..., status: _Optional[_Union[TaskStatus, str]] = ..., filename: _Optional[str] = ...) -> None: ...
