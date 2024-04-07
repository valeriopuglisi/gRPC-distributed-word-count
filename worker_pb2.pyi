from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkerStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    IDLE: _ClassVar[WorkerStatus]
    IN_PROGRESS: _ClassVar[WorkerStatus]
    COMPLETED: _ClassVar[WorkerStatus]
    FAILED: _ClassVar[WorkerStatus]
IDLE: WorkerStatus
IN_PROGRESS: WorkerStatus
COMPLETED: WorkerStatus
FAILED: WorkerStatus

class Worker(_message.Message):
    __slots__ = ("id", "host", "port", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    host: str
    port: int
    status: WorkerStatus
    def __init__(self, id: _Optional[str] = ..., host: _Optional[str] = ..., port: _Optional[int] = ..., status: _Optional[_Union[WorkerStatus, str]] = ...) -> None: ...
