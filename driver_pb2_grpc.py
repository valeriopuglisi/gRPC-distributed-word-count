# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import driver_pb2 as driver__pb2


class MapReduceDriverStub(object):
    """Servizio fornito dal driver per gestire i task MapReduce
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.AssignTask = channel.unary_unary(
                '/driver.MapReduceDriver/AssignTask',
                request_serializer=driver__pb2.AssignTaskRequest.SerializeToString,
                response_deserializer=driver__pb2.AssignTaskResponse.FromString,
                )
        self.CompleteTask = channel.unary_unary(
                '/driver.MapReduceDriver/CompleteTask',
                request_serializer=driver__pb2.CompleteTaskRequest.SerializeToString,
                response_deserializer=driver__pb2.CompleteTaskResponse.FromString,
                )


class MapReduceDriverServicer(object):
    """Servizio fornito dal driver per gestire i task MapReduce
    """

    def AssignTask(self, request, context):
        """Metodo per assegnare un task a un worker
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CompleteTask(self, request, context):
        """Segnala il completamento di un task da parte di un worker
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_MapReduceDriverServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'AssignTask': grpc.unary_unary_rpc_method_handler(
                    servicer.AssignTask,
                    request_deserializer=driver__pb2.AssignTaskRequest.FromString,
                    response_serializer=driver__pb2.AssignTaskResponse.SerializeToString,
            ),
            'CompleteTask': grpc.unary_unary_rpc_method_handler(
                    servicer.CompleteTask,
                    request_deserializer=driver__pb2.CompleteTaskRequest.FromString,
                    response_serializer=driver__pb2.CompleteTaskResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'driver.MapReduceDriver', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class MapReduceDriver(object):
    """Servizio fornito dal driver per gestire i task MapReduce
    """

    @staticmethod
    def AssignTask(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/driver.MapReduceDriver/AssignTask',
            driver__pb2.AssignTaskRequest.SerializeToString,
            driver__pb2.AssignTaskResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CompleteTask(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/driver.MapReduceDriver/CompleteTask',
            driver__pb2.CompleteTaskRequest.SerializeToString,
            driver__pb2.CompleteTaskResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
