# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import worker_pb2 as worker__pb2


class WorkerServiceStub(object):
    """Definizione del servizio per la comunicazione tra worker e driver
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RequestTask = channel.unary_unary(
                '/worker.WorkerService/RequestTask',
                request_serializer=worker__pb2.TaskRequest.SerializeToString,
                response_deserializer=worker__pb2.TaskResponse.FromString,
                )


class WorkerServiceServicer(object):
    """Definizione del servizio per la comunicazione tra worker e driver
    """

    def RequestTask(self, request, context):
        """Metodo per richiedere un task
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_WorkerServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RequestTask': grpc.unary_unary_rpc_method_handler(
                    servicer.RequestTask,
                    request_deserializer=worker__pb2.TaskRequest.FromString,
                    response_serializer=worker__pb2.TaskResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'worker.WorkerService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class WorkerService(object):
    """Definizione del servizio per la comunicazione tra worker e driver
    """

    @staticmethod
    def RequestTask(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/worker.WorkerService/RequestTask',
            worker__pb2.TaskRequest.SerializeToString,
            worker__pb2.TaskResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)