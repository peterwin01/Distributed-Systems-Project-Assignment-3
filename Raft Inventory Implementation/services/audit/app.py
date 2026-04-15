# node 6
# This section handles LogEvents, ListEvents

from concurrent import futures
import logging
import os

import grpc
from shared.gen import audit_pb2_grpc, audit_pb2

events = []

class AuditService(audit_pb2_grpc.AuditServiceServicer):
    def LogEvent(self, request, context):
        if (request.event_type == "1"):
            events.append(request.description)
            return audit_pb2.SimpleResponse(ok = True, message = "event logged")
        else:
            ind = int(request.description)
            return audit_pb2.SimpleResponse(ok = True, message = events[ind])
    


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    audit_pb2_grpc.add_AuditServiceServicer_to_server(AuditService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"[audit] listening on {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
