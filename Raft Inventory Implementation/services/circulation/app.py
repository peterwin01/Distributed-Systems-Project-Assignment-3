from concurrent import futures
import logging
from datetime import date, timedelta
import os

import grpc
from shared.gen import circulation_pb2_grpc, circulation_pb2
#import circulation_pb2_grpc, circulation_pb2, inventory_pb2_grpc, inventory_pb2
"""
def getAvailable(arg_id):
    with grpc.insecure_channel("localhost:50052") as channel:
        stub = inventory_pb2_grpc.InventoryServiceStub(channel)
        response = stub.getAvailability(inventory_pb2.Get(book_id=arg_id))
        return response

def getBook(arg_id):
    with grpc.insecure_channel("localhost:50052") as channel:
        stub = inventory_pb2_grpc.InventoryServiceStub(channel)
        response = stub.decrementCopies(inventory_pb2.Get(book_id=arg_id))
        return response.available
    
def putBook(arg_id):
    with grpc.insecure_channel("localhost:50052") as channel:
        stub = inventory_pb2_grpc.InventoryServiceStub(channel)
        response = stub.addCopies(inventory_pb2.Get(book_id=arg_id))
        return response.available
"""
class CirculationService(circulation_pb2_grpc.CirculationServiceServicer):
    def CheckoutBook(self, request, context):
        #resp = getAvailable(request.book_id)
        #if (resp.available > 1):
        #    getBook(request.book_id)
        return circulation_pb2.CheckoutResponse(ok = True, message = "Book Checked Out", due_date= str(date.today() + timedelta(days=5)))
        #else:
            #print(resp.message)
        
    def CheckinBook(self, request, context):
        #putBook(request.book_id)
        return circulation_pb2.SimpleResponse(ok = True, message = "Book Checked In")


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    circulation_pb2_grpc.add_CirculationServiceServicer_to_server(CirculationService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"[circulation] listening on {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
