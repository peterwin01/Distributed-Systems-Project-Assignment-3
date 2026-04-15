import os
import uuid
from concurrent import futures

import grpc
# gets the generated files from grpc
from shared.gen import catalog_pb2, catalog_pb2_grpc

# In-memory database
BOOKS: dict[str, dict] = {}


class CatalogService(catalog_pb2_grpc.CatalogServiceServicer):
    def PublishBook(self, request, context):
        # makes the unique book id
        book_id = str(uuid.uuid4())[:8]
        BOOKS[book_id] = {"book_id": book_id, "title": request.title, "author": request.author}

        return catalog_pb2.BookResponse(
            ok=True,
            message="Book published",
            book_id=book_id,
            title=request.title,
            author=request.author,
        )

    def GetBook(self, request, context):
        book = BOOKS.get(request.book_id)
        if not book:
            return catalog_pb2.BookResponse(ok=False, message="Book not found")

        return catalog_pb2.BookResponse(
            ok=True,
            message="OK",
            book_id=book["book_id"],
            title=book["title"],
            author=book["author"],
        )


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    catalog_pb2_grpc.add_CatalogServiceServicer_to_server(CatalogService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    print(f"[catalog] listening on {port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
