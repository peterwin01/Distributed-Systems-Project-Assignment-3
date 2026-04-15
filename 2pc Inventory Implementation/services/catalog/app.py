import os
import uuid
from concurrent import futures

import grpc
from shared.gen import catalog_pb2, catalog_pb2_grpc
from shared.gen import twopc_pb2, twopc_pb2_grpc

BOOKS: dict[str, dict] = {}
PREPARED_TXNS = {}
NODE_ID = "3"
COORDINATOR_NODE_ID = "1"


def log_voting_send(node_from: str, rpc_name: str, node_to: str):
    print(
        f"Phase Voting of Node {node_from} sends RPC {rpc_name} to Phase Voting of Node {node_to}",
        flush=True,
    )


def log_voting_run(node_self: str, rpc_name: str, caller_node: str):
    print(
        f"Phase Voting of Node {node_self} runs RPC {rpc_name} called by Phase Voting of Node {caller_node}",
        flush=True,
    )


def log_decision_send(node_from: str, rpc_name: str, node_to: str):
    print(
        f"Phase Decision of Node {node_from} sends RPC {rpc_name} to Phase Decision of Node {node_to}",
        flush=True,
    )


def log_decision_run(node_self: str, rpc_name: str, caller_node: str):
    print(
        f"Phase Decision of Node {node_self} runs RPC {rpc_name} called by Phase Decision of Node {caller_node}",
        flush=True,
    )

class CatalogService(catalog_pb2_grpc.CatalogServiceServicer):
    def PublishBook(self, request, context):
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


class CatalogTwoPCService(twopc_pb2_grpc.TwoPCParticipantServiceServicer):
    def PrepareCheckout(self, request, context):
        log_voting_run(NODE_ID, "PrepareCheckout", request.coordinator_node_id or COORDINATOR_NODE_ID)

        book = BOOKS.get(request.book_id)
        if book is None:
            return twopc_pb2.VoteResponse(
                transaction_id=request.transaction_id,
                node_id=NODE_ID,
                vote=twopc_pb2.VOTE_ABORT,
                reason="Book does not exist",
            )

        PREPARED_TXNS[request.transaction_id] = {
            "user_id": request.user_id,
            "book_id": request.book_id,
        }
        return twopc_pb2.VoteResponse(
            transaction_id=request.transaction_id,
            node_id=NODE_ID,
            vote=twopc_pb2.VOTE_COMMIT,
            reason="Catalog validation passed",
        )

    def FinalizeCheckout(self, request, context):
        log_decision_run(NODE_ID, "FinalizeCheckout", request.coordinator_node_id or COORDINATOR_NODE_ID)
        PREPARED_TXNS.pop(request.transaction_id, None)

        if request.decision == twopc_pb2.GLOBAL_COMMIT:
            return twopc_pb2.DecisionResponse(
                transaction_id=request.transaction_id,
                node_id=NODE_ID,
                success=True,
                message="Catalog phase committed",
            )

        return twopc_pb2.DecisionResponse(
            transaction_id=request.transaction_id,
            node_id=NODE_ID,
            success=True,
            message="Catalog phase aborted",
        )


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    catalog_pb2_grpc.add_CatalogServiceServicer_to_server(CatalogService(), server)
    twopc_pb2_grpc.add_TwoPCParticipantServiceServicer_to_server(CatalogTwoPCService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    print(f"[twopc-node3-catalog] Node 3 participant listening on {port}", flush=True)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
