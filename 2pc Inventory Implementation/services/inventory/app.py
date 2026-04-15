import logging
import os
from concurrent import futures

import grpc
from shared.gen import inventory_pb2_grpc, inventory_pb2
from shared.gen import twopc_pb2, twopc_pb2_grpc

BOOKSSTOCK: dict[str, int] = {}
PREPARED_TXNS = {}
NODE_ID = "4"
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


class InventoryService(inventory_pb2_grpc.InventoryServiceServicer):
    def AddCopies(self, request, context):
        if request.book_id in BOOKSSTOCK:
            BOOKSSTOCK[request.book_id] += request.count
        else:
            BOOKSSTOCK[request.book_id] = request.count

        return inventory_pb2.InventoryResponse(
            ok=True,
            message="Book added",
            book_id=request.book_id,
            available=BOOKSSTOCK[request.book_id],
        )

    def GetAvailability(self, request, context):
        if request.book_id in BOOKSSTOCK:
            return inventory_pb2.InventoryResponse(
                ok=True,
                message="Available copies:",
                book_id=request.book_id,
                available=BOOKSSTOCK[request.book_id],
            )
        return inventory_pb2.InventoryResponse(
            ok=True,
            message="Book not in inventory:",
            book_id=request.book_id,
            available=0,
        )

    def DecrementCopy(self, request, context):
        if request.book_id in BOOKSSTOCK:
            if BOOKSSTOCK[request.book_id] > 0:
                BOOKSSTOCK[request.book_id] -= 1
                return inventory_pb2.InventoryResponse(
                    ok=True,
                    message="Book taken",
                    book_id=request.book_id,
                    available=BOOKSSTOCK[request.book_id],
                )
            return inventory_pb2.InventoryResponse(
                ok=True,
                message="Book out of stock",
                book_id=request.book_id,
                available=BOOKSSTOCK[request.book_id],
            )
        return inventory_pb2.InventoryResponse(
            ok=True,
            message="Book not in inventory:",
            book_id=request.book_id,
            available=0,
        )

    def IncrementCopy(self, request, context):
        if request.book_id in BOOKSSTOCK:
            BOOKSSTOCK[request.book_id] += 1
        else:
            BOOKSSTOCK[request.book_id] = 1
        return inventory_pb2.InventoryResponse(
            ok=True,
            message="Book returned",
            book_id=request.book_id,
            available=BOOKSSTOCK[request.book_id],
        )


class InventoryTwoPCService(twopc_pb2_grpc.TwoPCParticipantServiceServicer):
    def PrepareCheckout(self, request, context):
        log_voting_run(NODE_ID, "PrepareCheckout", request.coordinator_node_id or COORDINATOR_NODE_ID)

        stock = BOOKSSTOCK.get(request.book_id)
        if stock is None:
            return twopc_pb2.VoteResponse(
                transaction_id=request.transaction_id,
                node_id=NODE_ID,
                vote=twopc_pb2.VOTE_ABORT,
                reason="Book not found in inventory",
            )
        if stock <= 0:
            return twopc_pb2.VoteResponse(
                transaction_id=request.transaction_id,
                node_id=NODE_ID,
                vote=twopc_pb2.VOTE_ABORT,
                reason="No copies available",
            )

        PREPARED_TXNS[request.transaction_id] = {"book_id": request.book_id}
        return twopc_pb2.VoteResponse(
            transaction_id=request.transaction_id,
            node_id=NODE_ID,
            vote=twopc_pb2.VOTE_COMMIT,
            reason="Inventory available",
        )

    def FinalizeCheckout(self, request, context):
        log_decision_run(NODE_ID, "FinalizeCheckout", request.coordinator_node_id or COORDINATOR_NODE_ID)
        prepared = PREPARED_TXNS.get(request.transaction_id)

        if request.decision == twopc_pb2.GLOBAL_COMMIT:
            if prepared:
                book_id = prepared["book_id"]
                if book_id in BOOKSSTOCK and BOOKSSTOCK[book_id] > 0:
                    BOOKSSTOCK[book_id] -= 1
            PREPARED_TXNS.pop(request.transaction_id, None)
            return twopc_pb2.DecisionResponse(
                transaction_id=request.transaction_id,
                node_id=NODE_ID,
                success=True,
                message="Inventory committed",
            )

        PREPARED_TXNS.pop(request.transaction_id, None)
        return twopc_pb2.DecisionResponse(
            transaction_id=request.transaction_id,
            node_id=NODE_ID,
            success=True,
            message="Inventory aborted",
        )


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryService(), server)
    twopc_pb2_grpc.add_TwoPCParticipantServiceServicer_to_server(InventoryTwoPCService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    print(f"[twopc-node4-inventory] Node 4 participant listening on {port}", flush=True)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
