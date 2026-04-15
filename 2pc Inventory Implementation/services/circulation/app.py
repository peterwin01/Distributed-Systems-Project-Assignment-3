import logging
import os
from concurrent import futures
from datetime import date, timedelta

import grpc
from shared.gen import circulation_pb2_grpc, circulation_pb2
from shared.gen import twopc_pb2, twopc_pb2_grpc

CHECKOUTS = []
PREPARED_TXNS = {}
NODE_ID = "5"
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

class CirculationService(circulation_pb2_grpc.CirculationServiceServicer):
    def CheckoutBook(self, request, context):
        due_date = str(date.today() + timedelta(days=5))
        CHECKOUTS.append({
            "user_id": request.user_id,
            "book_id": request.book_id,
            "due_date": due_date,
        })
        return circulation_pb2.CheckoutResponse(
            ok=True,
            message="Book Checked Out",
            due_date=due_date,
        )

    def CheckinBook(self, request, context):
        removed = False
        for idx, record in enumerate(CHECKOUTS):
            if record["user_id"] == request.user_id and record["book_id"] == request.book_id:
                CHECKOUTS.pop(idx)
                removed = True
                break
        return circulation_pb2.SimpleResponse(
            ok=True,
            message="Book Checked In" if removed else "No active checkout found",
        )


class CirculationTwoPCService(twopc_pb2_grpc.TwoPCParticipantServiceServicer):
    def PrepareCheckout(self, request, context):
        log_voting_run(NODE_ID, "PrepareCheckout", request.coordinator_node_id or COORDINATOR_NODE_ID)

        for record in CHECKOUTS:
            if record["user_id"] == request.user_id and record["book_id"] == request.book_id:
                return twopc_pb2.VoteResponse(
                    transaction_id=request.transaction_id,
                    node_id=NODE_ID,
                    vote=twopc_pb2.VOTE_ABORT,
                    reason="Duplicate checkout not allowed",
                )

        PREPARED_TXNS[request.transaction_id] = {
            "user_id": request.user_id,
            "book_id": request.book_id,
        }
        return twopc_pb2.VoteResponse(
            transaction_id=request.transaction_id,
            node_id=NODE_ID,
            vote=twopc_pb2.VOTE_COMMIT,
            reason="Circulation prepared",
        )

    def FinalizeCheckout(self, request, context):
        log_decision_run(NODE_ID, "FinalizeCheckout", request.coordinator_node_id or COORDINATOR_NODE_ID)
        prepared = PREPARED_TXNS.get(request.transaction_id)

        if request.decision == twopc_pb2.GLOBAL_COMMIT:
            if prepared:
                due_date = str(date.today() + timedelta(days=5))
                CHECKOUTS.append({
                    "user_id": prepared["user_id"],
                    "book_id": prepared["book_id"],
                    "due_date": due_date,
                })
            PREPARED_TXNS.pop(request.transaction_id, None)
            return twopc_pb2.DecisionResponse(
                transaction_id=request.transaction_id,
                node_id=NODE_ID,
                success=True,
                message="Circulation committed",
            )

        PREPARED_TXNS.pop(request.transaction_id, None)
        return twopc_pb2.DecisionResponse(
            transaction_id=request.transaction_id,
            node_id=NODE_ID,
            success=True,
            message="Circulation aborted",
        )


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    circulation_pb2_grpc.add_CirculationServiceServicer_to_server(CirculationService(), server)
    twopc_pb2_grpc.add_TwoPCParticipantServiceServicer_to_server(CirculationTwoPCService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"[twopc-node5-circulation] Node 5 participant listening on {port}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
