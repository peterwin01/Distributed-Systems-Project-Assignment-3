import os
import uuid
from concurrent import futures

import grpc

from shared.gen import users_pb2
from shared.gen import users_pb2_grpc
from shared.gen import twopc_pb2, twopc_pb2_grpc

USERS = {}
PREPARED_TXNS = {}
NODE_ID = "2"
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

class UsersService(users_pb2_grpc.UsersServiceServicer):
    def RegisterUser(self, request, context):
        user_id = str(uuid.uuid4())[:8]
        USERS[user_id] = {
            "user_id": user_id,
            "name": request.name,
            "email": request.email,
        }

        return users_pb2.UserResponse(
            ok=True,
            message="User registered",
            user_id=user_id,
            name=request.name,
            email=request.email,
        )

    def GetUser(self, request, context):
        user = USERS.get(request.user_id)
        if not user:
            return users_pb2.UserResponse(ok=False, message="User not found")

        return users_pb2.UserResponse(
            ok=True,
            message="OK",
            user_id=user["user_id"],
            name=user["name"],
            email=user["email"],
        )


class UsersTwoPCService(twopc_pb2_grpc.TwoPCParticipantServiceServicer):
    def PrepareCheckout(self, request, context):
        log_voting_run(NODE_ID, "PrepareCheckout", request.coordinator_node_id or COORDINATOR_NODE_ID)

        user = USERS.get(request.user_id)
        if user is None:
            return twopc_pb2.VoteResponse(
                transaction_id=request.transaction_id,
                node_id=NODE_ID,
                vote=twopc_pb2.VOTE_ABORT,
                reason="User does not exist",
            )

        PREPARED_TXNS[request.transaction_id] = {
            "user_id": request.user_id,
            "book_id": request.book_id,
        }
        return twopc_pb2.VoteResponse(
            transaction_id=request.transaction_id,
            node_id=NODE_ID,
            vote=twopc_pb2.VOTE_COMMIT,
            reason="User validation passed",
        )

    def FinalizeCheckout(self, request, context):
        log_decision_run(NODE_ID, "FinalizeCheckout", request.coordinator_node_id or COORDINATOR_NODE_ID)
        PREPARED_TXNS.pop(request.transaction_id, None)

        if request.decision == twopc_pb2.GLOBAL_COMMIT:
            return twopc_pb2.DecisionResponse(
                transaction_id=request.transaction_id,
                node_id=NODE_ID,
                success=True,
                message="User phase committed",
            )

        return twopc_pb2.DecisionResponse(
            transaction_id=request.transaction_id,
            node_id=NODE_ID,
            success=True,
            message="User phase aborted",
        )


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    users_pb2_grpc.add_UsersServiceServicer_to_server(UsersService(), server)
    twopc_pb2_grpc.add_TwoPCParticipantServiceServicer_to_server(UsersTwoPCService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    print(f"[twopc-node2-users] Node 2 participant listening on {port}", flush=True)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
