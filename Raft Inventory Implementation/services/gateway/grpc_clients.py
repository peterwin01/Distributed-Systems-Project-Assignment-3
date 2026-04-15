# =============================================================================
#  gRPC Client Stubs
# =============================================================================
# This file creates gRPC client stubs that the gateway uses to talk to
# each backend microservice.  Each function returns a "stub" -- a Python
# object whose methods correspond to the RPCs defined in the .proto files.
#
# For most services (users, catalog, circulation, audit) there is a single
# container, so we just connect to its hostname.
#
# For INVENTORY, there are 5 Raft nodes.  The gateway tries each node in
# order until one responds.  Whichever node it reaches will either:
#   - Handle the request directly (if it's the Raft leader), OR
#   - Forward it to the current leader (if it's a follower)
# This means the gateway doesn't need to know who the leader is.

import os
import grpc

# Import the generated stub classes (from proto files)
from shared.gen import users_pb2_grpc, catalog_pb2_grpc, inventory_pb2_grpc, circulation_pb2_grpc, audit_pb2_grpc


# --- Users Service Stub ---
# Single container, address from docker-compose environment variable
def users_stub():
    addr = os.getenv("USERS_ADDR", "users:50051")
    channel = grpc.insecure_channel(addr)
    return users_pb2_grpc.UsersServiceStub(channel)


# --- Catalog Service Stub ---
# Single container, address from docker-compose environment variable
def catalog_stub():
    addr = os.getenv("CATALOG_ADDR", "catalog:50051")
    channel = grpc.insecure_channel(addr)
    return catalog_pb2_grpc.CatalogServiceStub(channel)


# --- Inventory Service Stub (Raft Cluster) ---
# The inventory service runs on 5 Raft nodes (inventory-node1 through node5).
# We build a list of all node addresses from environment variables.
# INVENTORY_NODE_COUNT tells us how many nodes there are (default 5).
# INVENTORY_NODE_1, INVENTORY_NODE_2, etc. give us each node's address.
INVENTORY_NODES = [
    os.getenv(f"INVENTORY_NODE_{i}", f"inventory-node{i}:50051")
    for i in range(1, int(os.getenv("INVENTORY_NODE_COUNT", "5")) + 1)
]

def inventory_stub():
    """Return a gRPC stub connected to the first reachable inventory node.

    Tries each of the 5 Raft nodes in order (node1, node2, ..., node5).
    For each node, we do a quick 1-second connectivity check:
      - If the node responds -> return a stub connected to it
      - If the node is down  -> skip it and try the next one

    This ensures that even if node1 is dead, we can still reach
    node2, node3, etc.  Whichever node we reach will forward writes
    to the Raft leader if it's not the leader itself.

    If ALL nodes are down, we fall back to node1 and let the caller
    handle the connection error.
    """
    for addr in INVENTORY_NODES:
        try:
            channel = grpc.insecure_channel(addr)
            # channel_ready_future() checks if the gRPC channel can connect.
            # We give it 1 second -- if the node is dead, this will timeout
            # and we move on to the next node.
            grpc.channel_ready_future(channel).result(timeout=1)
            return inventory_pb2_grpc.InventoryServiceStub(channel)
        except Exception:
            # This node is unreachable, try the next one
            continue

    # Fallback: return a stub to node1 and let the caller see the error
    channel = grpc.insecure_channel(INVENTORY_NODES[0])
    return inventory_pb2_grpc.InventoryServiceStub(channel)


# --- Circulation Service Stub ---
# Single container, address from docker-compose environment variable
def circulation_stub():
    addr = os.getenv("CIRCULATION_ADDR", "circulation:50051")
    channel = grpc.insecure_channel(addr)
    return circulation_pb2_grpc.CirculationServiceStub(channel)


# --- Audit Service Stub ---
# Single container, address from docker-compose environment variable
def audit_stub():
    addr = os.getenv("AUDIT_ADDR", "audit:50051")
    channel = grpc.insecure_channel(addr)
    return audit_pb2_grpc.AuditServiceStub(channel)