import os
import grpc

from shared.gen import (
    users_pb2_grpc,
    catalog_pb2_grpc,
    inventory_pb2_grpc,
    circulation_pb2_grpc,
    audit_pb2_grpc,
    twopc_pb2_grpc,
)


def users_stub():
    addr = os.getenv("USERS_ADDR", "users:50051")
    channel = grpc.insecure_channel(addr)
    return users_pb2_grpc.UsersServiceStub(channel)


def catalog_stub():
    addr = os.getenv("CATALOG_ADDR", "catalog:50051")
    channel = grpc.insecure_channel(addr)
    return catalog_pb2_grpc.CatalogServiceStub(channel)


def inventory_stub():
    addr = os.getenv("INVENTORY_ADDR", "inventory:50051")
    channel = grpc.insecure_channel(addr)
    return inventory_pb2_grpc.InventoryServiceStub(channel)


def circulation_stub():
    addr = os.getenv("CIRCULATION_ADDR", "circulation:50051")
    channel = grpc.insecure_channel(addr)
    return circulation_pb2_grpc.CirculationServiceStub(channel)


def audit_stub():
    addr = os.getenv("AUDIT_ADDR", "audit:50051")
    channel = grpc.insecure_channel(addr)
    return audit_pb2_grpc.AuditServiceStub(channel)


def users_twopc_stub():
    addr = os.getenv("USERS_ADDR", "users:50051")
    channel = grpc.insecure_channel(addr)
    return twopc_pb2_grpc.TwoPCParticipantServiceStub(channel)


def catalog_twopc_stub():
    addr = os.getenv("CATALOG_ADDR", "catalog:50051")
    channel = grpc.insecure_channel(addr)
    return twopc_pb2_grpc.TwoPCParticipantServiceStub(channel)


def inventory_twopc_stub():
    addr = os.getenv("INVENTORY_ADDR", "inventory:50051")
    channel = grpc.insecure_channel(addr)
    return twopc_pb2_grpc.TwoPCParticipantServiceStub(channel)


def circulation_twopc_stub():
    addr = os.getenv("CIRCULATION_ADDR", "circulation:50051")
    channel = grpc.insecure_channel(addr)
    return twopc_pb2_grpc.TwoPCParticipantServiceStub(channel)


def local_twopc_decision_stub():
    addr = os.getenv("LOCAL_DECISION_ADDR", "localhost:50061")
    channel = grpc.insecure_channel(addr)
    return twopc_pb2_grpc.TwoPCDecisionServiceStub(channel)
