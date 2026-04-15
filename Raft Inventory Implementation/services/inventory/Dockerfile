# Dockerfile for Inventory Service (Raft Node)
# =============================================
# Each of the 5 inventory nodes uses this same Dockerfile.
# It installs Python dependencies, compiles the raft.proto file into
# Python code, then runs app.py which starts the Raft + Inventory server.

FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies (grpcio, grpcio-tools, protobuf)
COPY services/inventory/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the pre-generated protobuf stubs for inventory, catalog, etc.
# These were already generated and checked into shared/gen/
COPY shared ./shared

# Copy the .proto files and GENERATE the Raft protobuf stubs at build time.
# This runs the protobuf compiler (protoc) on raft.proto and inventory.proto,
# producing raft_pb2.py and raft_pb2_grpc.py in shared/gen/.
# We also regenerate inventory stubs to make sure everything is consistent.
COPY proto ./proto
RUN mkdir -p /app/shared/gen && \
    python -m grpc_tools.protoc \
      -I./proto \
      --python_out=/app/shared/gen \
      --grpc_python_out=/app/shared/gen \
      ./proto/raft.proto ./proto/inventory.proto

# Copy the inventory service code (app.py)
COPY services/inventory ./services/inventory

# PYTHONPATH includes both /app and /app/shared/gen so that
# "import raft_pb2" and "from shared.gen import ..." both work
ENV PYTHONPATH=/app:/app/shared/gen
ENV PORT=50051

WORKDIR /app/services/inventory
EXPOSE 50051

# Start the Raft-enabled inventory node
CMD ["python", "app.py"]
