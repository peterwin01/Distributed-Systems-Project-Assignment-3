import os
import uuid
from concurrent import futures

import grpc

#import generated protobuf
from shared.gen import users_pb2
from shared.gen import users_pb2_grpc

# In-memory database
USERS = {}

# gRPC Service Call from users.proto
# Receives: request.name, request.email and Returns: UserResponse.
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
            return users_pb2.UserResponse(
                ok=False,
                message="User not found"
            )

        return users_pb2.UserResponse(
            ok=True,
            message="OK",
            user_id=user["user_id"],
            name=user["name"],
            email=user["email"],
        )

def serve():
    port = int(os.getenv("PORT", "50051"))
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 
    users_pb2_grpc.add_UsersServiceServicer_to_server(
        UsersService(),
        server
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    print(f"[users] listening on {port}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
