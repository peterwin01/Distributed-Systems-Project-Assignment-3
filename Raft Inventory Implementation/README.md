# CSE5306-project2 - Distributed Library Management System
<p>Authors: Evelyn Lopez Paz & Aadhitya Kumar</p>
<p>This project implements a distributed management system using a microservice architecture.<br></p>
<p>It is similar to a library management system where a user is able to checkin/checkout a book, see the library catalog and publish books in the catalog section.</p>
<h3>Tech Stack</h3>
<ul>
  <li> Python (FastAPI + gRPC</li>
  <li> HTML/CSS (Jinja2 templates</li>
  <li> Docker + Docker Compose</li>
  <li> gRPC (Protocol Buffers)</li>
</ul>

<h3>Functional Requirements (6 nodes)<br></h3>
<ul>
  <li> <b>Gateway-</b> Translates HTTP to gRPC</li>
  <li> <b>Users-</b> Register and Retrieve Users</li>
  <li> <b>Catalog-</b> Publish and retrive books</li>
  <li> <b>Inventory-</b> Manage Book Inventory</li>
  <li> <b>Circulation-</b> Check in / Check out books</li>
  <li> <b>Audit-</b> Log system events (register, publish, checkout, checkin)</li>
</ul>

<h2>Running the Project</h2>
<h3>Prerequisites</h3>
<p>Make sure the following are installed:</p>
<ul>
  <li>Git</li>
  
    https://git-scm.com/download/
        
  <li>Docker Desktop</li>
    
    https://www.docker.com/products/docker-desktop/
    
  <li>Python 3.11+</li>

    https://www.python.org/downloads/release/python-3118/
  
  <li>grpcio-tools installed locally</li>

    pip install grpcio grpcio-tools protobuf
  
</ul>

<h3>Run the Project</h3>
<ol>
  <li>Install the project repo, and go into the folder</li>
  <li>Generate the Python gRPC files from the .proto definitions.<br> This generates the shared/gen/*folder*_pb2.py files & shared/gen/*folder*_pb2_grpc.py</li>

    python -m grpc_tools.protoc \
    -I proto \
    --python_out=shared/gen \
    --grpc_python_out=shared/gen \
    proto/*.proto

  <li>Build Docker Containers </li>

    docker compose build --no-cache

  <li> Start the Docker </li>

    docker compose up

  <li>Access the Application through your local host port 8080, with this you should see the Library System homepage!</li>

    http://localhost:8080
  
</ol>
