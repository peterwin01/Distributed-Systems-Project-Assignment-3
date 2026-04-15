# Distributed Systems Project – 2PC and Raft Implementation

##  Team Members

* Sachin Malik
* Peter Nguyen

---

##  Project Overview

This project implements two key distributed system protocols:

* **Two-Phase Commit (2PC)** for ensuring atomic transactions across multiple services
* **Raft Consensus Algorithm** for leader election and fault-tolerant log replication

We extended a microservices-based system (UTA Marketplace) using:

* **gRPC** for inter-service communication
* **Docker** for containerized deployment

---

##  Technologies Used

* Python (FastAPI)
* gRPC & Protocol Buffers
* Docker & Docker Compose
* Distributed Systems Concepts (2PC, Raft)

---

#  How to Run the Project

## Step 1: Clone Repository

```bash
git clone 
cd 
```

## Step 2: Start All Services

```bash
docker compose up --build
```

## Step 3: Open Application

Open browser:

```
http://localhost:8080
```

---

#  System Architecture

## 🔹 2PC Node Mapping

* Node 1 → Gateway (Coordinator)
* Node 2 → Users Service
* Node 3 → Catalog Service
* Node 4 → Inventory Service
* Node 5 → Circulation Service

##  Raft Cluster

* Multiple inventory nodes form a cluster
* Node states:

  * Follower
  * Candidate
  * Leader

---

#  Two-Phase Commit (2PC)

## Feature Implemented

Checkout (Circulation feature)

## Communication

All communication between coordinator and participants in both phases is implemented using **gRPC RPC calls defined via Protocol Buffers**.

---

## Phase 1: Voting Phase

1. Gateway (Node 1) sends `PrepareCheckout` to all participants
2. Each participant validates:

   * Users → user exists
   * Catalog → book exists
   * Inventory → stock available
   * Circulation → not already checked out
3. Each returns:

   * `COMMIT` or `ABORT`

---

## Phase 2: Decision Phase

* If ALL vote COMMIT → `GLOBAL_COMMIT`
* If ANY vote ABORT → `GLOBAL_ABORT`

Coordinator then sends `FinalizeCheckout` to all nodes.

---

## ✅ How to Test 2PC

### ✔ Successful Transaction

1. Register a user
2. Publish a book
3. Checkout using valid user + book

Expected:

* GLOBAL_COMMIT
* Checkout successful

---

### ❌ Failure Scenarios

#### 1. Invalid User

Use random user ID
→ GLOBAL_ABORT

#### 2. Invalid Book

Use non-existing book ID
→ GLOBAL_ABORT

#### 3. Out of Stock

Checkout same book repeatedly
→ GLOBAL_ABORT

#### 4. Participant Failure (IMPORTANT)

Stop inventory node:

```bash
docker stop twopc-node4-inventory
```

Then perform checkout.

Expected:

* Transaction aborted
* Message: participant unavailable
* Decision: GLOBAL_ABORT

---

##  Failure Handling Explanation

In the participant unavailability case, the coordinator fails to receive a response due to RPC failure. The system handles this by aborting the transaction and returning a **GLOBAL_ABORT decision**.

---
#  Raft Implementation

## Features Implemented

* Leader Election
* Heartbeats
* Log Replication
* Fault Tolerance

---

## Leader Election

* All nodes start as followers
* If no heartbeat → become candidate
* Candidate requests votes
* Majority → becomes leader

---

## Log Replication

1. Client sends request to any node
2. Request forwarded to leader
3. Leader appends log entry
4. Sends to followers
5. Majority ACK → commit

---

✅ How to Test Raft

✔ Successful Transaction

Publish a book

Add copies of book

Expected:

Leader commits log[0] publish given majority ACK
Followers commit log[0] on next heartbeat after leader executes
Leader commits log[1] add copies given majority ACK
Followers commit log[1] on next heartbeat after leader executes

---

#  Raft Failure Test Cases (Q5)

### 1. Leader Failure

* Stop leader
* New leader elected

### 2. Follower Failure

* Stop follower
* System continues

### 3. Election Timeout

* Delay heartbeat
* Node becomes candidate

### 4. New Node Joining

* Add node
* Syncs with leader

### 5. Log Inconsistency

* Outdated logs overwritten
* System restored

---

# ⚠️ Important Notes

* Each service runs as a **separate Docker container**
* All communication uses **gRPC**
* 2PC ensures **atomicity (all-or-nothing)**
* Raft ensures **fault tolerance and consistency**
* Failure scenarios are simulated using **Docker stop/start**

---

#  Anything Unusual

* 2PC implemented at **application level (checkout feature)**
* Raft implemented on **inventory as distributed cluster**
* Logs printed for:

  * Voting Phase
  * Decision Phase
  * Raft RPCs
* Failure simulation done using Docker containers

---

#  AI Usage

AI tools (ChatGPT) were used for:

* Understanding distributed system concepts
* Debugging issues
* Structuring implementation and documentation

---

#  GitHub Repository



---

#  Work Distribution

**Sachin Malik**

* Implemented Two-Phase Commit (2PC)
* Designed voting and decision phases
* Implemented gRPC communication for 2PC
* Tested failure scenarios (abort cases)

**Peter Nguyen**

* Implemented Raft consensus
* Designed leader election and log replication
* Implemented 5 failure test cases (Q5)
* Managed Docker-based node simulation

---

#  Conclusion

This project demonstrates:

* Strong consistency using 2PC
* Fault-tolerant consensus using Raft
* Real-world distributed system behavior using microservices and Docker

---



