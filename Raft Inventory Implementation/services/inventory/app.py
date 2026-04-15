"""
Inventory Service with Raft Consensus
======================================
This is the original inventory microservice (manages book stock counts)
with Raft consensus built on top of it so that 5 copies of this service
can stay in sync even if some nodes crash.

Two things are added on top of the original inventory code:
  Q3 - Leader Election   (nodes vote to pick one leader)
  Q4 - Log Replication   (leader replicates every write to followers)

How it works at a high level:
  1. All 5 nodes start as FOLLOWERS and wait for a leader.
  2. If no heartbeat arrives within a random timeout (1.5-3s), a node
     becomes a CANDIDATE and asks others to vote for it.
  3. The node that gets a majority of votes (3 out of 5) becomes LEADER.
  4. The LEADER sends heartbeats every 1 second to stay in power.
  5. When a client sends a write (e.g. AddCopies), the leader appends it
     to its log, sends the log to all followers, and only commits once
     a majority (3/5) acknowledge it.
  6. If a follower receives a write, it forwards it to the leader.
  7. Reads (GetAvailability) are forwarded to the leader to guarantee
     the client sees the latest committed state, but the leader just
     reads from its local dict -- no log entry or replication needed.
  8. Before serving a read, the leader verifies its "lease" -- it checks
     that it recently heard back from a majority of nodes via heartbeats.
     This prevents a partitioned leader from serving stale data (if it
     got cut off from the cluster and a new leader was elected).

Edge cases handled:
  - Dead nodes: leader skips them with a 0.5s timeout, stays leader.
  - No majority: if only 2/5 nodes are alive, writes are rejected with
    an error message explaining why.
  - Client connects to any node: followers forward ALL requests (reads
    and writes) to the leader so the client always sees fresh data.
  - Partitioned leader: if the leader can't reach a majority via
    heartbeats, it refuses reads to avoid returning stale data.
"""

import os
import time
import random
import threading
import logging
from concurrent import futures

import grpc

# =============================================================================
#  Proto Imports
# =============================================================================
# raft_pb2 / raft_pb2_grpc  -> generated at Docker build time from raft.proto
#                               (defines RequestVote, AppendEntries, ForwardRequest)
# inventory_pb2 / inventory_pb2_grpc -> from shared/gen (the original inventory proto)
#
# The try/except handles different PYTHONPATH setups (local dev vs Docker).

try:
    import raft_pb2
    import raft_pb2_grpc
except ImportError:
    from shared.gen import raft_pb2, raft_pb2_grpc

try:
    import inventory_pb2
    import inventory_pb2_grpc
except ImportError:
    from shared.gen import inventory_pb2, inventory_pb2_grpc


# =============================================================================
#  Configuration  (all from environment variables set in docker-compose.yml)
# =============================================================================

# Each node gets a unique ID (1-5) from docker-compose environment
NODE_ID    = int(os.getenv("NODE_ID", "1"))
# Total number of nodes in the Raft cluster
NODE_COUNT = int(os.getenv("NODE_COUNT", "5"))
# Port this node listens on for gRPC
GRPC_PORT  = int(os.getenv("PORT", "50051"))

# Build a dict of peer addresses: {2: "inventory-node2:50051", 3: ..., ...}
# We skip our own NODE_ID since we don't need to talk to ourselves.
PEERS = {}
for _i in range(1, NODE_COUNT + 1):
    if _i != NODE_ID:
        PEERS[_i] = os.getenv(f"PEER_{_i}", f"inventory-node{_i}:50051")

# --- Raft Timing Constants ---
HEARTBEAT_INTERVAL   = 1.0    # Leader sends heartbeat every 1 second
ELECTION_TIMEOUT_MIN = 1.5    # Minimum wait before starting an election
ELECTION_TIMEOUT_MAX = 3.0    # Maximum wait before starting an election
PEER_RPC_TIMEOUT     = 0.5    # Max time to wait for a peer to respond
                               # Kept short so dead nodes don't stall the leader

# Set up logging with node ID prefix so we can tell nodes apart in logs
logging.basicConfig(
    level=logging.INFO,
    format=f"[Node {NODE_ID}] %(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# =============================================================================
#  Raft State
# =============================================================================
# All mutable Raft state is stored in one object protected by a threading lock.
# This prevents race conditions since multiple threads (heartbeat, election,
# client requests) all read/write this state concurrently.

class RaftState:
    # The three possible roles a node can be in
    FOLLOWER  = "follower"    # Default state; listens for heartbeats
    CANDIDATE = "candidate"   # Trying to become leader (requesting votes)
    LEADER    = "leader"      # Accepted leader; sends heartbeats & handles writes

    def __init__(self):
        self.lock = threading.Lock()  # protects ALL fields below

        # --- Persistent state (survives restarts in real Raft; in-memory here) ---
        self.current_term = 0     # Monotonically increasing term number.
                                  # Incremented every time an election starts.
        self.voted_for    = None  # Which candidate we voted for in this term.
                                  # None = haven't voted yet. Each node votes
                                  # at most once per term (first-come-first-served).

        # --- Volatile state ---
        self.role           = self.FOLLOWER  # Current role of this node
        self.leader_id      = None           # ID of the node we believe is leader
        self.last_heartbeat = time.time()    # Timestamp of last heartbeat received.
                                             # If too much time passes without one,
                                             # we start an election.

        # --- Leader lease state ---
        # The leader tracks how many nodes responded to its most recent
        # heartbeat round.  Before serving a read, the leader checks that
        # it heard back from a majority within the last heartbeat interval.
        # This prevents a partitioned leader from serving stale reads --
        # if it can't reach a majority, it might have been deposed and a
        # new leader elected on the other side of the partition.
        self.last_heartbeat_ack_count = 0     # ACKs from most recent heartbeat round
        self.last_heartbeat_send_time = 0.0   # When the most recent heartbeat was sent

        # --- Log Replication state (Q4) ---
        self.log          = []    # The replicated log. Each entry is a dict:
                                  #   {"operation": "AddCopies",
                                  #    "book_id": "abc123",
                                  #    "count": 5,
                                  #    "term": 1,
                                  #    "index": 0}
        self.commit_index = -1   # Index of the last COMMITTED log entry.
                                  # -1 means nothing committed yet.
                                  # Entries are "committed" = safe to execute,
                                  # because a majority of nodes have them.

        # --- Replicated state machine (the actual inventory data) ---
        # This replaces the old global BOOKSSTOCK dict from the original code.
        # It's modified ONLY by executing committed log entries.
        self.inventory = {}   # {book_id: available_count}

    def random_election_timeout(self):
        """Return a random timeout between 1.5 and 3.0 seconds.
        The randomness prevents all nodes from starting elections at once
        (which would cause split votes)."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)


# Create the single global Raft state instance
raft = RaftState()


def leader_has_valid_lease():
    """Check if the leader's lease is still valid (called under raft.lock).

    A leader's lease is valid if BOTH conditions are true:
      1. The most recent heartbeat round happened within the last
         2 * HEARTBEAT_INTERVAL seconds (gives some slack for timing)
      2. A majority of nodes responded to that heartbeat round

    If the lease is invalid, the leader may have been partitioned away
    from the cluster and a new leader could have been elected.  In that
    case, it's not safe to serve reads because our state might be stale.

    Returns True if the leader can safely serve reads, False otherwise.
    """
    majority = (NODE_COUNT // 2) + 1
    time_since_heartbeat = time.time() - raft.last_heartbeat_send_time

    # Allow up to 2x the heartbeat interval as a grace period.
    # If we haven't sent a heartbeat yet (send_time == 0), the lease
    # is invalid because we just became leader and haven't confirmed
    # reachability yet.
    if raft.last_heartbeat_send_time == 0:
        return False
    if time_since_heartbeat > HEARTBEAT_INTERVAL * 2:
        return False
    if raft.last_heartbeat_ack_count < majority:
        return False
    return True


# =============================================================================
#  Inventory State Machine
# =============================================================================
# These functions modify raft.inventory (the book stock dictionary).
# They are ONLY called when a log entry is committed (has majority agreement).
# This is the "state machine" in Raft terminology -- the actual application
# logic that the consensus algorithm protects.

def apply_operation(op, book_id, count):
    """Apply one inventory operation to the state machine (raft.inventory).

    This is the same logic as the original InventoryService methods,
    but extracted into a function so it can be called when log entries
    are committed.

    Args:
        op:      Operation name ("AddCopies", "DecrementCopy", etc.)
        book_id: The book to operate on
        count:   How many copies (only used for AddCopies)

    Returns:
        dict with {ok, message, book_id, available} matching InventoryResponse
    """
    inv = raft.inventory

    if op == "AddCopies":
        # Add 'count' copies of this book to inventory
        inv[book_id] = inv.get(book_id, 0) + count
        return {"ok": True, "message": "Book added",
                "book_id": book_id, "available": inv[book_id]}

    elif op == "GetAvailability":
        # Just read the current count (no mutation)
        avail = inv.get(book_id, 0)
        msg = "Available copies:" if book_id in inv else "Book not in inventory:"
        return {"ok": True, "message": msg,
                "book_id": book_id, "available": avail}

    elif op == "DecrementCopy":
        # Remove one copy (e.g. when a book is checked out)
        if book_id in inv and inv[book_id] > 0:
            inv[book_id] -= 1
            return {"ok": True, "message": "Book taken",
                    "book_id": book_id, "available": inv[book_id]}
        elif book_id in inv:
            return {"ok": True, "message": "Book out of stock",
                    "book_id": book_id, "available": inv[book_id]}
        else:
            return {"ok": True, "message": "Book not in inventory:",
                    "book_id": book_id, "available": 0}

    elif op == "IncrementCopy":
        # Add one copy back (e.g. when a book is returned)
        inv[book_id] = inv.get(book_id, 0) + 1
        return {"ok": True, "message": "Book incremented",
                "book_id": book_id, "available": inv[book_id]}

    # Unknown operation
    return {"ok": False, "message": f"Unknown operation: {op}",
            "book_id": book_id, "available": 0}


def execute_committed_entries(up_to_index):
    """Execute all log entries that have been committed but not yet applied.

    When the leader commits new entries (after getting majority ACKs),
    or when a follower learns the commit_index has advanced, this function
    walks through each unapplied entry and calls apply_operation().

    Args:
        up_to_index: Apply entries up to and including this log index
    """
    start = raft.commit_index + 1  # Start from the first unapplied entry
    for i in range(start, up_to_index + 1):
        if i < len(raft.log):
            entry = raft.log[i]
            apply_operation(entry["operation"], entry["book_id"], entry["count"])
            log.info(f"Executed log[{i}]: {entry['operation']} book_id={entry['book_id']}")
    # Update commit_index to reflect what we've applied
    raft.commit_index = min(up_to_index, len(raft.log) - 1)


# =============================================================================
#  gRPC Raft Service  (inter-node communication)
# =============================================================================
# These RPCs are called BY OTHER NODES (not by clients/gateway).
# They implement the Raft protocol for leader election and log replication.

class RaftServiceServicer(raft_pb2_grpc.RaftServiceServicer):

    # ---- Q3: RequestVote RPC ------------------------------------------------
    # Called by a CANDIDATE node asking this node to vote for it.
    # Each node votes at most once per term, first-come-first-served.
    def RequestVote(self, request, context):
        with raft.lock:
            # Print the required RPC message (server side)
            log.info(f"Node {NODE_ID} runs RPC RequestVote called by Node {request.candidate_id}")

            # If the candidate has a higher term than us, we're outdated.
            # Step down to follower and update our term.
            if request.term > raft.current_term:
                log.info(f"Stepped down: Node {request.candidate_id} has higher "
                         f"term {request.term} (ours was {raft.current_term})")
                raft.current_term = request.term
                raft.role = RaftState.FOLLOWER
                raft.voted_for = None    # Reset vote for the new term
                raft.leader_id = None
                # Reset election timer so we don't immediately start our own election
                raft.last_heartbeat = time.time()

            # Decide whether to grant the vote
            grant = False
            if request.term >= raft.current_term:
                # Only vote if we haven't voted yet, or already voted for this candidate
                if raft.voted_for is None or raft.voted_for == request.candidate_id:
                    raft.voted_for = request.candidate_id
                    grant = True
                    # Reset election timeout so we don't immediately start our own election
                    raft.last_heartbeat = time.time()
                    log.info(f"Voted for Node {request.candidate_id} in term {request.term}")

            return raft_pb2.RequestVoteResponse(
                term=raft.current_term,
                vote_granted=grant,
            )

    # ---- Q3 + Q4: AppendEntries RPC -----------------------------------------
    # Dual purpose:
    #   1. HEARTBEAT (Q3): Leader sends this periodically to say "I'm alive"
    #   2. LOG REPLICATION (Q4): Leader sends its log + commit_index so
    #      followers can copy the log and execute committed entries.
    def AppendEntries(self, request, context):
        with raft.lock:
            # Print the required RPC message (server side)
            log.info(f"Node {NODE_ID} runs RPC AppendEntries called by Node {request.leader_id}")

            # If the leader has a higher term, update ours
            if request.term > raft.current_term:
                log.info(f"Stepped down: Node {request.leader_id} has higher "
                         f"term {request.term} (ours was {raft.current_term})")
                raft.current_term = request.term
                raft.voted_for = None

            # If the leader's term is LOWER than ours, reject it
            # (this leader is outdated)
            if request.term < raft.current_term:
                return raft_pb2.AppendEntriesResponse(
                    term=raft.current_term, success=False)

            # --- Valid heartbeat from a legitimate leader ---

            # Accept this node as our leader and reset to follower state
            raft.role = RaftState.FOLLOWER
            raft.leader_id = request.leader_id
            # Reset election timeout -- we just heard from the leader
            raft.last_heartbeat = time.time()

            # --- Q4: Log Replication ---
            # The leader sends its ENTIRE log. We replace ours with it.
            # (Simplified version of Raft -- real Raft does incremental sync)
            if request.entries:
                raft.log = []
                for e in request.entries:
                    raft.log.append({
                        "operation": e.operation,
                        "book_id":   e.book_id,
                        "count":     e.count,
                        "term":      e.term,
                        "index":     e.index,
                    })

            # If the leader's commit_index is ahead of ours, execute the
            # newly committed entries to update our local inventory state
            if request.commit_index > raft.commit_index:
                execute_committed_entries(request.commit_index)

            return raft_pb2.AppendEntriesResponse(
                term=raft.current_term, success=True)

    # ---- Q4: ForwardRequest RPC ---------------------------------------------
    # When a FOLLOWER receives a client write request, it can't handle it
    # (only the leader can). So the follower calls this RPC on the leader
    # to forward the request.
    def ForwardRequest(self, request, context):
        """Handle a forwarded client request -- only the leader processes these.
        For writes: go through log replication (handle_client_request).
        For reads (GetAvailability): verify leader lease first, then read
        from committed state.  The lease check ensures a partitioned leader
        doesn't serve stale data."""
        # Print the required RPC message (server side)
        log.info(f"Node {NODE_ID} runs RPC ForwardRequest called by a follower")

        if request.operation == "GetAvailability":
            # Verify leader lease before serving the read
            with raft.lock:
                if not leader_has_valid_lease():
                    log.warning("Leader lease invalid on forwarded read")
                    return raft_pb2.ForwardRequestResponse(
                        ok=False,
                        message="Leader lease expired, cannot serve read. "
                                "Try again shortly.",
                        book_id=request.book_id, available=0)
                # Lease valid -- safe to read
                avail = raft.inventory.get(request.book_id, 0)
                msg = "Available copies:" if request.book_id in raft.inventory \
                      else "Book not in inventory:"
            return raft_pb2.ForwardRequestResponse(
                ok=True, message=msg,
                book_id=request.book_id, available=avail)
        else:
            # Write operation -- go through full log replication
            result = handle_client_request(request.operation, request.book_id, request.count)
            return raft_pb2.ForwardRequestResponse(
                ok=result["ok"], message=result["message"],
                book_id=result["book_id"], available=result["available"])


# =============================================================================
#  gRPC Inventory Service  (client-facing API -- same interface as original)
# =============================================================================
# This is the service that the gateway (and other microservices) call.
# The RPC methods are the same as the original inventory service:
#   AddCopies, GetAvailability, DecrementCopy, IncrementCopy
#
# ALL operations (including reads) are routed through the Raft leader
# to guarantee consistency.  A follower could have stale data if the
# latest heartbeat hasn't arrived yet, so we always read from the leader
# who has the most up-to-date committed state.
#
#   - If this node IS the leader   -> handle the request directly
#   - If this node is a FOLLOWER   -> forward the request to the leader

class InventoryService(inventory_pb2_grpc.InventoryServiceServicer):

    def _forward_to_leader(self, operation, book_id, count):
        """Forward a request to the current Raft leader.

        This is called when a follower receives ANY request (read or write).
        We always go through the leader to guarantee the client sees the
        most up-to-date committed state.  The follower looks up who the
        leader is and sends the request there via the ForwardRequest RPC.
        """
        # Read the leader_id under the lock
        with raft.lock:
            leader_id = raft.leader_id

        # If we don't know who the leader is, tell the client to retry
        if leader_id is None or leader_id not in PEERS:
            return inventory_pb2.InventoryResponse(
                ok=False, message="No leader elected yet, try again later",
                book_id=book_id, available=0)
        try:
            addr = PEERS[leader_id]
            # Print the required RPC message (client side)
            log.info(f"Node {NODE_ID} sends RPC ForwardRequest to Node {leader_id}")
            # Open a gRPC channel to the leader and send the request
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            resp = stub.ForwardRequest(
                raft_pb2.ForwardRequestMessage(
                    operation=operation, book_id=book_id, count=count),
                timeout=5)  # 5 second timeout for forwarding
            channel.close()
            # Return the leader's response to the client
            return inventory_pb2.InventoryResponse(
                ok=resp.ok, message=resp.message,
                book_id=resp.book_id, available=resp.available)
        except Exception as e:
            log.warning(f"Failed to forward to leader Node {leader_id}: {e}")
            return inventory_pb2.InventoryResponse(
                ok=False, message="Leader unavailable, try again later",
                book_id=book_id, available=0)

    def _route_to_leader(self, operation, book_id, count=0):
        """Route ANY operation (read or write) through the Raft leader.

        All operations go through the leader to guarantee consistency.
        If we're the leader, handle it directly.
        If we're a follower, forward it to the leader.

        For WRITES (AddCopies, DecrementCopy, IncrementCopy):
          The leader appends to log, replicates, and commits with majority.
        For READS (GetAvailability):
          The leader first verifies its lease is still valid (i.e., it
          recently heard back from a majority of nodes via heartbeats).
          This prevents a partitioned leader from serving stale reads.
          If the lease is valid, it reads directly from its own state.
        """
        with raft.lock:
            role = raft.role

        if role == RaftState.LEADER:
            # We ARE the leader -- handle it directly
            if operation == "GetAvailability":
                # Before serving a read, verify our leader lease.
                # A partitioned leader might have been deposed -- if we
                # can't confirm we recently heard from a majority, reject
                # the read to avoid returning stale data.
                with raft.lock:
                    if not leader_has_valid_lease():
                        log.warning("Leader lease invalid -- cannot confirm "
                                    "majority reachability, rejecting read")
                        return inventory_pb2.InventoryResponse(
                            ok=False,
                            message="Leader lease expired, cannot serve read "
                                    "(may be partitioned). Try again shortly.",
                            book_id=book_id, available=0)
                    # Lease is valid -- safe to read
                    avail = raft.inventory.get(book_id, 0)
                    msg = "Available copies:" if book_id in raft.inventory \
                          else "Book not in inventory:"
                return inventory_pb2.InventoryResponse(
                    ok=True, message=msg,
                    book_id=book_id, available=avail)
            else:
                # Write operation -- go through log replication
                res = handle_client_request(operation, book_id, count)
                return inventory_pb2.InventoryResponse(
                    ok=res["ok"], message=res["message"],
                    book_id=res["book_id"], available=res["available"])
        else:
            # We're a follower -- forward to whoever is the leader
            return self._forward_to_leader(operation, book_id, count)

    # -- The original InventoryService RPCs, now ALL routed through leader ---

    def AddCopies(self, request, context):
        """Add copies of a book to inventory. Routed through Raft leader."""
        return self._route_to_leader("AddCopies", request.book_id, request.count)

    def GetAvailability(self, request, context):
        """Check how many copies of a book are available.
        Routed through the Raft leader to guarantee we read the latest
        committed state.  A follower might have stale data if the most
        recent heartbeat hasn't arrived yet, so we always ask the leader."""
        return self._route_to_leader("GetAvailability", request.book_id)

    def DecrementCopy(self, request, context):
        """Remove one copy (checkout). Routed through Raft leader."""
        return self._route_to_leader("DecrementCopy", request.book_id)

    def IncrementCopy(self, request, context):
        """Add one copy back (return). Routed through Raft leader."""
        return self._route_to_leader("IncrementCopy", request.book_id)


# =============================================================================
#  Leader: Handle Client Writes (Log Replication - Q4)
# =============================================================================
# This is the core of Q4. When the leader receives a write request:
#   Step 1: Append the operation to the leader's log
#   Step 2: Send the entire log to all followers (in parallel)
#   Step 3: Wait for a MAJORITY of ACKs, then commit and execute
#
# If we don't get a majority (e.g., 3 of 5 nodes are dead), the write
# is REJECTED -- we roll back the log entry and return an error.

def _build_entries_pb(log_list):
    """Helper: convert our internal log dicts into protobuf LogEntry messages
    for sending over gRPC."""
    entries = []
    for e in log_list:
        entries.append(raft_pb2.LogEntry(
            operation=e["operation"], book_id=e["book_id"],
            count=e["count"], term=e["term"], index=e["index"]))
    return entries


def handle_client_request(operation, book_id, count):
    """Process a client write request as the Raft leader.

    This function implements the log replication protocol:
    1. Append the operation to our log
    2. Send the log to all followers and count ACKs
    3. If majority ACKed -> commit and execute the operation
       If not enough ACKs -> roll back and return error

    Args:
        operation: "AddCopies", "DecrementCopy", or "IncrementCopy"
        book_id:   The book to operate on
        count:     Number of copies (only for AddCopies)

    Returns:
        dict with {ok, message, book_id, available}
    """

    # ---- Step 1: Append to leader's log ----
    with raft.lock:
        # Safety check: only the leader should be calling this
        if raft.role != RaftState.LEADER:
            return {"ok": False, "message": "Not the leader",
                    "book_id": book_id, "available": 0}

        # Create a new log entry with the operation details
        new_index = len(raft.log)
        entry = {"operation": operation, "book_id": book_id, "count": count,
                 "term": raft.current_term, "index": new_index}
        raft.log.append(entry)
        log.info(f"Leader appended log[{new_index}]: {operation} book_id={book_id}")

        # Snapshot current state for sending to followers
        # (we release the lock during network calls)
        current_term   = raft.current_term
        current_log    = list(raft.log)       # copy so it's safe outside the lock
        current_commit = raft.commit_index

    # ---- Step 2: Replicate to all followers in parallel ----
    # We send AppendEntries to every peer at the same time using threads.
    # Each peer that responds with success counts as one ACK.
    ack_count = 1          # Leader itself counts as 1 ACK
    majority  = (NODE_COUNT // 2) + 1  # For 5 nodes, majority = 3
    ack_lock  = threading.Lock()       # Protects ack_count from race conditions

    def send_append_entries(peer_id, peer_addr):
        """Thread function: send the log to one follower and count the ACK."""
        nonlocal ack_count
        try:
            # Print the required RPC message (client side)
            log.info(f"Node {NODE_ID} sends RPC AppendEntries to Node {peer_id}")

            # Open gRPC channel to this peer
            channel = grpc.insecure_channel(peer_addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)

            # Send the entire log + current commit index
            resp = stub.AppendEntries(
                raft_pb2.AppendEntriesRequest(
                    term=current_term,
                    leader_id=NODE_ID,
                    entries=_build_entries_pb(current_log),
                    commit_index=current_commit),
                timeout=PEER_RPC_TIMEOUT)  # 0.5s timeout -- don't wait long
            channel.close()

            if resp.success:
                # Follower accepted our log -- count it as an ACK
                with ack_lock:
                    ack_count += 1
            elif resp.term > current_term:
                # This follower has a HIGHER term -- we're outdated, step down
                with raft.lock:
                    if resp.term > raft.current_term:
                        raft.current_term = resp.term
                        raft.role = RaftState.FOLLOWER
                        raft.voted_for = None
                        raft.leader_id = None
                        # Reset election timer so we wait before starting a new election
                        raft.last_heartbeat = time.time()

        except grpc.RpcError:
            # Peer is dead or unreachable -- just skip it.
            # IMPORTANT: this does NOT cause the leader to lose its role.
            # The leader stays leader as long as IT is healthy.
            # Dead-node timeouts do NOT trigger new elections.
            log.warning(f"Node {peer_id} unreachable during log replication (skipped)")
        except Exception:
            log.warning(f"Node {peer_id} unreachable during log replication (skipped)")

    # Launch one thread per peer to send AppendEntries in parallel
    threads = []
    for pid, paddr in PEERS.items():
        t = threading.Thread(target=send_append_entries, args=(pid, paddr), daemon=True)
        t.start()
        threads.append(t)

    # Wait for all threads to finish (with a timeout so dead nodes don't block us)
    for t in threads:
        t.join(timeout=PEER_RPC_TIMEOUT + 0.5)

    # ---- Step 3: Check majority and commit (or roll back) ----
    with raft.lock:
        # If we lost leadership while waiting for ACKs, abort
        if raft.role != RaftState.LEADER:
            return {"ok": False, "message": "Lost leadership during replication",
                    "book_id": book_id, "available": 0}

        if ack_count >= majority:
            # SUCCESS: We got enough ACKs -- commit the entry!
            # This executes the operation on our local inventory state
            execute_committed_entries(new_index)
            log.info(f"Committed up to index {new_index} "
                     f"({ack_count}/{NODE_COUNT} ACKs, needed {majority})")

            # Build the response message based on what operation we did
            avail = raft.inventory.get(book_id, 0)
            if operation == "AddCopies":
                msg = "Book added"
            elif operation == "DecrementCopy":
                if book_id in raft.inventory and avail >= 0:
                    msg = "Book taken"
                else:
                    msg = "Book out of stock"
            elif operation == "IncrementCopy":
                msg = "Book incremented"
            else:
                msg = "OK"
            return {"ok": True, "message": msg,
                    "book_id": book_id, "available": avail}
        else:
            # FAILURE: Not enough ACKs -- cannot safely commit.
            # This happens when too many nodes are dead (e.g. only 2 of 5 alive).
            # Roll back: remove the entry we just appended since it can't be committed.
            if len(raft.log) > new_index and raft.log[new_index] == entry:
                raft.log.pop(new_index)
            log.warning(f"Cannot commit: only {ack_count}/{NODE_COUNT} ACKs "
                        f"(need {majority}).  Cluster does not have majority.")
            return {"ok": False,
                    "message": (f"Cannot commit: only {ack_count}/{NODE_COUNT} "
                                f"nodes alive (need {majority} for majority)"),
                    "book_id": book_id, "available": 0}


# =============================================================================
#  Background Threads
# =============================================================================
# Two threads run in the background on every node:
#   1. election_timer_loop  -- watches for missing heartbeats and starts elections
#   2. heartbeat_loop       -- (leader only) sends heartbeats to all followers

def election_timer_loop():
    """Background thread: watches for election timeout.

    Every 0.1 seconds, this checks if we've gone too long without hearing
    from a leader. If the timeout expires:
      1. We become a CANDIDATE
      2. We increment the term (a new "election round")
      3. We vote for ourselves
      4. We ask all peers to vote for us (run_election)

    The timeout is randomized (1.5-3s) so that different nodes time out
    at different times, reducing the chance of split votes.

    IMPORTANT: If the elapsed time is way larger than the timeout (e.g.,
    10+ seconds), it means this node was likely paused/frozen and just
    resumed. In that case, we reset the timer and wait a full timeout
    period instead of immediately starting an election. This gives the
    current leader a chance to send us a heartbeat first.
    """
    timeout = raft.random_election_timeout()
    while True:
        time.sleep(0.1)  # Check every 100ms
        with raft.lock:
            # Leaders don't need to watch for elections -- they ARE the leader
            if raft.role == RaftState.LEADER:
                continue

            # Check if enough time has passed since last heartbeat
            elapsed = time.time() - raft.last_heartbeat
            if elapsed < timeout:
                continue  # Still within timeout, keep waiting

            # If elapsed time is larger than the max election timeout, this
            # node was probably paused/frozen and just resumed, OR it missed
            # several heartbeats.  Either way, reset the timer and wait a full
            # cycle so the existing leader can send us a heartbeat before we
            # start a disruptive election.  The max election timeout is 3s,
            # and heartbeats come every 1s, so if we've been waiting more
            # than 3s without hearing anything, something unusual happened.
            if elapsed > ELECTION_TIMEOUT_MAX:
                log.info(f"Detected possible resume from pause "
                         f"(elapsed={elapsed:.1f}s). Resetting election timer "
                         f"to wait for leader heartbeat.")
                raft.last_heartbeat = time.time()
                continue

            # --- Election timeout expired! ---
            # No heartbeat received in time, so we assume the leader is dead.
            # Transition to CANDIDATE and start an election.
            raft.role = RaftState.CANDIDATE
            raft.current_term += 1       # New term for this election
            raft.voted_for = NODE_ID     # Vote for ourselves
            term = raft.current_term
            raft.last_heartbeat = time.time()  # Reset timer

        log.info(f"Election timeout! Starting election for term {term}")
        run_election(term)
        # Pick a new random timeout for the next round
        timeout = raft.random_election_timeout()


def run_election(term):
    """Run a leader election: ask all peers to vote for us.

    We send RequestVote RPCs to all peers in parallel and count the votes.
    If we get a majority (3 out of 5), we become the leader.
    If not, we revert to follower and wait for the next timeout.

    Args:
        term: The term number we're running the election for
    """
    votes     = 1  # We already voted for ourselves
    majority  = (NODE_COUNT // 2) + 1  # Need 3 out of 5
    vote_lock = threading.Lock()       # Protects 'votes' counter

    def request_vote(peer_id, peer_addr):
        """Thread function: ask one peer to vote for us."""
        nonlocal votes
        try:
            # Print the required RPC message (client side)
            log.info(f"Node {NODE_ID} sends RPC RequestVote to Node {peer_id}")

            channel = grpc.insecure_channel(peer_addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            resp = stub.RequestVote(
                raft_pb2.RequestVoteRequest(term=term, candidate_id=NODE_ID),
                timeout=PEER_RPC_TIMEOUT)
            channel.close()

            if resp.vote_granted:
                # Peer voted for us!
                with vote_lock:
                    votes += 1
            elif resp.term > term:
                # Peer has a higher term -- someone else is ahead of us.
                # Step down to follower.
                with raft.lock:
                    if resp.term > raft.current_term:
                        raft.current_term = resp.term
                        raft.role = RaftState.FOLLOWER
                        raft.voted_for = None
                        raft.leader_id = None
                        # Reset election timer so we wait before trying again
                        raft.last_heartbeat = time.time()
        except grpc.RpcError:
            # Peer is dead -- skip it. We can still win if enough others vote.
            log.warning(f"Node {peer_id} unreachable during election (skipped)")
        except Exception:
            log.warning(f"Node {peer_id} unreachable during election (skipped)")

    # Send RequestVote to all peers in parallel
    threads = []
    for pid, paddr in PEERS.items():
        t = threading.Thread(target=request_vote, args=(pid, paddr), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=PEER_RPC_TIMEOUT + 0.5)

    # Check the results
    with raft.lock:
        # If our role or term changed while we were waiting, abort
        # (someone else may have won, or a higher term was discovered)
        if raft.role != RaftState.CANDIDATE or raft.current_term != term:
            return

        if votes >= majority:
            # We won the election!
            raft.role = RaftState.LEADER
            raft.leader_id = NODE_ID
            log.info(f"*** Won election for term {term} with "
                     f"{votes}/{NODE_COUNT} votes! ***")
        else:
            # Didn't get enough votes -- go back to follower and wait
            raft.role = RaftState.FOLLOWER
            log.info(f"Election failed for term {term}: "
                     f"{votes}/{NODE_COUNT} votes (need {majority})")


def heartbeat_loop():
    """Background thread (leader only): send periodic heartbeats.

    Every 1 second, if we're the leader, we send AppendEntries RPCs
    to all followers. This serves three purposes:
      1. Tells followers "I'm still alive, don't start an election"
      2. Syncs the log and commit_index so followers stay up-to-date
      3. Tracks how many followers responded (ACK count) so we can
         verify our leader lease before serving reads.

    The ACK count from each heartbeat round is stored in
    raft.last_heartbeat_ack_count.  Before serving a read, the leader
    checks that it heard back from a majority within the last heartbeat
    interval.  If not, it may have been partitioned and deposed, so it
    refuses the read.

    IMPORTANT: If a follower is dead and doesn't respond, we just skip it.
    The timeout from a dead node does NOT affect the leader's status.
    The leader stays leader as long as IT is healthy.
    """
    while True:
        time.sleep(HEARTBEAT_INTERVAL)  # Wait 1 second between heartbeats

        # Only the leader sends heartbeats
        with raft.lock:
            if raft.role != RaftState.LEADER:
                continue
            # Snapshot state for sending (release lock during network calls)
            current_term   = raft.current_term
            current_log    = list(raft.log)
            current_commit = raft.commit_index

        # Count how many followers ACK this heartbeat round
        # Start at 1 because the leader itself counts
        hb_ack_count = 1
        hb_ack_lock  = threading.Lock()

        def send_heartbeat(peer_id, peer_addr):
            """Thread function: send heartbeat to one follower."""
            nonlocal hb_ack_count
            try:
                # Print the required RPC message (client side)
                log.info(f"Node {NODE_ID} sends RPC AppendEntries to Node {peer_id}")

                channel = grpc.insecure_channel(peer_addr)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                resp = stub.AppendEntries(
                    raft_pb2.AppendEntriesRequest(
                        term=current_term,
                        leader_id=NODE_ID,
                        entries=_build_entries_pb(current_log),
                        commit_index=current_commit),
                    timeout=PEER_RPC_TIMEOUT)
                channel.close()

                if resp.success:
                    # Follower responded successfully -- count this ACK
                    with hb_ack_lock:
                        hb_ack_count += 1

                # If the follower has a higher term, we need to step down
                if resp.term > current_term:
                    with raft.lock:
                        if resp.term > raft.current_term:
                            raft.current_term = resp.term
                            raft.role = RaftState.FOLLOWER
                            raft.voted_for = None
                            raft.leader_id = None
                            # Reset election timer so we wait before starting election
                            raft.last_heartbeat = time.time()
                            log.info(f"Stepped down: Node {peer_id} "
                                     f"has higher term {resp.term}")
            except grpc.RpcError:
                # Dead node -- silently skip.
                # CRITICAL: We do NOT step down or trigger an election here.
                # A dead follower doesn't mean the leader is broken.
                pass
            except Exception:
                pass

        # Send heartbeats to all peers in parallel
        threads = []
        for pid, paddr in PEERS.items():
            t = threading.Thread(target=send_heartbeat, args=(pid, paddr), daemon=True)
            t.start()
            threads.append(t)
        # Wait briefly for responses (don't block forever on dead nodes)
        for t in threads:
            t.join(timeout=PEER_RPC_TIMEOUT + 0.2)

        # Store the results of this heartbeat round so that read requests
        # can check whether the leader still has a valid lease
        with raft.lock:
            if raft.role == RaftState.LEADER:
                raft.last_heartbeat_ack_count = hb_ack_count
                raft.last_heartbeat_send_time = time.time()
                log.info(f"Heartbeat round complete: {hb_ack_count}/{NODE_COUNT} "
                         f"nodes reachable")


# =============================================================================
#  Server Startup
# =============================================================================

def serve():
    """Start the gRPC server and background threads.

    The server registers TWO gRPC services on the same port:
      1. RaftService       -- for inter-node Raft RPCs (RequestVote, AppendEntries)
      2. InventoryService  -- for client-facing inventory RPCs (AddCopies, etc.)

    Then it starts two background threads:
      1. election_timer_loop  -- watches for missing heartbeats
      2. heartbeat_loop       -- leader sends periodic heartbeats
    """
    log.info(f"Starting Raft inventory node (ID={NODE_ID}, peers={PEERS})")

    # Create gRPC server with thread pool for handling concurrent requests
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))

    # Register BOTH services on the same port so the node handles
    # both Raft protocol traffic and client inventory requests
    raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServiceServicer(), server)
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryService(), server)

    server.add_insecure_port(f"0.0.0.0:{GRPC_PORT}")
    server.start()
    log.info(f"gRPC server listening on port {GRPC_PORT}")

    # Start background threads for Raft protocol
    threading.Thread(target=election_timer_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    # All nodes start as FOLLOWER -- an election will happen automatically
    # once the first node's election timeout expires (1.5-3 seconds)
    log.info("All threads started. Node is FOLLOWER, waiting for election...")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
