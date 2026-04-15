from concurrent import futures
import threading
import uuid

import grpc
import grpc_clients
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from shared.gen import audit_pb2, catalog_pb2, circulation_pb2, inventory_pb2, twopc_pb2, twopc_pb2_grpc, users_pb2

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
bookIDs = []
logNum = 0


def log_voting_send(node_from: str, rpc_name: str, node_to: str):
    print(
        f"Phase Voting of Node {node_from} sends RPC {rpc_name} to Phase Voting of Node {node_to}",
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


class TwoPCDecisionService(twopc_pb2_grpc.TwoPCDecisionServiceServicer):
    def TriggerDecision(self, request, context):
        log_decision_run("1", "TriggerDecision", "1")
        all_commit = all(v.vote == twopc_pb2.VOTE_COMMIT for v in request.votes)
        decision = twopc_pb2.GLOBAL_COMMIT if all_commit else twopc_pb2.GLOBAL_ABORT
        return twopc_pb2.DecisionResult(transaction_id=request.transaction_id, decision=decision)


def start_twopc_decision_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    twopc_pb2_grpc.add_TwoPCDecisionServiceServicer_to_server(TwoPCDecisionService(), server)
    server.add_insecure_port("[::]:50061")
    server.start()
    print("[twopc-node1-gateway] Local 2PC decision service listening on 50061", flush=True)
    server.wait_for_termination()


threading.Thread(target=start_twopc_decision_server, daemon=True).start()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/users", response_class=HTMLResponse)
def users_page(request: Request, user_id: str | None = None):
    result = None
    if user_id:
        try:
            res = grpc_clients.users_stub().GetUser(users_pb2.GetUserRequest(user_id=user_id))
            result = {
                "ok": res.ok,
                "message": res.message,
                "user_id": res.user_id,
                "name": res.name,
                "email": res.email,
            }
        except grpc.RpcError as e:
            result = {
                "ok": False,
                "message": f"gRPC error calling Users: {e.code()} - {e.details()}",
            }
    return templates.TemplateResponse("users.html", {"request": request, "result": result})


@app.get("/books", response_class=HTMLResponse)
def books_page(request: Request, book_id: str | None = None):
    result = None
    for b in bookIDs:
        res = grpc_clients.catalog_stub().GetBook(catalog_pb2.GetBookRequest(book_id=b))
        result = {
            "ok": res.ok,
            "message": res.message,
            "book_id": res.book_id,
            "title": res.title,
            "author": res.author,
        }
        print(res.title + " " + res.author + " " + res.book_id)
    return templates.TemplateResponse("books.html", {"request": request, "result": result})


@app.post("/users/register", response_class=HTMLResponse)
def register_user(request: Request, name: str = Form(...), email: str = Form(...)):
    global logNum
    res = grpc_clients.users_stub().RegisterUser(
        users_pb2.RegisterUserRequest(name=name, email=email),
        timeout=3,
    )
    result = {"ok": res.ok, "message": res.message, "user_id": res.user_id}
    print("your user id: " + res.user_id)
    comp = res.user_id + " registered as new user"
    grpc_clients.audit_stub().LogEvent(audit_pb2.LogRequest(event_type="1", description=comp))
    logNum += 1
    return templates.TemplateResponse("users.html", {"request": request, "result": result})


@app.post("/books/publish", response_class=HTMLResponse)
def publish_book(request: Request, title: str = Form(...), author: str = Form(...)):
    global logNum
    res = grpc_clients.catalog_stub().PublishBook(catalog_pb2.PublishBookRequest(title=title, author=author))
    result = {"ok": res.ok, "message": res.message, "book_id": res.book_id}
    grpc_clients.inventory_stub().AddCopies(
        inventory_pb2.AddCopiesRequest(book_id=res.book_id, count=1), timeout=3
    )
    comp = res.book_id + " published"
    grpc_clients.audit_stub().LogEvent(audit_pb2.LogRequest(event_type="1", description=comp))
    bookIDs.append(res.book_id)
    logNum += 1
    return templates.TemplateResponse("books.html", {"request": request, "result": result})


@app.get("/inventory", response_class=HTMLResponse)
def inventory_page(request: Request, book_id: str | None = None):
    result = None
    if book_id:
        try:
            res = grpc_clients.inventory_stub().GetAvailability(inventory_pb2.BookRequest(book_id=book_id))
            result = {
                "ok": res.ok,
                "message": res.message,
                "book_id": res.book_id,
                "available": res.available,
            }
            print(res.book_id + " " + str(res.available))
        except grpc.RpcError:
            result = {"ok": False, "message": "gRPC error calling Inventory"}
    return templates.TemplateResponse("inventory.html", {"request": request, "result": result})


@app.post("/inventory/add", response_class=HTMLResponse)
def inventory_add(request: Request, book_id: str = Form(...), count: int = Form(...)):
    global logNum
    res = grpc_clients.inventory_stub().AddCopies(
        inventory_pb2.AddCopiesRequest(book_id=book_id, count=count),
        timeout=3,
    )
    result = {
        "ok": res.ok,
        "message": res.message,
        "book_id": res.book_id,
        "available": res.available,
    }
    comp = str(count) + " new copies of " + book_id + " added"
    grpc_clients.audit_stub().LogEvent(audit_pb2.LogRequest(event_type="1", description=comp))
    logNum += 1
    return templates.TemplateResponse("inventory.html", {"request": request, "result": result})


@app.get("/circulation", response_class=HTMLResponse)
def circulation_page(request: Request, book_id: str | None = None, user_id: str | None = None):
    global logNum
    result = None

    if book_id and user_id:
        tx_id = str(uuid.uuid4())
        prepare_request = twopc_pb2.PrepareRequest(
            transaction_id=tx_id,
            user_id=user_id,
            book_id=book_id,
            coordinator_node_id="1",
        )

        try:
            votes = []
            log_voting_send("1", "PrepareCheckout", "2")
            votes.append(grpc_clients.users_twopc_stub().PrepareCheckout(prepare_request))

            log_voting_send("1", "PrepareCheckout", "3")
            votes.append(grpc_clients.catalog_twopc_stub().PrepareCheckout(prepare_request))

            log_voting_send("1", "PrepareCheckout", "4")
            votes.append(grpc_clients.inventory_twopc_stub().PrepareCheckout(prepare_request))

            log_voting_send("1", "PrepareCheckout", "5")
            votes.append(grpc_clients.circulation_twopc_stub().PrepareCheckout(prepare_request))

            log_decision_send("1", "TriggerDecision", "1")
            decision_result = grpc_clients.local_twopc_decision_stub().TriggerDecision(
                twopc_pb2.VoteCollection(
                    transaction_id=tx_id,
                    user_id=user_id,
                    book_id=book_id,
                    votes=votes,
                )
            )

            print(f"[twopc-node1-gateway] Transaction {tx_id} collected {len(votes)} votes", flush=True)

            print(f"[twopc-node1-gateway] Final decision: "
                f"{'GLOBAL_COMMIT' if decision_result.decision == twopc_pb2.GLOBAL_COMMIT else 'GLOBAL_ABORT'}",
                flush=True)

            decision_request = twopc_pb2.DecisionRequest(
                transaction_id=tx_id,
                user_id=user_id,
                book_id=book_id,
                coordinator_node_id="1",
                decision=decision_result.decision,
            )

            log_decision_send("1", "FinalizeCheckout", "2")
            grpc_clients.users_twopc_stub().FinalizeCheckout(decision_request)

            log_decision_send("1", "FinalizeCheckout", "3")
            grpc_clients.catalog_twopc_stub().FinalizeCheckout(decision_request)

            log_decision_send("1", "FinalizeCheckout", "4")
            inventory_decision = grpc_clients.inventory_twopc_stub().FinalizeCheckout(decision_request)

            log_decision_send("1", "FinalizeCheckout", "5")
            circulation_decision = grpc_clients.circulation_twopc_stub().FinalizeCheckout(decision_request)

            vote_summary = [
                f"Node {v.node_id}: {'COMMIT' if v.vote == twopc_pb2.VOTE_COMMIT else 'ABORT'} ({v.reason})"
                for v in votes
            ]

            if decision_result.decision == twopc_pb2.GLOBAL_COMMIT:
                due_date = ""
                if circulation_decision.success:
                    due_date = str(__import__('datetime').date.today() + __import__('datetime').timedelta(days=5))
                comp = book_id + " checked out by " + user_id + ", due on " + due_date
                grpc_clients.audit_stub().LogEvent(audit_pb2.LogRequest(event_type="1", description=comp))
                logNum += 1
                result = {
                    "ok": True,
                    "message": "Book checkout committed successfully",
                    "due_date": due_date,
                    "transaction_id": tx_id,
                    "votes": vote_summary,
                    "decision": "GLOBAL_COMMIT",
                    "inventory_message": inventory_decision.message,
                    "circulation_message": circulation_decision.message,
                }
            else:
                abort_reason = next((v.reason for v in votes if v.vote == twopc_pb2.VOTE_ABORT), "Checkout aborted")
                result = {
                    "ok": False,
                    "message": f"Book checkout aborted: {abort_reason}",
                    "due_date": "",
                    "transaction_id": tx_id,
                    "votes": vote_summary,
                    "decision": "GLOBAL_ABORT",
                }
        except grpc.RpcError as e:
            print(
                f"[twopc-node1-gateway] Transaction {tx_id} aborted because participant RPC failed: "
                f"{e.code()} - {e.details()}",
                flush=True,
            )

            print(
              f"[twopc-node1-gateway] Final decision: GLOBAL_ABORT",
              flush=True,
            )

            result = {
                "ok": False,
                "message": f"Checkout aborted: participant unavailable ({e.code().name})",
                "transaction_id": tx_id,
                "decision": "GLOBAL_ABORT",
          }

    return templates.TemplateResponse("circulation.html", {"request": request, "result": result})


@app.post("/circulation/checkin", response_class=HTMLResponse)
def circulation_checkout(request: Request, book_id: str = Form(...), user_id: str = Form(...)):
    global logNum
    res = grpc_clients.circulation_stub().CheckinBook(circulation_pb2.CheckinRequest(book_id=book_id, user_id=user_id))
    grpc_clients.inventory_stub().IncrementCopy(inventory_pb2.BookRequest(book_id=book_id), timeout=3)
    result = {"ok": res.ok, "message": res.message}
    comp = book_id + " checked in by " + user_id
    print(comp)
    grpc_clients.audit_stub().LogEvent(audit_pb2.LogRequest(event_type="1", description=comp))
    logNum += 1
    return templates.TemplateResponse("circulation.html", {"request": request, "result": result})


@app.get("/audit", response_class=HTMLResponse)
def log_page(request: Request, book_id: str | None = None):
    global logNum
    result = None
    for b in range(logNum):
        res = grpc_clients.audit_stub().LogEvent(audit_pb2.LogRequest(event_type="2", description=str(b)))
        result = {"ok": res.ok, "message": res.message}
        print(res.message)
    return templates.TemplateResponse("audit.html", {"request": request, "result": result})


@app.get("/health")
def health():
    return {"ok": True}
