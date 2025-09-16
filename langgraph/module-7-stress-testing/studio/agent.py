import operator
from typing import Annotated

from langgraph.graph import StateGraph, START, END
from langgraph.types import Send
from typing_extensions import TypedDict


# ---- 1. State Definition ----
# We add 'tasks_to_spawn' to hold the number of tasks we want to run.
class CounterState(TypedDict):
    """Defines the state for our graph."""
    counter: Annotated[int, operator.add]
    tasks_to_spawn: int
    message: str


# ---- 2. Node Implementations ----

def prepare_tasks(state: CounterState):
    """
    This node prepares the data for the parallel tasks.
    It simply returns a dictionary to update the state.
    """
    print("Node: prepare_tasks")
    return {"tasks_to_spawn": 10000}


def increment_node(state: CounterState):
    """
    Each parallel task runs this node and contributes +1 to the counter.
    It returns a dictionary, which the reducer function handles.
    """
    return {"counter": 1}


def collector_node(state: CounterState):
    """
    This node acts as a barrier, ensuring all parallel 'increment'
    tasks have completed before the graph proceeds.
    """
    print("Node: collector (all increments complete)")
    return {}


def result_node(state: CounterState):
    """Checks if the reducer worked correctly and sets the final message."""
    expected = 10000
    got = state.get("counter", 0)
    print(f"Node: result (Expected={expected}, Got={got})")
    if got == expected:
        msg = f"✅ Success! Counter reached {got} as expected."
    else:
        msg = f"❌ Failure! Expected {expected}, but got {got}."
    return {"message": msg}


# ---- 3. Router Function ----
def spawn_router(state: CounterState) -> list[Send]:
    """
    This is NOT a node. It's a plain function that directs the graph.
    It reads the number of tasks from the state and returns a list of `Send`
    commands, which tells the conditional edge to trigger parallel execution.
    """
    print("Routing... Spawning tasks.")
    n = state["tasks_to_spawn"]
    return [Send("increment", {}) for _ in range(n)]


# ---- 4. Graph Construction ----
graph_builder = StateGraph(CounterState)

# Add all the nodes
graph_builder.add_node("prepare_tasks", prepare_tasks)
graph_builder.add_node("increment", increment_node)
graph_builder.add_node("collector", collector_node)
graph_builder.add_node("result", result_node)

# Set the entrypoint
graph_builder.add_edge(START, "prepare_tasks")

# Define the conditional edge for parallel execution
# It starts from 'prepare_tasks', uses the 'spawn_router' function to generate
# the list of tasks, and sends them to the 'increment' node.
graph_builder.add_conditional_edges("prepare_tasks", spawn_router, ["increment"])

# After each 'increment' task is done, it goes to the 'collector'.
# The graph will wait here until ALL parallel branches are finished.
graph_builder.add_edge("increment", "collector")

# Once all tasks are collected, proceed to the final result check.
graph_builder.add_edge("collector", "result")
graph_builder.add_edge("result", END)

# Compile the final graph
graph = graph_builder.compile()


# --- Verification ---
if __name__ == "__main__":
    # The 'counter' starts at 0 by default for the `operator.add` reducer.
    # We don't need to specify 'tasks_to_spawn' here, as the first node does that.
    initial_state = {}

    final_state = graph.invoke(initial_state)

    print("\n" + "="*30)
    print("      FINAL EXECUTION RESULT")
    print("="*30)
    print(final_state["message"])
    print(f"Final counter value: {final_state['counter']}")