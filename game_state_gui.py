import tkinter as tk
from tkinter import messagebox, simpledialog
import redis
import json
from datetime import datetime

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# --- Color Scheme ---
BG_COLOR = "#232946"
FG_COLOR = "#fffffe"
BTN_BG = "#eebbc3"
BTN_FG = "#232946"
ACCENT = "#b8c1ec"
TITLE_FONT = ("Segoe UI", 18, "bold")
LABEL_FONT = ("Segoe UI", 12, "bold")
BTN_FONT = ("Segoe UI", 11, "bold")

# --- Helper functions ---
def show_info(title, msg):
    # Pretty-print JSON if it's JSON
    try:
        msg = json.dumps(json.loads(msg), indent=2)
    except Exception:
        pass
    top = tk.Toplevel(root)
    top.title(title)
    top.configure(bg=BG_COLOR)
    tk.Label(top, text=title, font=LABEL_FONT, bg=BG_COLOR, fg=BTN_BG).pack(pady=(10,5))
    text = tk.Text(top, width=45, height=12, bg=FG_COLOR, fg=BG_COLOR, font=("Consolas", 10), bd=0)
    text.pack(padx=15, pady=(0,10))
    text.insert("1.0", msg)
    text.config(state=tk.DISABLED)
    tk.Button(top, text="OK", command=top.destroy, bg=BTN_BG, fg=BTN_FG, font=BTN_FONT, bd=0).pack(pady=(0,12))

def add_update_player_state():
    game_id = simpledialog.askstring("Game ID", "Enter Game ID:")
    player_id = simpledialog.askstring("Player ID", "Enter Player ID:")
    x = simpledialog.askinteger("X position", "Enter X position:")
    y = simpledialog.askinteger("Y position", "Enter Y position:")
    z = simpledialog.askinteger("Z position", "Enter Z position:")
    hp = simpledialog.askinteger("Health", "Enter Health (hp):")
    status = simpledialog.askstring("Status", "Status (alive/dead/etc):")
    if None in (game_id, player_id, x, y, z, hp, status):
        return
    key = f"game:{game_id}:player:{player_id}:state"
    data = {
        "x": x, "y": y, "z": z, "hp": hp, "status": status,
        "last_update": datetime.now().isoformat()
    }
    r.set(key, json.dumps(data))
    show_info("Player State Saved", f"{key}:\n{json.dumps(data, indent=2)}")

def get_player_state():
    game_id = simpledialog.askstring("Game ID", "Enter Game ID:")
    player_id = simpledialog.askstring("Player ID", "Enter Player ID:")
    key = f"game:{game_id}:player:{player_id}:state"
    result = r.get(key)
    if result:
        show_info("Player State", f"{key}:\n{result}")
    else:
        show_info("Player State", "No data found.")

def log_event():
    game_id = simpledialog.askstring("Game ID", "Enter Game ID:")
    player_id = simpledialog.askstring("Player ID", "Enter Player ID:")
    event_type = simpledialog.askstring("Event type", "Type (pickup/kill/etc):")
    item_id = simpledialog.askstring("Item ID", "Item ID (or leave blank):")
    x = simpledialog.askinteger("X position", "Event X position:")
    y = simpledialog.askinteger("Y position", "Event Y position:")
    if None in (game_id, player_id, event_type, x, y):
        return
    event = {
        "timestamp": datetime.now().isoformat(),
        "type": event_type,
        "player_id": player_id,
        "item_id": item_id if item_id else None,
        "x": x, "y": y
    }
    key = f"game:{game_id}:events"
    r.lpush(key, json.dumps(event))
    show_info("Event Logged", f"{key}:\n{json.dumps(event, indent=2)}")

def show_recent_events():
    game_id = simpledialog.askstring("Game ID", "Enter Game ID:")
    n = simpledialog.askinteger("How many?", "How many recent events to show?")
    if None in (game_id, n):
        return
    key = f"game:{game_id}:events"
    events = r.lrange(key, 0, n-1)
    if not events:
        show_info("Events", "No events found.")
        return
    msg = ""
    for idx, e in enumerate(events):
        msg += f"{idx+1}:\n{json.dumps(json.loads(e), indent=2)}\n"
    show_info(f"Last {n} Events", msg)

def set_world_state():
    game_id = simpledialog.askstring("Game ID", "Enter Game ID:")
    zone_radius = simpledialog.askinteger("Zone Radius", "Zone radius:")
    supply_id = simpledialog.askstring("Supply Drop ID", "Supply drop ID:")
    x = simpledialog.askinteger("Supply X", "Supply X:")
    y = simpledialog.askinteger("Supply Y", "Supply Y:")
    if None in (game_id, zone_radius, supply_id, x, y):
        return
    key = f"game:{game_id}:world:state"
    state = {
        "zone_radius": zone_radius,
        "supply_drops": [{"id": supply_id, "x": x, "y": y}],
        "last_update": datetime.now().isoformat()
    }
    r.set(key, json.dumps(state))
    show_info("World State Saved", f"{key}:\n{json.dumps(state, indent=2)}")

def get_world_state():
    game_id = simpledialog.askstring("Game ID", "Enter Game ID:")
    key = f"game:{game_id}:world:state"
    result = r.get(key)
    if result:
        show_info("World State", f"{key}:\n{result}")
    else:
        show_info("World State", "No data found.")

# --- Main GUI Window ---
root = tk.Tk()
root.title("Game State Redis GUI")
root.geometry("400x500")
root.configure(bg=BG_COLOR)

tk.Label(
    root, text="Online Game State Control", font=TITLE_FONT, bg=BG_COLOR, fg=ACCENT
).pack(pady=(28,16))

button_frame = tk.Frame(root, bg=BG_COLOR)
button_frame.pack(pady=12)

btns = [
    ("Add/Update Player State", add_update_player_state),
    ("Get Player State", get_player_state),
    ("Log Game Event", log_event),
    ("Show Recent Game Events", show_recent_events),
    ("Set World State", set_world_state),
    ("Get World State", get_world_state),
    ("Quit", root.quit)
]

for text, cmd in btns:
    b = tk.Button(
        button_frame, text=text, command=cmd,
        font=BTN_FONT, width=28, pady=8,
        bg=BTN_BG, fg=BTN_FG, activebackground=ACCENT, activeforeground=BG_COLOR,
        bd=0, highlightthickness=0, cursor="hand2"
    )
    b.pack(pady=7)

root.mainloop()
