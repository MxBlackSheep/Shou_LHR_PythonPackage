import tkinter as tk
from tkinter import ttk, messagebox
import pyodbc
from collections import defaultdict
import time

# -------------------- CONFIG --------------------
DRIVER = "{ODBC Driver 11 for SQL Server}"   # change to 17/18 if needed
SERVER = r"LOCALHOST\HAMILTON"
DATABASE = "Labwares"
UID = "Hamilton"
PWD = "mkdpw:V43"
TRUSTED = "no"  # "yes" for Windows auth
TRUST_SERVER_CERT = "yes"

AUTO_REFRESH_MS = 15_000  # 15 seconds (still refreshes if no pending changes)
GRID_COLS = 12            # 96 tips = 12 x 8
GRID_ROWS = 8
DOT_R = 4                 # smaller dots to reduce height
CELL_W = 14               # tighter spacing
CELL_H = 14
PADDING = 10

# Left & right column rack order (top -> bottom).
# Keep 0009 (left) and 0010 (right) at the bottom.
LEFT_TOP_TO_BOTTOM  = ["VER_HT_0005", "VER_HT_0001", "VER_HT_0002", "VER_HT_0006", "VER_HT_0009"]
RIGHT_TOP_TO_BOTTOM = ["VER_HT_0003", "VER_HT_0004", "VER_HT_0007", "VER_HT_0008", "VER_HT_0010"]

COLA_SET = set(LEFT_TOP_TO_BOTTOM)
COLB_SET = set(RIGHT_TOP_TO_BOTTOM)

STATUS_ORDER = ["clean", "empty", "dirty", "rinsed", "washed"]
STATUS_COLOR = {
    "clean":  "#22c55e",  # green
    "empty":  "#d1d5db",  # gray
    "dirty":  "#ef4444",  # red
    "rinsed": "#3b82f6",  # blue
    "washed": "#a855f7",  # purple
}
UNKNOWN_COLOR = "#9ca3af"

# -------------------- DB --------------------
def connect():
    conn_str = (
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD};"
        f"Trusted_Connection={TRUSTED};TrustServerCertificate={TRUST_SERVER_CERT};"
    )
    return pyodbc.connect(conn_str, autocommit=True)

def fetch_tip_map():
    """
    Returns:
      dict[labware_id] -> dict[position_id:int] = status:str
    Reads from TipUsage_ColA + TipUsage_ColB.
    """
    q = """
        SELECT labware_id, position_id, status FROM dbo.TipUsage_ColA
        UNION ALL
        SELECT labware_id, position_id, status FROM dbo.TipUsage_ColB
    """
    tipmap = defaultdict(dict)
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(q)
        for labware_id, pos, status in cur.fetchall():
            tipmap[str(labware_id)][int(pos)] = str(status).lower().strip()
    return tipmap

def save_changes(changes):
    """
    changes: dict[(labware_id:str, position:int)] = new_status:str
    Writes to the appropriate table (ColA or ColB). Returns number of rows updated.
    """
    if not changes:
        return 0
    colA_updates = []
    colB_updates = []
    for (lw, pos), new_status in changes.items():
        if lw in COLA_SET:
            colA_updates.append((new_status, lw, pos))
        elif lw in COLB_SET:
            colB_updates.append((new_status, lw, pos))

    updated = 0
    with connect() as conn:
        cur = conn.cursor()
        if colA_updates:
            cur.executemany(
                "UPDATE dbo.TipUsage_ColA SET status=? WHERE labware_id=? AND position_id=?",
                colA_updates,
            )
            updated += cur.rowcount if cur.rowcount != -1 else len(colA_updates)
        if colB_updates:
            cur.executemany(
                "UPDATE dbo.TipUsage_ColB SET status=? WHERE labware_id=? AND position_id=?",
                colB_updates,
            )
            updated += cur.rowcount if cur.rowcount != -1 else len(colB_updates)
    return updated

# -------------------- UI helpers --------------------
def pos_to_row_col(pos):
    """
    Column-major numbering:
    - columns go left -> right
    - within each column, numbers increase top -> bottom
    - GRID_ROWS = 8, GRID_COLS = 12
    Examples:
      pos=96 -> (row=7, col=11)  # bottom-right
      pos=95 -> (row=6, col=11)  # just above 96
    """
    i = pos - 1
    row = i % GRID_ROWS
    col = i // GRID_ROWS
    return row, col

def row_col_to_pos(row, col):
    # inverse of the above (column-major)
    return col * GRID_ROWS + row + 1


# -------------------- APP --------------------
class TipApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Tip Usage Viewer / Editor (ColB & ColA)")
        # Lower, wider window
        self.geometry("900x860")

        # State
        self.tipmap = {}                 # labware -> {pos: status}
        self.pending = {}                # (labware, pos) -> new_status
        self.dot_items = {}              # canvas item id -> (labware, pos)
        self.dot_color_overrides = {}    # (labware,pos) -> color (preview for pending)
        self.selected = None             # (labware, pos)
        self.last_draw_params = None

        # Top toolbar
        toolbar = ttk.Frame(self)
        toolbar.pack(side="top", fill="x", pady=6, padx=8)

        ttk.Button(toolbar, text="Refresh", command=self.refresh).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Save", command=self.on_save).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Discard Changes", command=self.discard_changes).pack(side="left", padx=4)
        self.pending_var = tk.StringVar(value="Pending: 0")
        ttk.Label(toolbar, textvariable=self.pending_var).pack(side="left", padx=16)

        self.last_ref_var = tk.StringVar(value="—")
        ttk.Label(toolbar, textvariable=self.last_ref_var).pack(side="right")

        # Right control panel (edit single dot / rack)
        panel = ttk.Frame(self)
        panel.pack(side="right", fill="y", padx=8, pady=6)

        ttk.Label(panel, text="Selected Tip").pack(anchor="w")
        self.sel_lab_var = tk.StringVar(value="-")
        self.sel_pos_var = tk.StringVar(value="-")
        self.sel_status_var = tk.StringVar(value="-")
        ttk.Label(panel, textvariable=self.sel_lab_var).pack(anchor="w")
        ttk.Label(panel, textvariable=self.sel_pos_var).pack(anchor="w")
        ttk.Label(panel, textvariable=self.sel_status_var).pack(anchor="w")

        ttk.Label(panel, text="Set status").pack(anchor="w", pady=(10,2))
        self.status_choice = tk.StringVar(value=STATUS_ORDER[0])
        ttk.Combobox(panel, textvariable=self.status_choice, values=STATUS_ORDER, width=12, state="readonly").pack(anchor="w")
        ttk.Button(panel, text="Apply to Tip", command=self.apply_to_tip).pack(anchor="w", pady=4)

        ttk.Separator(panel, orient="horizontal").pack(fill="x", pady=8)

        ttk.Label(panel, text="Apply to Rack").pack(anchor="w")
        self.rack_choice = tk.StringVar(value=LEFT_TOP_TO_BOTTOM[0])
        rack_values = LEFT_TOP_TO_BOTTOM + RIGHT_TOP_TO_BOTTOM
        ttk.Combobox(panel, textvariable=self.rack_choice, values=rack_values, width=18, state="readonly").pack(anchor="w")
        self.rack_status_choice = tk.StringVar(value="clean")
        ttk.Combobox(panel, textvariable=self.rack_status_choice, values=STATUS_ORDER, width=12, state="readonly").pack(anchor="w", pady=2)
        ttk.Button(panel, text="Apply to Whole Rack", command=self.apply_to_rack).pack(anchor="w", pady=4)
        ttk.Label(panel, text="(Queued; click Save to write to DB)").pack(anchor="w")

        # Canvas (center/left)
        self.canvas = tk.Canvas(self, bg="#f9fafb")
        self.canvas.pack(side="left", fill="both", expand=True, padx=(8,0), pady=6)
        self.canvas.bind("<Button-1>", self.on_canvas_click)

        # Initial draw & auto refresh
        self.after(200, self.refresh)
        self.after(AUTO_REFRESH_MS, self._tick)

    # --------- draw ----------
    def draw(self):
        self.canvas.delete("all")
        self.dot_items.clear()

        col_gap = 40
        rack_w = GRID_COLS * CELL_W + 2*PADDING
        rack_h = GRID_ROWS * CELL_H + 2*PADDING
        left_x  = 20
        right_x = left_x + rack_w + col_gap
        top_y   = 40
        v_gap   = 18

        # titles
        self.canvas.create_text(left_x, 20, text="ColA", anchor="w", font=("Segoe UI", 11, "bold"))
        self.canvas.create_text(right_x, 20, text="ColB", anchor="w", font=("Segoe UI", 11, "bold"))

        # left column
        y = top_y
        for lw in LEFT_TOP_TO_BOTTOM:
            self.draw_rack(left_x, y, lw, self.tipmap.get(lw, {}))
            y += rack_h + v_gap

        # right column
        y = top_y
        for lw in RIGHT_TOP_TO_BOTTOM:
            self.draw_rack(right_x, y, lw, self.tipmap.get(lw, {}))
            y += rack_h + v_gap

        # legend
        legend_x = right_x + rack_w + 24
        legend_y = top_y
        self.canvas.create_text(legend_x, legend_y-12, text="Legend", font=("Segoe UI", 10, "bold"), anchor="w")
        for k in STATUS_ORDER:
            legend_y += 18
            color = STATUS_COLOR.get(k, UNKNOWN_COLOR)
            self.canvas.create_oval(legend_x, legend_y, legend_x+12, legend_y+12, fill=color, outline="white")
            self.canvas.create_text(legend_x+20, legend_y+6, text=k, anchor="w", font=("Segoe UI", 9))

    def draw_rack(self, x, y, labware_id, tips_for_labware):
        rack_w = GRID_COLS * CELL_W + 2*PADDING
        rack_h = GRID_ROWS * CELL_H + 2*PADDING
        self.canvas.create_rectangle(x, y, x + rack_w, y + rack_h, outline="#93c5fd", width=2)
        self.canvas.create_text(x + rack_w/2, y - 8, text=labware_id, fill="#111827", font=("Segoe UI", 9, "bold"))

        for pos in range(1, GRID_COLS * GRID_ROWS + 1):
            row, col = pos_to_row_col(pos)
            cx = x + PADDING + col * CELL_W + CELL_W/2
            cy = y + PADDING + row * CELL_H + CELL_H/2
            status = self.pending.get((labware_id, pos), tips_for_labware.get(pos, None))
            color = STATUS_COLOR.get(status, UNKNOWN_COLOR)
            oid = self.canvas.create_oval(cx - DOT_R, cy - DOT_R, cx + DOT_R, cy + DOT_R,
                                          fill=color, outline="white", width=1)
            self.dot_items[oid] = (labware_id, pos)

    # --------- interactions ----------
    def on_canvas_click(self, event):
        # find topmost item under cursor
        items = self.canvas.find_overlapping(event.x, event.y, event.x, event.y)
        for oid in reversed(items):
            if oid in self.dot_items:
                lw, pos = self.dot_items[oid]
                self.select_tip(lw, pos)
                return

    def select_tip(self, labware_id, pos):
        self.selected = (labware_id, pos)
        current = self.pending.get((labware_id, pos), self.tipmap.get(labware_id, {}).get(pos, "-"))
        self.sel_lab_var.set(f"Labware: {labware_id}")
        self.sel_pos_var.set(f"Position: {pos}")
        self.sel_status_var.set(f"Current: {current}")

    def apply_to_tip(self):
        if not self.selected:
            messagebox.showinfo("No selection", "Click a tip to select it first.")
            return
        lw, pos = self.selected
        new_status = self.status_choice.get()
        self.pending[(lw, pos)] = new_status
        self.pending_var.set(f"Pending: {len(self.pending)}")
        self.refresh_preview_only()

    def apply_to_rack(self):
        lw = self.rack_choice.get()
        if not lw:
            return
        new_status = self.rack_status_choice.get()
        # queue all 96 positions
        for pos in range(1, GRID_ROWS * GRID_COLS + 1):
            self.pending[(lw, pos)] = new_status
        self.pending_var.set(f"Pending: {len(self.pending)}")
        self.refresh_preview_only()

    def discard_changes(self):
        if not self.pending:
            return
        if not messagebox.askyesno("Discard", "Discard all pending changes?"):
            return
        self.pending.clear()
        self.pending_var.set("Pending: 0")
        self.refresh()  # redraw from DB

    def on_save(self):
        if not self.pending:
            messagebox.showinfo("Nothing to save", "No pending changes.")
            return
        try:
            updated = save_changes(self.pending)
            self.pending.clear()
            self.pending_var.set("Pending: 0")
            messagebox.showinfo("Saved", f"Updated {updated} positions.")
            self.refresh()
        except Exception as e:
            messagebox.showerror("Save failed", str(e))

    # --------- refresh ----------
    def refresh(self):
        # Don’t auto-refresh if user has pending edits (avoid overwriting preview)
        if self.pending:
            self.last_ref_var.set(time.strftime("Pending changes — last DB read: %H:%M:%S"))
            self.draw()
            return
        try:
            self.tipmap = fetch_tip_map()
            self.last_ref_var.set(time.strftime("Last DB read: %H:%M:%S"))
            self.draw()
        except Exception as e:
            self.last_ref_var.set(f"Error: {e}")

    def refresh_preview_only(self):
        # redraw using self.tipmap + self.pending overlay
        self.draw()

    def _tick(self):
        self.refresh()
        self.after(AUTO_REFRESH_MS, self._tick)

if __name__ == "__main__":
    app = TipApp()
    app.mainloop()
