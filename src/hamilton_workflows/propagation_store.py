"""SQL Server 2008 R2 persistence for Champions propagation plans.

Callers own the connection transaction. No legacy mutating procedure is invoked.
"""

from datetime import datetime

from . import database
from .propagation import PARAMETER_NAMES, deserialize_plan, serialize_plan, validate_parameters, well_key


def query(cursor, sql, params=()):
    cursor.execute(sql, tuple(params))
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def single(rows, label):
    if len(rows) != 1:
        raise ValueError(f"Expected exactly one {label}; found {len(rows)}.")
    return rows[0]


def load_context(cursor, barcode, run_id):
    experiment = single(query(cursor, """
        SELECT e.ExperimentID AS experiment_id, a.PlateID AS root_plate_id
        FROM dbo.Experiments e JOIN dbo.AncestPlatesInExperiments a ON a.ExperimentID=e.ExperimentID
        WHERE e.ScheduledToRun=1
    """), "scheduled experiment ancestor")
    source = single(query(cursor, """
        SELECT PlateID AS source_plate_id, BarCode AS source_barcode
        FROM dbo.Plates WHERE BarCode=? AND Discarded=0
    """, (barcode,)), "active source barcode")
    chain = {r["plate_id"] for r in query(cursor,
        "SELECT DescPlateID AS plate_id FROM dbo.Descendants(?)", (experiment["root_plate_id"],))}
    chain.add(experiment["root_plate_id"])
    if source["source_plate_id"] not in chain:
        raise ValueError("Source plate does not belong to the scheduled experiment.")
    run = single(query(cursor,
        "SELECT StartTime AS started FROM HamiltonVectorDB.dbo.HxRun WHERE RunGUID=?", (run_id,)), "run")
    if run["started"] is None:
        raise ValueError("Run has no start timestamp.")
    return {**experiment, **source, "run_id": str(run_id), "run_timestamp": run["started"].isoformat()}


def acquire_locks(cursor, context):
    # Session lock survives the biological commit until output publication/close.
    # All sources in a run share the legacy filenames, so serialize their exports.
    for resource, owner in ((f"ChampionsExport:{context['run_id']}", "Session"),
                            (f"ChampionsExperiment:{context['experiment_id']}", "Transaction")):
        cursor.execute("""
            DECLARE @result int;
            EXEC @result=sys.sp_getapplock @Resource=?, @LockMode='Exclusive',
                 @LockOwner=?, @LockTimeout=15000;
            SELECT @result;
        """, (resource, owner))
        result = cursor.fetchone()[0]
        if result is None or result < 0:
            raise RuntimeError(f"Could not acquire propagation {owner.lower()} lock ({result}).")


def load_parameters(cursor, experiment_id):
    rows = query(cursor, """
        SELECT ParameterName AS name, ParamValueTxt AS value
        FROM dbo.ExperimentParameters WHERE ExperimentID=?
    """, (experiment_id,))
    parameters = {}
    for name in PARAMETER_NAMES:
        parameters[name] = single([r for r in rows if r["name"] == name], f"parameter {name}")["value"]
    return validate_parameters(parameters)


def load_readings(cursor, context, sources):
    channels = {}
    for channel in ("FlEx482Em510", "FlEx587Em611"):
        # Identifiers are fixed above, never supplied by a caller.
        rows = query(cursor, f"SELECT WellID AS well, {channel} AS reading FROM dbo.Import{channel} WHERE PlateID=? AND RunID=?",
                     (context["source_plate_id"], context["run_id"]))
        channels[channel] = {s["well"]: single([r for r in rows if r["well"] == s["well"]],
                                               f"{channel} reading for {s['well']}")["reading"] for s in sources}
    return [{**s, "raw_gfp": channels["FlEx482Em510"][s["well"]],
             "raw_rfp": channels["FlEx587Em611"][s["well"]]} for s in sources]


def load_snapshot(cursor, context):
    parameters = load_parameters(cursor, context["experiment_id"])
    latest = query(cursor, """
        SELECT c.CultureID AS culture_id,c.WellID AS well,h.Iteration AS iteration,
               h.Status_mature AS mature,h.TimeStamp AS timestamp
        FROM dbo.Cultures c
        LEFT JOIN dbo.CulturesHistory h ON h.CultureID=c.CultureID AND h.Iteration=
          (SELECT MAX(h2.Iteration) FROM dbo.CulturesHistory h2 WHERE h2.CultureID=c.CultureID)
        WHERE c.PlateID=? AND c.WellID IS NOT NULL
    """, (context["source_plate_id"],))
    if len({r["culture_id"] for r in latest}) != len(latest) or len({r["well"] for r in latest}) != len(latest):
        raise ValueError("Ambiguous source culture/latest history records.")
    sources = []
    started = datetime.fromisoformat(context["run_timestamp"])
    for row in latest:
        well_key(row["well"])
        if row["iteration"] is None or row["timestamp"] is None or row["mature"] is None:
            raise ValueError(f"Missing source history for {row['well']}.")
        if row["timestamp"] >= started:
            raise ValueError("Source already processed or changed at/after this run; automatic replay is disabled. Review the existing records and VENUS execution before recovery.")
        if not row["mature"]:
            sources.append({"culture_id": row["culture_id"], "well": row["well"],
                            "iteration": row["iteration"], "timestamp": row["timestamp"].isoformat()})
    sources = load_readings(cursor, context, sorted(sources, key=lambda s: (*well_key(s["well"]), s["culture_id"])))
    destinations = query(cursor, """
        SELECT u.PlateID AS plate_id,u.WellID AS well,p.BarCode AS barcode
        FROM dbo.UnoccupiedWellsInPlatesChain(?) u JOIN dbo.Plates p ON p.PlateID=u.PlateID
        WHERE p.Discarded=0 AND u.PlateID<>? AND u.WellID IS NOT NULL
    """, (context["root_plate_id"], context["source_plate_id"]))
    positions = {}
    for plate_id in sorted({d["plate_id"] for d in destinations}):
        positions[plate_id] = [r["position"] for r in query(cursor,
            "SELECT CytomatPos AS position FROM dbo.Cytomat WHERE PlateID=? ORDER BY CytomatPos", (plate_id,))]
    for destination in destinations:
        destination["cytomat_positions"] = positions[destination["plate_id"]]
    destinations.sort(key=lambda d: (d["plate_id"], *well_key(d["well"])))
    return {"context": context, "parameters": parameters, "sources": sources, "destinations": destinations}


HISTORY_SQL = """
    INSERT INTO dbo.CulturesHistory
    (CultureID,Iteration,OD,DilutionFactor,Status_mature,Status_frozen,TimeStamp,
     FlEx482Em510,FlEx587Em611,OD_FlEx482Em510,OD_FlEx587Em611)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
"""


def persist_plan(cursor, plan):
    # Reconstruct the derived rows before the first insert, so callers cannot
    # accidentally persist an edited transfer list with an unchanged snapshot.
    plan = deserialize_plan(serialize_plan(plan))
    snapshot = plan["snapshot"]
    context = snapshot["context"]
    if load_context(cursor, context["source_barcode"], context["run_id"]) != context or load_snapshot(cursor, context) != snapshot:
        raise ValueError("Propagation inputs or destination occupancy changed before persistence.")
    # Serializable isolation protects these checks from legacy writers that do
    # not participate in the application lock protocol.
    for transfer in plan["transfers"]:
        count = database.fetch_one_value(cursor, """
            SELECT COUNT(*) FROM dbo.Cultures WITH (UPDLOCK,HOLDLOCK)
            WHERE CultureID=? OR (PlateID=? AND WellID=?)
        """, (transfer["child_culture_id"], transfer["destination_plate_id"], transfer["destination_well"]))
        if count:
            raise ValueError("Destination well or generated culture ID is already occupied.")
    timestamp = datetime.fromisoformat(context["run_timestamp"])
    cursor.executemany(HISTORY_SQL, [
        (p["culture_id"],p["next_iteration"],p["od"],p["dilution_factor"],int(p["mature"]),0,timestamp,
         p["gfp"],p["rfp"],p["od_gfp"],p["od_rfp"]) for p in plan["parent_history"]])
    cursor.executemany("INSERT INTO dbo.Cultures (CultureID,PlateID,WellID) VALUES (?,?,?)", [
        (t["child_culture_id"],t["destination_plate_id"],t["destination_well"]) for t in plan["transfers"]])
    cursor.executemany(HISTORY_SQL, [
        (t["child_culture_id"],1,t["od"],1,0,0,timestamp,t["gfp"],t["rfp"],t["od_gfp"],t["od_rfp"])
        for t in plan["transfers"]])
    cursor.executemany("INSERT INTO dbo.Propagation (ParentCultureID,ChldCultureID) VALUES (?,?)", [
        (t["parent_culture_id"],t["child_culture_id"]) for t in plan["transfers"]])
