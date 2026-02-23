#!/usr/bin/env python3
"""
Auto-reschedule a scheduler job based on OD growth prediction.

Workflow
1) Input: PlateBarcode
   Find PlateID in Plates, then all CultureID in Cultures for that PlateID.
   Pull OD readings from ChampionsCulturesHistory for those CultureID.
   OD used here = OD_FlEx482Em510 + OD_FlEx587Em611.

2) Compute growth rate per culture using linear regression on ln(OD) vs time.
   Predict the time each culture reaches OD=0.3.
   Use the earliest predicted time across cultures.

3) If earliest predicted time is within the next 2 hours:
   Query Experiments table to find ExperimentID where ScheduledToRun = 1.
   Call scheduling backend at http://127.0.0.1:8005 using admin / ShouGroupAdmin.
   List schedules, find the schedule that:
     - experiment_name matches "Champions_FL_Python" (case insensitive)
     - prerequisites contain that ExperimentID
   Update schedule start_time to the predicted time rounded up to 10-minute units.
   After successful reschedule, send an email (via scheduling SMTP API) to the
   schedule's configured notification contacts.

Verification
- When updating the schedule, enforce start_time > current time
  If predicted time is not in the future, set it to now rounded up to next 10-minute boundary.

Style requirements respected
- Never raise exceptions to caller: all errors are logged and exitcode is 1
- Logging format and connection string match your style
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pyodbc
import requests


# === Logging Setup ===
log_dir = r"C:\Python Log"
os.makedirs(log_dir, exist_ok=True)
script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")


# === Constants ===
TARGET_OD = 0.3
PREDICT_WITHIN_HOURS = 2.0
ROUND_MINUTES = 10

# Only consider the three most recent readings per culture
RECENT_POINTS = 3

DEFAULT_API_BASE_URL = "http://127.0.0.1:8005"
DEFAULT_API_USERNAME = "admin"
DEFAULT_API_PASSWORD = "ShouGroupAdmin"

TARGET_SCHEDULE_NAME = "Champions_Fl_Python"


# === Database Connection ===
def establish_connection():
    try:
        conn_str = (
            "DRIVER={ODBC Driver 11 for SQL Server};"
            "SERVER=LOCALHOST\\HAMILTON;"
            "DATABASE=EvoYeast;"
            "UID=Hamilton;"
            "PWD=mkdpw:V43;"
            "Trust_Connection=no;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        log(f"ERROR: DB connection failed: {e}")
        sys.exit(1)


# === Helpers ===
def ceil_dt_to_minutes(dt: datetime, minutes: int) -> datetime:
    if minutes <= 0:
        return dt
    epoch = int(dt.timestamp())
    step = minutes * 60
    ceiled = ((epoch + step - 1) // step) * step
    return datetime.fromtimestamp(ceiled)


def safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def linear_regression(x: List[float], y: List[float]) -> Optional[Tuple[float, float]]:
    n = len(x)
    if n < 2 or n != len(y):
        return None
    sx = sum(x)
    sy = sum(y)
    sxx = sum(v * v for v in x)
    sxy = sum(a * b for a, b in zip(x, y))
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return None
    slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    return slope, intercept


def parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _extract_error(payload: Any) -> str:
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if message:
                return str(message)
        detail = payload.get("detail")
        if detail:
            return str(detail)
        message = payload.get("message")
        if message:
            return str(message)
    return "Unknown API error"


def prerequisites_contain_experiment_id(prereq: Any, experiment_id: str) -> bool:
    exp = str(experiment_id)

    if prereq is None:
        return False

    if isinstance(prereq, list):
        for item in prereq:
            if item is None:
                continue
            if isinstance(item, str):
                if exp == item.strip():
                    return True
                if exp in item:
                    return True
            elif isinstance(item, dict):
                for key in ("experimentID", "ExperimentID", "experiment_id", "experimentId", "id"):
                    if key in item and str(item.get(key)) == exp:
                        return True
                try:
                    if exp in json.dumps(item, default=str):
                        return True
                except Exception:
                    pass
            else:
                if exp == str(item):
                    return True
        return False

    if isinstance(prereq, dict):
        return prerequisites_contain_experiment_id([prereq], exp)

    if isinstance(prereq, str):
        return exp in prereq

    return exp == str(prereq)


def format_dt_for_text(value: Optional[datetime]) -> str:
    if not value:
        return "N/A"
    try:
        return value.isoformat(sep=" ", timespec="seconds")
    except TypeError:
        return value.isoformat()


def summarize_latest_od_snapshot(
    rows: List[Tuple[int, datetime, float]]
) -> Optional[Tuple[datetime, float, int]]:
    """
    Return (latest_timestamp_overall, avg_latest_od_per_culture, culture_count_used).

    Average OD is computed from each culture's latest usable reading so a single
    lagging or extra reading in one culture does not dominate the summary.
    """
    latest_by_culture: Dict[int, Tuple[datetime, float]] = {}
    latest_timestamp: Optional[datetime] = None

    for cid, ts, od in rows:
        prev = latest_by_culture.get(cid)
        if prev is None or ts > prev[0]:
            latest_by_culture[cid] = (ts, od)
        if latest_timestamp is None or ts > latest_timestamp:
            latest_timestamp = ts

    if not latest_by_culture or latest_timestamp is None:
        return None

    avg_latest_od = sum(item[1] for item in latest_by_culture.values()) / float(len(latest_by_culture))
    return latest_timestamp, float(avg_latest_od), len(latest_by_culture)


# === Scheduling API Client ===
class SchedulingApiClient:
    def __init__(self, base_url: str, timeout_seconds: float = 20.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.token: Optional[str] = None

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        require_auth: bool = True,
    ) -> Dict[str, Any]:
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if require_auth:
            if not self.token:
                raise RuntimeError("Missing bearer token. Login first.")
            headers["Authorization"] = f"Bearer {self.token}"

        url = f"{self.base_url}{path}"
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=payload,
            timeout=self.timeout_seconds,
        )

        try:
            body = response.json()
        except ValueError:
            body = {"detail": response.text}

        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code}: {_extract_error(body)}")

        if isinstance(body, dict) and body.get("success") is False:
            raise RuntimeError(_extract_error(body))

        if not isinstance(body, dict):
            raise RuntimeError("Unexpected non-JSON API response")
        return body

    def login(self, username: str, password: str) -> None:
        result = self._request(
            "POST",
            "/api/auth/login",
            payload={"username": username, "password": password},
            require_auth=False,
        )
        data = result.get("data") or {}
        token = data.get("access_token")
        if not token:
            raise RuntimeError("Login succeeded but no access_token was returned")
        self.token = str(token)

    def list_schedules(self) -> List[Dict[str, Any]]:
        result = self._request("GET", "/api/scheduling/list", params={"active_only": False, "archived_only": False})
        data = result.get("data")
        if not isinstance(data, list):
            return []
        return [x for x in data if isinstance(x, dict)]

    def get_schedule(self, schedule_id: str) -> Dict[str, Any]:
        result = self._request("GET", f"/api/scheduling/{schedule_id}")
        data = result.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Schedule API returned unexpected payload")
        return data

    def update_schedule(self, schedule_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self._request("PUT", f"/api/scheduling/{schedule_id}", payload=payload)
        data = result.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Update API returned unexpected payload")
        return data

    def send_schedule_notification_email(self, schedule_id: str, subject: str, body: str) -> Dict[str, Any]:
        result = self._request(
            "POST",
            "/api/scheduling/notifications/send",
            payload={
                "schedule_id": schedule_id,
                "subject": subject,
                "body": body,
            },
        )
        data = result.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Notification email API returned unexpected payload")
        return data


# === DB Queries ===
def get_plate_id_by_barcode(plate_barcode: str) -> int:
    try:
        conn = establish_connection()
        cur = conn.cursor()
        cur.execute("SELECT PlateID FROM Plates WHERE BarCode = ?", (plate_barcode,))
        row = cur.fetchone()
        conn.close()
        if not row:
            log(f"ERROR: No PlateID found for barcode {plate_barcode}")
            sys.exit(1)
        return int(row[0])
    except Exception as e:
        log(f"ERROR: Failed to query PlateID by barcode: {e}")
        sys.exit(1)


def get_culture_ids_for_plate(plate_id: int) -> List[int]:
    try:
        conn = establish_connection()
        cur = conn.cursor()
        cur.execute("SELECT CultureID FROM Cultures WHERE PlateID = ?", (plate_id,))
        rows = cur.fetchall()
        conn.close()
        ids = [int(r[0]) for r in rows if r and r[0] is not None]
        if not ids:
            log(f"ERROR: No cultures found in Cultures for PlateID={plate_id}")
            sys.exit(1)
        return ids
    except Exception as e:
        log(f"ERROR: Failed to query CultureIDs for PlateID={plate_id}: {e}")
        sys.exit(1)


def get_scheduled_experiment_id() -> str:
    try:
        conn = establish_connection()
        cur = conn.cursor()
        cur.execute("SELECT TOP 1 ExperimentID FROM Experiments WHERE ScheduledToRun = 1 ORDER BY ExperimentID DESC")
        row = cur.fetchone()
        conn.close()
        if not row or row[0] is None:
            log("ERROR: No ExperimentID found where ScheduledToRun = 1")
            sys.exit(1)
        return str(row[0])
    except Exception as e:
        log(f"ERROR: Failed to query scheduled ExperimentID: {e}")
        sys.exit(1)


def fetch_od_history_for_cultures(culture_ids: List[int]) -> List[Tuple[int, datetime, float]]:
    """
    Returns rows: (CultureID, TimeStamp, OD_sum)
    OD_sum = OD_FlEx482Em510 + OD_FlEx587Em611

    Uses ALL available readings for those cultures.
    Downselection to most recent points happens in predict_time_to_target().
    """
    try:
        conn = establish_connection()
        cur = conn.cursor()

        placeholders = ",".join("?" for _ in culture_ids)
        sql = f"""
        SELECT
            CultureID,
            [TimeStamp],
            OD_FlEx482Em510,
            OD_FlEx587Em611
        FROM dbo.ChampionsCulturesHistory
        WHERE CultureID IN ({placeholders})
        ORDER BY CultureID, [TimeStamp]
        """
        cur.execute(sql, list(culture_ids))

        rows_out: List[Tuple[int, datetime, float]] = []
        for cid, ts, od1, od2 in cur.fetchall():
            dt = parse_dt(ts)
            if not dt:
                continue
            v1 = safe_float(od1)
            v2 = safe_float(od2)
            if v1 is None or v2 is None:
                continue
            od_sum = float(v1 + v2)
            if od_sum <= 0:
                continue
            rows_out.append((int(cid), dt, od_sum))

        conn.close()
        return rows_out
    except Exception as e:
        log(f"ERROR: Failed to query ChampionsCulturesHistory OD values: {e}")
        sys.exit(1)


# -----------------------------------------
# SELECTION + PREDICTION LOGIC (LIKELY TO CHANGE)
# Current behavior:
# - Fit growth per culture (ln(OD) vs time)
# - Only use the most recent RECENT_POINTS readings per culture
# - Predict time each culture reaches TARGET_OD
# - Choose EARLIEST predicted time across cultures
# -----------------------------------------
def predict_time_to_target(
    rows: List[Tuple[int, datetime, float]],
    target_od: float,
    recent_points: int = RECENT_POINTS,
) -> Optional[Tuple[int, float, float, datetime, datetime, int]]:
    by_culture: Dict[int, List[Tuple[datetime, float]]] = {}
    for cid, ts, od in rows:
        by_culture.setdefault(cid, []).append((ts, od))

    best: Optional[Tuple[int, float, float, datetime, datetime, int]] = None

    for cid, series in by_culture.items():
        series.sort(key=lambda x: x[0])

        if recent_points > 0 and len(series) > recent_points:
            series = series[-recent_points:]

        if len(series) < 2:
            continue

        t0 = series[0][0]
        x: List[float] = []
        y: List[float] = []
        for t, od in series:
            hours = (t - t0).total_seconds() / 3600.0
            x.append(hours)
            y.append(math.log(od))

        lr = linear_regression(x, y)
        if not lr:
            continue

        mu, _ = lr
        if mu <= 0:
            continue

        last_time, last_od = series[-1]
        ln_target = math.log(target_od)
        ln_last = math.log(last_od)
        delta_hours = (ln_target - ln_last) / mu

        if delta_hours <= 0:
            predicted = last_time
        else:
            predicted = last_time + timedelta(hours=delta_hours)

        points_used = len(series)
        cand = (cid, float(mu), float(last_od), last_time, predicted, points_used)
        if best is None or predicted < best[4]:
            best = cand

    return best


# -----------------------------------------
# RESCHEDULE TRIGGER POLICY (LIKELY TO CHANGE)
# Current behavior:
# - Only reschedule if earliest predicted time is within next PREDICT_WITHIN_HOURS
# - Round predicted time up to ROUND_MINUTES
# Verification added:
# - Final schedule time must be greater than current time
# -----------------------------------------
def main() -> None:
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("PlateBarcode", type=str, help="Plate Barcode Identifier")
        parser.add_argument("--target-od", type=float, default=TARGET_OD, help="OD target threshold")
        parser.add_argument("--within-hours", type=float, default=PREDICT_WITHIN_HOURS, help="Reschedule if within N hours")
        parser.add_argument("--round-minutes", type=int, default=ROUND_MINUTES, help="Round predicted time up to N minutes")
        parser.add_argument("--dry-run", action="store_true", help="Compute only, do not call API")
        parser.add_argument("--api-base-url", type=str, default=DEFAULT_API_BASE_URL, help="Scheduling API base URL")
        parser.add_argument("--api-username", type=str, default=DEFAULT_API_USERNAME, help="Scheduling API username")
        parser.add_argument("--api-password", type=str, default=DEFAULT_API_PASSWORD, help="Scheduling API password")
        parser.add_argument("--api-timeout", type=float, default=20.0, help="Scheduling API timeout seconds")
        args = parser.parse_args()

        plate_barcode = args.PlateBarcode
        now = datetime.now()

        log("=== Auto-reschedule by OD prediction started ===")
        log(f"PlateBarcode={plate_barcode}")
        log(f"target_od={args.target_od} within_hours={args.within_hours} round_minutes={args.round_minutes}")
        log(f"recent_points_per_culture={RECENT_POINTS}")
        log(f"dry_run={bool(args.dry_run)} api_base_url={args.api_base_url}")

        plate_id = get_plate_id_by_barcode(plate_barcode)
        log(f"Retrieved PlateID={plate_id}")

        culture_ids = get_culture_ids_for_plate(plate_id)
        log(f"Retrieved {len(culture_ids)} CultureID values for PlateID={plate_id}")

        rows = fetch_od_history_for_cultures(culture_ids)
        log(f"Fetched {len(rows)} OD history rows (all available, before per-culture downselection)")

        if not rows:
            log("ERROR: No usable OD rows found for prediction")
            sys.exit(1)

        best = predict_time_to_target(rows=rows, target_od=float(args.target_od), recent_points=RECENT_POINTS)
        if not best:
            log("ERROR: No valid growth estimates computed (check data availability and OD values)")
            sys.exit(1)

        best_cid, mu, last_od, last_time, predicted_time, points_used = best
        od_summary = summarize_latest_od_snapshot(rows)
        predicted_rounded = ceil_dt_to_minutes(predicted_time, int(args.round_minutes))

        log(f"Earliest predicted culture={best_cid} points_used={points_used} mu_per_hour={mu:.6f} last_od={last_od:.6f} last_time={last_time}")
        log(f"Predicted_time_raw={predicted_time.isoformat()}")
        log(f"Predicted_time_rounded={predicted_rounded.isoformat()}")

        if predicted_rounded <= now:
            new_time = ceil_dt_to_minutes(now, int(args.round_minutes))
            log(f"Predicted time is not in the future, forcing start_time to {new_time.isoformat()}")
            predicted_rounded = new_time

        delta_hours = (predicted_rounded - now).total_seconds() / 3600.0
        log(f"Final_start_time={predicted_rounded.isoformat()}")
        log(f"Time_from_now_hours={delta_hours:.3f}")

        if delta_hours > float(args.within_hours):
            log(f"Prediction is beyond the next {args.within_hours} hours, no reschedule performed")
            sys.exit(0)

        if bool(args.dry_run):
            log("Dry-run enabled, skipping API calls")
            sys.exit(0)

        scheduled_experiment_id = get_scheduled_experiment_id()
        log(f"ExperimentID with ScheduledToRun=1 is {scheduled_experiment_id}")

        client = SchedulingApiClient(base_url=str(args.api_base_url), timeout_seconds=float(args.api_timeout))
        try:
            client.login(str(args.api_username), str(args.api_password))
            log("Scheduling API login succeeded")
        except Exception as e:
            log(f"ERROR: Scheduling API login failed: {e}")
            sys.exit(1)

        try:
            schedules = client.list_schedules()
            log(f"Retrieved {len(schedules)} schedules from API")
        except Exception as e:
            log(f"ERROR: Failed to list schedules: {e}")
            sys.exit(1)

        target_schedule = None
        target_name_norm = TARGET_SCHEDULE_NAME.strip().lower()

        for s in schedules:
            name_norm = str(s.get("experiment_name") or "").strip().lower()
            if name_norm != target_name_norm:
                continue
            if prerequisites_contain_experiment_id(s.get("prerequisites"), scheduled_experiment_id):
                target_schedule = s
                break

        if not target_schedule:
            log(
                f"ERROR: No schedule matched name={TARGET_SCHEDULE_NAME} "
                f"and prerequisites containing ExperimentID={scheduled_experiment_id}"
            )
            sys.exit(1)

        schedule_id = str(target_schedule.get("schedule_id") or "")
        log(f"Matched schedule_id={schedule_id} experiment_name={target_schedule.get('experiment_name')}")

        if not schedule_id:
            log("ERROR: Matched schedule has empty schedule_id")
            sys.exit(1)

        try:
            fresh = client.get_schedule(schedule_id)
        except Exception as e:
            log(f"ERROR: Failed to fetch schedule details: {e}")
            sys.exit(1)

        previous_start_time = parse_dt(fresh.get("start_time"))

        payload: Dict[str, Any] = {"start_time": predicted_rounded.isoformat()}
        token = fresh.get("updated_at")
        if token:
            payload["expected_updated_at"] = token

        log(f"Updating schedule_id={schedule_id} start_time={payload['start_time']}")

        try:
            updated = client.update_schedule(schedule_id, payload)
        except Exception as e:
            log(f"ERROR: Failed to update schedule: {e}")
            sys.exit(1)

        log(f"Schedule updated successfully schedule_id={updated.get('schedule_id')} start_time={updated.get('start_time')}")

        try:
            updated_start_time = parse_dt(updated.get("start_time")) or predicted_rounded
            schedule_name = str(updated.get("experiment_name") or fresh.get("experiment_name") or TARGET_SCHEDULE_NAME)

            subject = f"RobotControl schedule updated: {schedule_name}"
            body_lines = [
                "Schedule reschedule notification (OD prediction automation)",
                "",
                f"Experiment: {schedule_name}",
                f"Schedule ID: {schedule_id}",
                f"Plate barcode: {plate_barcode}",
                f"Scheduled ExperimentID (ScheduledToRun=1): {scheduled_experiment_id}",
                "",
                f"Previous scheduled start time: {format_dt_for_text(previous_start_time)}",
                f"Updated scheduled start time: {format_dt_for_text(updated_start_time)}",
                "",
                f"Prediction target OD: {float(args.target_od):.6f}",
                f"Selected culture ID (earliest predicted): {best_cid}",
                f"Points used for selected culture: {points_used}",
                f"Estimated growth rate mu (/hr): {mu:.6f}",
                f"Last OD used for selected culture: {last_od:.6f}",
                f"Selected culture last timestamp: {format_dt_for_text(last_time)}",
                f"Predicted threshold timestamp (raw): {format_dt_for_text(predicted_time)}",
                f"Predicted threshold timestamp (rounded/final): {format_dt_for_text(predicted_rounded)}",
            ]

            if od_summary:
                summary_ts, avg_latest_od, culture_count_used = od_summary
                body_lines.extend(
                    [
                        "",
                        f"Last OD data point timestamp (all cultures): {format_dt_for_text(summary_ts)}",
                        f"Average OD (latest reading per culture, n={culture_count_used}): {avg_latest_od:.6f}",
                    ]
                )
            else:
                body_lines.extend(
                    [
                        "",
                        "Last OD data point timestamp (all cultures): N/A",
                        "Average OD (latest reading per culture): N/A",
                    ]
                )

            email_result = client.send_schedule_notification_email(
                schedule_id=schedule_id,
                subject=subject,
                body="\n".join(body_lines),
            )

            if email_result.get("sent"):
                recipients = email_result.get("recipients") or []
                log(
                    "Schedule update notification email sent to "
                    f"{len(recipients)} recipient(s): {recipients}"
                )
            elif email_result.get("skipped"):
                log(
                    "Schedule update notification email skipped by API "
                    f"(no active recipients). details={email_result}"
                )
            else:
                log(f"Schedule update notification email API returned non-sent result: {email_result}")
        except Exception as e:
            log(f"WARNING: Schedule updated, but failed to send notification email: {e}")

        log("=== Auto-reschedule by OD prediction completed successfully ===")
        sys.exit(0)

    except SystemExit:
        raise
    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()