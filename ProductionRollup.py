# <summary>
# Module Name : ProductionRollup
# Description : LIVE incremental rollups with anchored baselines (hour/shift/week).
# Author      : Akash J
# Created On  : 2025-06-22
# Scope       : Gateway
# Timer       : Call run_rollups() every 5 seconds from a Gateway Timer script
# </summary>

import system
from collections import namedtuple
from threading import RLock
from java.util import Calendar, TimeZone
from MagnaDataOps.LoggerFunctions import (
    log_info as _log_info,
    log_warn as _log_warn,
    log_error as _log_error,
)

MODULE = "ProductionRollup"

# -------- Tuning --------
_PROV = "[MagnaDataOps]"
# NOTE: Using your provided base (points at DataInsert). If your selects live elsewhere, adjust _NQ_SEL.
_NQ_INS = "MagnaDataOps/Dashboard/AllProductionData/DataInsert/"
_NQ_SEL = _NQ_INS  # keep same unless your selects are under a different folder

SHIFT_CACHE_SEC   = 8         # refresh shift windows (today + yesterday)
STATION_CACHE_SEC = 300       # refresh station list every 5 minutes
WRITE_IDLE_SEC    = 30        # if no increments, still upsert the open hour row every N sec

# -------- Small return types (avoid multi-value returns) --------
ShiftWindow = namedtuple("ShiftWindow", "id date start end")

# -------- Module state (avoid raw globals) --------
class _State(object):
    def __init__(self):
        # sid -> dict with hour/shift/week rolling state
        self.station_state = {}
        # cached metadata
        self.meta = {
            "stations": [],
            "lastStationsLoad": 0,
            "lastShiftLoad": 0,
            "shiftsToday": [],
            "shiftsYday": [],
            "pastShiftsByLine": {},  # {line_id: [ShiftWindow,...]}
        }
        # 'yyyy-MM-dd' of last bootstrap
        self.boot_day = None
        self.lock = RLock()

_S = _State()


# ====================== small time utils ======================
def _floor_hour_utc(d):
    cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.setTime(d)
    cal.set(Calendar.MILLISECOND, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    return cal.getTime()

def _ceil_hour_utc(d):
    cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.setTime(d)
    ms = cal.get(Calendar.MILLISECOND)
    s  = cal.get(Calendar.SECOND)
    m  = cal.get(Calendar.MINUTE)
    if ms != 0 or s != 0 or m != 0:
        cal.set(Calendar.MILLISECOND, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.add(Calendar.HOUR_OF_DAY, 1)
    return cal.getTime()

def _week_start_local_sys(d, dow_1to7):
    cal = Calendar.getInstance()  # system TZ
    cal.setTime(d)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    back = (cal.get(Calendar.DAY_OF_WEEK) - int(dow_1to7) + 7) % 7
    cal.add(Calendar.DAY_OF_MONTH, -back)
    return cal.getTime()

def _u(x):
    try:
        return unicode(x)
    except Exception:
        return unicode(str(x) if x is not None else u"")


# ====================== historian helpers (anchor + delta) ======================
def _hist_value_at_or_before(tag_path, at_dt):
    """
    Return the last numeric value at/before 'at_dt' (using includeBoundingValues).
    If nothing found, return None.
    """
    if not tag_path:
        return None
    try:
        ds = system.tag.queryTagHistory(
            paths=[tag_path],
            startDate=system.date.addHours(at_dt, -48),
            endDate=at_dt,
            returnAggregated=False,
            returnFormat='Wide',
            includeBoundingValues=True
        )
        if ds and ds.getRowCount() > 0:
            try:
                return float(ds.getValueAt(ds.getRowCount() - 1, 1))
            except Exception:
                return None
    except Exception:
        _log_error(MODULE + "::_hist_value_at_or_before")
    return None

def _series_positive_delta(tag_path, start_dt, end_dt):
    """
    Reset-safe delta (sum of increases only). Dips/resets are ignored.
    """
    if not tag_path or start_dt >= end_dt:
        return 0
    try:
        ds = system.tag.queryTagHistory(
            paths=[tag_path],
            startDate=start_dt, endDate=end_dt,
            returnAggregated=False, returnFormat='Wide',
            includeBoundingValues=True
        )
    except Exception:
        _log_error(MODULE + "::_series_positive_delta/query")
        ds = None
    if not ds or ds.getRowCount() == 0:
        return 0

    peak = None
    for i in range(ds.getRowCount()):
        try:
            peak = float(ds.getValueAt(i, 1))
            break
        except Exception:
            continue
    if peak is None:
        return 0

    total = 0.0
    for i in range(ds.getRowCount()):
        try:
            v = float(ds.getValueAt(i, 1))
        except Exception:
            continue
        if v > peak:
            total += (v - peak)
            peak = v
    return int(total)


# ====================== config & cache loads ======================
def _get_settings():
    try:
        ds = system.db.runNamedQuery(_NQ_SEL + "getRollupSettings", {})
        if ds and ds.getRowCount():
            r = ds[0]
            return {
                "enabled":        bool(r["is_enabled"]),
                "week_start_dow": int(r["week_start_dow"]),  # 1..7 (Sun..Sat)
            }
    except Exception:
        _log_error(MODULE + "::_get_settings")
    return {"enabled": True, "week_start_dow": 2}  # default Monday

def _load_stations_if_needed(force=False):
    now_sec = system.date.toMillis(system.date.now()) // 1000
    if (not force) and (now_sec - _S.meta["lastStationsLoad"] < STATION_CACHE_SEC) and _S.meta["stations"]:
        return _S.meta["stations"]

    try:
        ds = system.db.runNamedQuery(_NQ_SEL + "getActiveStationsForRollup", {"critical_only": 1})
    except Exception:
        _log_error(MODULE + "::_load_stations_if_needed/query")
        ds = None

    out = []
    for r in (ds or []):
        try:
            st = {
                "station_id": int(r["station_id"]),
                "line_id":    int(r["line_id"]),
                "area":       _u(r["area_name"]),
                "subarea":    _u(r["subarea_name"]),
                "line":       _u(r["line_name"]),
                "station":    _u(r["station_name"]),
            }
            tag = u"%sMagnaStations/%s/%s/%s/%s/TotalParts" % (_PROV, st["area"], st["subarea"], st["line"], st["station"])
            st["tag"] = tag if system.tag.exists(tag) else None
            out.append(st)
        except Exception:
            _log_warn(MODULE + "::_load_stations_if_needed", message="row parse failed")

    _S.meta["stations"], _S.meta["lastStationsLoad"] = out, now_sec
    return out

def _reconcile_late_shifts_for_station(st, now_dt, shift_rows):
    lid = int(st["line_id"])
    sid = int(st["station_id"])
    tag = st["tag"]
    s   = _S.station_state[sid]

    done = s.setdefault("past_shift_done_keys", set())

    for win in _S.meta.get("pastShiftsByLine", {}).get(lid, []):
        key = u"%s|%s" % (int(win.id), unicode(win.date))
        if key in done:
            continue  # already emitted for this station

        # historian-anchored closed row
        if tag:
            start_cnt = _hist_value_at_or_before(tag, win.start)
            end_cnt   = _hist_value_at_or_before(tag, win.end)
            tot       = _series_positive_delta(tag, win.start, win.end)
        else:
            start_cnt = None
            end_cnt   = None
            tot       = 0

        shift_rows.append({
            "station_id": sid,
            "shift_id":   int(win.id),
            "shift_local_date": win.date,
            "total_parts": int(tot or 0),
            "start_count": int(start_cnt) if start_cnt is not None else None,
            "end_count":   int(end_cnt)   if end_cnt   is not None else None,
            "is_closed":   1
        })

        done.add(key)  # mark this window reconciled

def _load_shift_windows_if_needed(force=True):
    now = system.date.now()
    now_sec = system.date.toMillis(now) // 1000
    if (not force) and (now_sec - _S.meta["lastShiftLoad"] < SHIFT_CACHE_SEC) and (_S.meta["shiftsToday"] or _S.meta["shiftsYday"]):
        return

    today = system.date.format(now, "yyyy-MM-dd")
    yday  = system.date.format(system.date.addDays(now, -1), "yyyy-MM-dd")

    try:
        _S.meta["shiftsToday"] = list(system.db.runNamedQuery(_NQ_SEL + "getShiftScheduleOnDate", {"shift_date": today}) or [])
        _S.meta["shiftsYday"]  = list(system.db.runNamedQuery(_NQ_SEL + "getShiftScheduleOnDate", {"shift_date": yday})  or [])
    except Exception:
        _log_error(MODULE + "::_load_shift_windows_if_needed/query")

    # Build a per-line list of fully ended windows (end <= now) for both days
    past = {}
    for src in (_S.meta["shiftsYday"], _S.meta["shiftsToday"]):
        for r in (src or []):
            try:
                lid = int(r["line_id"])
                shid = int(r["shift_id"])
                st   = r["start_time"]
                en   = r["end_time"]
                if not st or not en:
                    continue
                if en <= now:
                    day_str = system.date.format(st, "yyyy-MM-dd")
                    past.setdefault(lid, []).append(ShiftWindow(id=shid, date=day_str, start=st, end=en))
            except Exception:
                _log_warn(MODULE + "::_load_shift_windows_if_needed", message="row parse failed for past window")
                continue
    # Sort each line’s list by end time to keep reconciliation deterministic
    for lid in list(past.keys()):
        try:
            past[lid].sort(key=lambda w: system.date.toMillis(w.end))
        except Exception:
            _log_warn(MODULE + "::_load_shift_windows_if_needed", message="sort failed for line %s" % lid)

    _S.meta["pastShiftsByLine"] = past
    _S.meta["lastShiftLoad"] = now_sec


# ====================== init station state ======================
def _ensure_init_station_state(st, settings):
    sid = st["station_id"]
    if sid in _S.station_state:
        return

    now      = system.date.now()
    cur_hr_u = _floor_hour_utc(now)
    week_st  = _week_start_local_sys(now, settings["week_start_dow"])

    # Read current live counter once
    curr = 0
    if st["tag"]:
        try:
            vq = system.tag.readBlocking([st["tag"]])[0]
            if vq.quality.isGood():
                curr = int(vq.value or 0)
        except Exception:
            curr = 0

    # ----- Hour (anchor at top-of-hour)
    hr_start_cnt = _hist_value_at_or_before(st["tag"], cur_hr_u)
    if hr_start_cnt is None:
        hr_start_cnt = float(curr)
    hour_partial = _series_positive_delta(st["tag"], cur_hr_u, now)

    # ----- Shift (if active now, anchor at shift start)
    sh = _active_shift_for_line(int(st["line_id"]), now)
    if sh.id is not None:
        sh_start_cnt = _hist_value_at_or_before(st["tag"], sh.start) if sh.start else float(curr)
        if sh_start_cnt is None:
            sh_start_cnt = float(curr)
        shift_partial = _series_positive_delta(st["tag"], sh.start, now) if sh.start else 0
    else:
        sh_start_cnt  = float(curr)
        shift_partial = 0

    # ----- Week (initialize “so far”)
    week_so_far = _series_positive_delta(st["tag"], week_st, now)

    _S.station_state[sid] = {
        "line_id": st["line_id"], "tag": st["tag"],
        # hour window
        "hour_start_utc": cur_hr_u,
        "hour_start_count": int(hr_start_cnt),
        "hour_total": int(hour_partial),
        "last_peak": int(curr),
        "hour_last_flush": system.date.toMillis(now),
        # shift window
        "shift_id": sh.id,
        "shift_date": sh.date,
        "shift_start_count": int(sh_start_cnt),
        "shift_total": int(shift_partial),
        # week window
        "week_start_local": week_st,
        "week_total": int(week_so_far),
        "past_shift_done_keys": set(),
    }


# ====================== bootstrap: create today's closed slots once ======================
def _bootstrap_today(stations, settings):
    """
    One-time per day:
      - Insert/merge all CLOSED hourly slots for today up to floor(now)
      - Insert/merge shift rows for today up to now (open or closed per schedule)
    """
    now = system.date.now()
    today_str = system.date.format(now, "yyyy-MM-dd")
    start_local = system.date.parse("%s 00:00" % today_str, "yyyy-MM-dd HH:mm")
    h_start = _floor_hour_utc(start_local)
    h_end   = _floor_hour_utc(now)  # exclusive; current hour stays live

    # ---- Hourly closed slots
    h_rows = []
    cur = h_start
    while cur < h_end:
        nxt = system.date.addHours(cur, 1)
        for st in stations:
            try:
                sid = st["station_id"]
                lid = st["line_id"]
                tag = st["tag"]
                if not tag:
                    # still write dense zero to seed slot
                    h_rows.append({
                        "station_id": sid, "line_id": lid,
                        "hour_start_utc_ms": system.date.toMillis(cur),
                        "total_parts": 0,
                        "start_count": None,
                        "end_count": None,
                        "is_closed": 1
                    })
                    continue
                start_cnt = _hist_value_at_or_before(tag, cur)
                end_cnt   = _hist_value_at_or_before(tag, nxt)
                tot = _series_positive_delta(tag, cur, nxt)
                h_rows.append({
                    "station_id": sid, "line_id": lid,
                    "hour_start_utc_ms": system.date.toMillis(cur),
                    "total_parts": int(tot or 0),
                    "start_count": int(start_cnt) if start_cnt is not None else None,
                    "end_count": int(end_cnt) if end_cnt is not None else None,
                    "is_closed": 1
                })
            except Exception:
                _log_warn(MODULE + "::_bootstrap_today", message="hour slot build failed")
        cur = nxt

    if h_rows:
        try:
            system.db.runNamedQuery(_NQ_INS + "upsertHourlyBatch", {"payload": system.util.jsonEncode(h_rows)})
        except Exception:
            _log_warn(MODULE + "::_bootstrap_today", message="upsertHourlyBatch failed during bootstrap")

    # ---- Shifts (today) up to now
    sh_rows = []
    try:
        shifts = system.db.runNamedQuery(_NQ_SEL + "getShiftScheduleOnDate", {"shift_date": today_str})
    except Exception:
        _log_error(MODULE + "::_bootstrap_today/queryShifts")
        shifts = None

    if shifts:
        # by line
        by_line = {}
        for st in stations:
            by_line.setdefault(int(st["line_id"]), []).append(st)

        for r in shifts:
            try:
                lid = int(r["line_id"])
                shift_id = int(r["shift_id"])
                st_dt = r["start_time"]
                en_dt = r["end_time"]
                if not st_dt or not en_dt:
                    continue
                eff_end = en_dt if en_dt <= now else now
                if eff_end <= st_dt:
                    continue
                for st in by_line.get(lid, []):
                    sid = st["station_id"]
                    tag = st["tag"]
                    start_cnt = _hist_value_at_or_before(tag, st_dt) if tag else None
                    tot = _series_positive_delta(tag, st_dt, eff_end) if tag else 0
                    sh_rows.append({
                        "station_id": sid,
                        "shift_id": shift_id,
                        "shift_local_date": system.date.format(st_dt, "yyyy-MM-dd"),
                        "total_parts": int(tot or 0),
                        "start_count": int(start_cnt) if start_cnt is not None else None,
                        "end_count": None if eff_end < en_dt else _hist_value_at_or_before(tag, en_dt),
                        "is_closed": 0 if eff_end < en_dt else 1
                    })
            except Exception:
                _log_warn(MODULE + "::_bootstrap_today", message="shift row build failed")

    if sh_rows:
        try:
            system.db.runNamedQuery(_NQ_INS + "upsertShiftBatch", {"payload": system.util.jsonEncode(sh_rows)})
        except Exception:
            _log_warn(MODULE + "::_bootstrap_today", message="upsertShiftBatch failed during bootstrap")


# ====================== batched DB writes ======================
def _flush_batches(hour_rows, shift_rows, week_rows, wm_rows):
    try:
        if hour_rows:
            system.db.runNamedQuery(_NQ_INS + "upsertHourlyBatch", {"payload": system.util.jsonEncode(hour_rows)})
        if shift_rows:
            system.db.runNamedQuery(_NQ_INS + "upsertShiftBatch",  {"payload": system.util.jsonEncode(shift_rows)})
        if week_rows:
            system.db.runNamedQuery(_NQ_INS + "upsertWeeklyBatch", {"payload": system.util.jsonEncode(week_rows)})
        if wm_rows:
            system.db.runNamedQuery(_NQ_INS + "upsertHourlyWatermarksBatch", {"payload": system.util.jsonEncode(wm_rows)})
    except Exception:
        _log_error(MODULE + "::_flush_batches")


def _active_shift_for_line(line_id, at_dt):
    """
    Return ShiftWindow (id,date,start,end) for 'line_id' at time 'at_dt';
    if none, returns ShiftWindow(None,None,None,None)
    """
    try:
        for r in _S.meta["shiftsToday"]:
            if int(r["line_id"]) == line_id:
                st, en = r["start_time"], r["end_time"]
                if st and en and (st <= at_dt < en):   # end exclusive
                    return ShiftWindow(
                        id=int(r["shift_id"]),
                        date=system.date.format(st, "yyyy-MM-dd"),
                        start=st,
                        end=en
                    )
        for r in _S.meta["shiftsYday"]:
            if int(r["line_id"]) == line_id:
                st, en = r["start_time"], r["end_time"]
                if st and en and (st <= at_dt < en):   # end exclusive
                    return ShiftWindow(
                        id=int(r["shift_id"]),
                        date=system.date.format(st, "yyyy-MM-dd"),
                        start=st,
                        end=en
                    )
    except Exception:
        _log_error(MODULE + "::_active_shift_for_line")
    return ShiftWindow(None, None, None, None)


# ====================== public entry (call every 5s) ======================
def run_rollups():
    """
    Call this from a 5-second Gateway Timer script:
        from MagnaDataOps import ProductionRollup as PR
        PR.run_rollups()
    """
    loc = MODULE + "::run_rollups"

    # small inner helpers to keep complexity in check without changing behavior
    def _maybe_hour_rollover(s, sid, cur_hr, now_ms, hour_rows, wm_rows):
        if s["hour_start_utc"].getTime() == cur_hr.getTime():
            return
        # close previous hour window
        hour_rows.append({
            "station_id": sid,
            "line_id":    s["line_id"],
            "hour_start_utc_ms": system.date.toMillis(s["hour_start_utc"]),
            "total_parts": int(max(0, s["hour_total"])),
            "start_count": int(s["hour_start_count"]),
            "end_count":   int(s["last_peak"]),
            "is_closed":   1
        })
        wm_rows.append({
            "station_id": sid,
            "last_utc_ms": system.date.toMillis(s["hour_start_utc"]),
            "cur_hour_start_utc_ms": system.date.toMillis(cur_hr),
            "cur_hour_start_count": int(s["last_peak"]),
            "cur_hour_last_peak":   int(s["last_peak"])
        })
        # reset hour baselines
        s["hour_start_utc"]   = cur_hr
        hr_start_cnt = _hist_value_at_or_before(s["tag"], cur_hr)
        s["hour_start_count"] = int(hr_start_cnt) if hr_start_cnt is not None else int(s["last_peak"])
        s["hour_total"]       = 0
        s["hour_last_flush"]  = now_ms

    def _maybe_flush_open_hour(s, sid, now_ms, hour_rows):
        if (now_ms - s["hour_last_flush"]) < (WRITE_IDLE_SEC * 1000):
            return
        hour_rows.append({
            "station_id": sid,
            "line_id":    s["line_id"],
            "hour_start_utc_ms": system.date.toMillis(s["hour_start_utc"]),
            "total_parts": int(max(0, s["hour_total"])),
            "start_count": int(s["hour_start_count"]),
            "end_count":   None,
            "is_closed":   0
        })
        s["hour_last_flush"] = now_ms

    try:
        settings = _get_settings()
        if not settings["enabled"]:
            _log_warn(loc, message="disabled in settings")
            return

        _load_shift_windows_if_needed()
        stations = _load_stations_if_needed()

        # ---- one-time daily bootstrap (dense closed hours + today's shifts to now)
        today_str = system.date.format(system.date.now(), "yyyy-MM-dd")
        if _S.boot_day != today_str:
            try:
                _bootstrap_today(stations, settings)
            except Exception:
                _log_warn(MODULE + "::bootstrap", message="bootstrap failed; continuing live")
            _S.boot_day = today_str

        # init station states (one-time per station)
        for st in stations:
            _ensure_init_station_state(st, settings)

        # batch-read live counters (good quality only)
        tags, idx = [], {}
        for st in stations:
            if st["tag"]:
                idx[st["tag"]] = st["station_id"]
                tags.append(st["tag"])
        try:
            reads = system.tag.readBlocking(tags) if tags else []
        except Exception:
            _log_error(loc + "::readBlocking")
            reads = []

        now     = system.date.now()
        now_ms  = system.date.toMillis(now)
        cur_hr  = _floor_hour_utc(now)
        week_st = _week_start_local_sys(now, settings["week_start_dow"])

        # prepare batches
        hour_rows, shift_rows, week_rows, wm_rows = [], [], [], []

        # RECONCILE: write closed rows for any past windows we learned about late
        for st in stations:
            try:
                _reconcile_late_shifts_for_station(st, now, shift_rows)
            except Exception:
                _log_warn(loc, message="late shift reconcile failed for sid=%s" % st["station_id"])

        # map current values
        cur_by_sid = {}
        for i, vq in enumerate(reads):
            try:
                if vq.quality.isGood():
                    sid = idx[tags[i]]
                    cur_by_sid[sid] = int(vq.value or 0)
            except Exception:
                # ignore bad element
                continue

        for st in stations:
            sid = st["station_id"]
            s   = _S.station_state[sid]
            curr = cur_by_sid.get(sid, s["last_peak"])  # freeze on bad quality

            # -------- Hour rollover detection --------
            # keep last_peak as "current" before rollover baseline changes
            s["last_peak"] = curr
            _maybe_hour_rollover(s, sid, cur_hr, now_ms, hour_rows, wm_rows)

            # -------- Incremental accumulation (reset-safe) --------
            # already set last_peak above; compute increment from previous
            # to preserve original behavior, we need a previous snapshot.
            # We'll derive inc using local cache prior to overwrite, so store temp:
            # (original logic updated last_peak *after* computing inc)
            # Here, imitate it by fetching diff with a stored field "_prev_peak" if present.
            prev_peak = s.get("_prev_peak", s["last_peak"])
            if curr >= prev_peak:
                inc = curr - prev_peak
                if inc > 0:
                    s["hour_total"] += inc
                    s["shift_total"] += inc
                    s["week_total"]  += inc
            # refresh _prev_peak for next iteration
            s["_prev_peak"] = curr

            # occasional open-hour write to keep charts alive
            _maybe_flush_open_hour(s, sid, now_ms, hour_rows)

            # -------- Shift handling via ShiftWindow --------
            cur_sh = _active_shift_for_line(int(s["line_id"]), now)
            try:
                if (cur_sh.id is not None) and cur_sh.end and \
                   (system.date.toMillis(now) >= system.date.toMillis(cur_sh.end)):
                    cur_sh = ShiftWindow(None, None, None, None)
            except Exception:
                _log_warn(loc, message="shift end compare failed")

            if s["shift_id"] != cur_sh.id or s["shift_date"] != cur_sh.date:
                # close previous shift if any
                if s["shift_id"] is not None and s["shift_date"]:
                    shift_rows.append({
                        "station_id": sid,
                        "shift_id": s["shift_id"],
                        "shift_local_date": s["shift_date"],
                        "total_parts": int(max(0, s["shift_total"])),
                        "start_count": int(s["shift_start_count"]),
                        "end_count": int(curr),
                        "is_closed": 1
                    })
                # open new shift (anchor at true shift start)
                s["shift_id"] = cur_sh.id
                s["shift_date"] = cur_sh.date
                if cur_sh.id is not None:
                    base = _hist_value_at_or_before(s["tag"], cur_sh.start) if cur_sh.start else None
                    s["shift_start_count"] = int(base) if base is not None else int(curr)
                    s["shift_total"] = _series_positive_delta(s["tag"], cur_sh.start, now) if cur_sh.start else 0
                else:
                    s["shift_start_count"] = int(curr)
                    s["shift_total"] = 0

            # upsert current (open) shift snapshot each tick so deletions self-heal
            if s["shift_id"] is not None:
                shift_rows.append({
                    "station_id": sid,
                    "shift_id": s["shift_id"],
                    "shift_local_date": s["shift_date"],
                    "total_parts": int(max(0, s["shift_total"])),
                    "start_count": int(s["shift_start_count"]),
                    "end_count": None,
                    "is_closed": 0
                })

            # -------- Weekly handling (local week anchor) --------
            if system.date.format(s["week_start_local"], "yyyy-MM-dd") != system.date.format(week_st, "yyyy-MM-dd"):
                # close last week
                week_rows.append({
                    "station_id": sid,
                    "week_start_local": system.date.format(s["week_start_local"], "yyyy-MM-dd"),
                    "total_parts": int(max(0, s["week_total"])),
                    "is_closed": 1
                })
                # open new week
                s["week_start_local"] = week_st
                week_seed = _series_positive_delta(s["tag"], week_st, now)
                s["week_total"] = int(week_seed)

            # upsert current (open) week
            week_rows.append({
                "station_id": sid,
                "week_start_local": system.date.format(s["week_start_local"], "yyyy-MM-dd"),
                "total_parts": int(max(0, s["week_total"])),
                "is_closed": 0
            })

        # -------- Persist all changes in 3–4 batched NQs --------
        if hour_rows or shift_rows or week_rows or wm_rows:
            _flush_batches(hour_rows, shift_rows, week_rows, wm_rows)

    except Exception:
        _log_error(loc)


def backfill_day_dense(date_str, write_zero_on_no_data=True, chunk=500):
    """
    Recompute & upsert all HOURLY rows (dense) and all SHIFT rows for a given past day.
    - date_str: 'yyyy-MM-dd' (Gateway system timezone day)
    - write_zero_on_no_data=True: if historian has no samples in an hour/shift, still write a closed row with total_parts=0
      and NULL start/end counts (keeps the day dense).
    - chunk: batch size for upsert payloads (to keep payloads small).
    Returns: {"hourly_upserted": <int>, "shift_upserted": <int>}
    """
    loc = MODULE + "::backfill_day_dense"
    try:
        # ensure we have the current station set (critical-only) and settings
        settings = _get_settings()
        stations = _load_stations_if_needed(force=True)
        if not stations:
            _log_warn(loc, message="No stations in scope")
            return {"hourly_upserted": 0, "shift_upserted": 0}

        # ---- Hourly (dense closed) -----------------------------------------
        start_local = system.date.parse("%s 00:00" % date_str, "yyyy-MM-dd HH:mm")
        end_local   = system.date.addDays(start_local, 1)
        cur_u = _floor_hour_utc(start_local)
        end_u = _floor_hour_utc(end_local)

        hourly_rows, h_up = [], 0
        while cur_u < end_u:
            nxt_u = system.date.addHours(cur_u, 1)
            for st in stations:
                try:
                    sid, lid, tag = st["station_id"], st["line_id"], st["tag"]

                    if tag:
                        # Anchor boundaries to last value at/before the boundary time
                        start_cnt = _hist_value_at_or_before(tag, cur_u)
                        end_cnt   = _hist_value_at_or_before(tag, nxt_u)

                        # Detect if we have *any* data context for this hour (inside or bounding)
                        had = False
                        try:
                            ds = system.tag.queryTagHistory(
                                paths=[tag], startDate=cur_u, endDate=nxt_u,
                                returnAggregated=False, returnFormat='Wide', includeBoundingValues=True
                            )
                            had = bool(ds and ds.getRowCount() > 0) or (start_cnt is not None) or (end_cnt is not None)
                        except Exception:
                            had = (start_cnt is not None) or (end_cnt is not None)

                        if had:
                            tot = _series_positive_delta(tag, cur_u, nxt_u)
                        else:
                            if not write_zero_on_no_data:
                                continue
                            tot = 0
                            start_cnt, end_cnt = None, None
                    else:
                        if not write_zero_on_no_data:
                            continue
                        tot, start_cnt, end_cnt = 0, None, None

                    hourly_rows.append({
                        "station_id": sid,
                        "line_id":    lid,
                        "hour_start_utc_ms": system.date.toMillis(cur_u),
                        "total_parts": int(tot or 0),
                        "start_count": int(start_cnt) if start_cnt is not None else None,
                        "end_count":   int(end_cnt)   if end_cnt   is not None else None,
                        "is_closed":   1
                    })
                except Exception:
                    _log_warn(loc, message="hour row build failed for sid=%s" % st.get("station_id"))

            if len(hourly_rows) >= int(chunk):
                try:
                    system.db.runNamedQuery(_NQ_INS + "upsertHourlyBatch",
                                            {"payload": system.util.jsonEncode(hourly_rows)})
                    h_up += len(hourly_rows)
                except Exception:
                    _log_error(loc + "::upsertHourlyBatch")
                hourly_rows = []
            cur_u = nxt_u

        # Flush remainder
        if hourly_rows:
            try:
                system.db.runNamedQuery(_NQ_INS + "upsertHourlyBatch",
                                        {"payload": system.util.jsonEncode(hourly_rows)})
                h_up += len(hourly_rows)
            except Exception:
                _log_error(loc + "::upsertHourlyBatch(remainder)")

        # ---- Shifts (closed) -----------------------------------------------
        sh_rows, sh_up = [], 0
        try:
            shifts = system.db.runNamedQuery(_NQ_SEL + "getShiftScheduleOnDate", {"shift_date": date_str})
        except Exception:
            _log_error(loc + "::queryShifts")
            shifts = None

        if shifts:
            by_line = {}
            for st in stations:
                by_line.setdefault(int(st["line_id"]), []).append(st)

            for r in shifts:
                try:
                    lid      = int(r["line_id"])
                    shift_id = int(r["shift_id"])
                    st_dt    = r["start_time"]
                    en_dt    = r["end_time"]
                    if not st_dt or not en_dt or en_dt <= st_dt:
                        continue

                    for st in by_line.get(lid, []):
                        sid, tag = st["station_id"], st["tag"]
                        if tag:
                            start_cnt = _hist_value_at_or_before(tag, st_dt)
                            end_cnt   = _hist_value_at_or_before(tag, en_dt)
                            tot       = _series_positive_delta(tag, st_dt, en_dt)
                            had       = (tot > 0) or (start_cnt is not None) or (end_cnt is not None)
                            if not had and not write_zero_on_no_data:
                                continue
                        else:
                            if not write_zero_on_no_data:
                                continue
                            start_cnt, end_cnt, tot = None, None, 0

                        sh_rows.append({
                            "station_id":       sid,
                            "shift_id":         shift_id,
                            "shift_local_date": system.date.format(st_dt, "yyyy-MM-dd"),
                            "total_parts":      int(tot or 0),
                            "start_count":      int(start_cnt) if start_cnt is not None else None,
                            "end_count":        int(end_cnt)   if end_cnt   is not None else None,
                            "is_closed":        1
                        })

                    if len(sh_rows) >= int(chunk):
                        try:
                            system.db.runNamedQuery(_NQ_INS + "upsertShiftBatch",
                                                    {"payload": system.util.jsonEncode(sh_rows)})
                            sh_up += len(sh_rows)
                        except Exception:
                            _log_error(loc + "::upsertShiftBatch")
                        sh_rows = []
                except Exception:
                    _log_warn(loc, message="shift row build failed")

        # Flush remainder
        if sh_rows:
            try:
                system.db.runNamedQuery(_NQ_INS + "upsertShiftBatch",
                                        {"payload": system.util.jsonEncode(sh_rows)})
                sh_up += len(sh_rows)
            except Exception:
                _log_error(loc + "::upsertShiftBatch(remainder)")

        _log_info(loc, message="date=%s hourly_rows_upserted=%d shift_rows_upserted=%d" % (date_str, h_up, sh_up))
        return {"hourly_upserted": h_up, "shift_upserted": sh_up}

    except Exception:
        _log_error(loc)
        return {"hourly_upserted": 0, "shift_upserted": 0}