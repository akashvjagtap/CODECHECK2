# <summary>
# Module Name : ProductionTargetsLive
# Description : Computes effective CT per station (with parallelism), writes CT segments
#               (seed + on-change), and upserts break-aware hourly/shift base targets.
# Scope       : Gateway (Timer: call run_targets() every ~5s)
# Author      : Akash J
# Created On  : 2025-06-22
# </summary>

import system
from collections import namedtuple
from java.util import Calendar, TimeZone
from MagnaDataOps.LoggerFunctions import (
    log_info as _log_info,
    log_warn as _log_warn,
    log_error as _log_error,
)

MODULE = "ProductionTargetsLive"

# ---------- Named Query bases ----------
PBASE = "MagnaDataOps/Dashboard/AllProductionData/TargetCycleTime/"
PPATH = "MagnaDataOps/Dashboard/AllProductionData/TargetCycleTime/"

# ---------- Tunables ----------
DEBOUNCE_TICKS             = 1          # fixture set must be stable this many ticks to switch CT
CONFIG_REFRESH_SEC         = 60         # refresh stations + per-station CT cache
MAX_FIX_PER_SIDE           = 8          # safety cap for TT
WATCHDOG_SEC               = 10         # (kept for completeness; not used in side-agnostic reads)
CFG_REFRESH_PER_STATION_MS = 10000      # throttle for on-demand part CT refresh
_REPAIR_PERIOD_SEC         = 120        # run repair once every 2 minutes
_repair_last_run_ms        = 0          # module-level

# Breaks & shifts
BREAKS_REFRESH_SEC         = 120
SHIFT_REFRESH_SEC          = 60

# Writes
WRITE_CT_SEGMENTS          = True   # seed + on-change CT segments
WRITE_HOURLY_BASE_TO_DB    = True   # break-aware base per hour
WRITE_SHIFT_BASE_TO_DB     = True   # break-aware base per shift

# ------ Repair windows (lookback) ------
_REPAIR_HOURLY_LOOKBACK_HRS = 24
_REPAIR_SHIFT_LOOKBACK_DAYS = 2

# ------ Named queries (selects) ------
_NQ_GET_HOURS_MISSING_BASE  = "getHoursMissingBase"
_NQ_GET_SHIFTS_MISSING_BASE = "getShiftsMissingBase"

# ---------- Module state ----------
# sid -> per-station state (no live accumulators here)
_tstate = {}
_cfg    = {
    "last_load": 0,
    "stations": [],
    "parts_ct_by_sid": {},
    "parts_mult_by_sid": {},
    "ct_epoch_by_sid": {}
}

_breaks = {"last_load": 0, "today": None, "yday": None, "by_line": {}}
_shifts = {"last_load": 0, "today": None, "yday": None, "by_line": {}}

# ---------- Minimal safe NQ wrappers ----------

# Simple, CodeGuru-safe NQ helpers
def _nq_exec(path, params):
    """
    Works with Query, Scalar, or Update.
    Returns a dict: {"ok": bool, "data": object|None}
    Treats 'no result set' as success. Single return.
    """
    ok = False
    data = None
    try:
        data = system.db.runNamedQuery(path, params or {})
        ok = True
    except Exception as e:
        # Many EXEC/UPDATE NQs wired as "Query" throw this — accept as success.
        if _is_no_resultset_exc(e):
            ok = True
            data = None
        else:
            # Lightweight log; no extra try/except
            system.util.getLogger("ProductionTargetsLive")\
                .error(u"_nq_exec failed | path=%s | err=%s | params=%s" %
                       (unicode(path), unicode(e), unicode(params)))
    return {"ok": ok, "data": data}


def _nq_select(path, params):
    """
    Dataset-or-empty. Single return; no tuple unpacking.
    """
    out = []
    r = _nq_exec(path, params)
    if r["ok"] and r["data"] is not None:
        try:
            # Dataset-like? (has getRowCount)
            _ = r["data"].getRowCount()
            out = r["data"]
        except Exception:
            out = []  # scalar/mismatched -> empty
    return out


def _nq_update(path, params):
    """
    Fire-and-forget. Errors already logged in _nq_exec.
    """
    _nq_exec(path, params)

# ---------- Helpers ----------
def _floor_hour_utc(d):
    cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.setTime(d)
    cal.set(Calendar.MILLISECOND, 0); cal.set(Calendar.SECOND, 0); cal.set(Calendar.MINUTE, 0)
    return cal.getTime()

def _floor_hour_local(d):
    cal = Calendar.getInstance()  # LOCAL tz
    cal.setTime(d)
    cal.set(Calendar.MILLISECOND, 0); cal.set(Calendar.SECOND, 0); cal.set(Calendar.MINUTE, 0)
    return cal.getTime()

def _safe_n(fps):
    try:
        n = int(fps or 0)
    except Exception:
        n = 0
    return 1 if n < 1 else n

def _datestr(d):
    return system.date.format(d, "yyyy-MM-dd")

def _col(r, name):
    try:
        return r[name]
    except Exception:
        return None

# ---------- Stations / per-part CT cache ----------
def _load_stations_if_needed():
    now_ms = system.date.toMillis(system.date.now())
    if _cfg["stations"] and (now_ms - _cfg["last_load"] < CONFIG_REFRESH_SEC * 1000):
        return _cfg["stations"]
    ds = _nq_select(PBASE + "getActiveStationsForTargets", {})
    out = []
    for r in (ds or []):
        try:
            try:
                pf = float(r["parallelism_factor"])
            except Exception:
                pf = 0.0
            out.append({
                "station_id":        int(r["station_id"]),
                "line_id":           int(r["line_id"]),
                "area":              unicode(r["area_name"]),
                "subarea":           unicode(r["subarea_name"]),
                "line":              unicode(r["line_name"]),
                "station":           unicode(r["station_name"]),
                "is_turntable":      bool(r["is_turntable"]),
                "fixtures_per_side": min(max(int(r["fixtures_per_side"] or 0), 0), MAX_FIX_PER_SIDE),
                "is_critical":       bool(r["is_critical"]),
                "parallelism_factor": pf,
            })
        except Exception as e:
            _log_warn(MODULE + "::_load_stations_if_needed[row]", message=unicode(e))
    _cfg["stations"] = out
    _cfg["last_load"] = now_ms

    # refresh per-station part CTs (+ multipliers)
    _cfg["parts_ct_by_sid"] = {}
    _cfg["parts_mult_by_sid"] = {}
    for st in out:
        _refresh_station_ct_config(st["station_id"])
    return _cfg["stations"]

def _refresh_station_ct_config(sid):
    try:
        ds2 = _nq_select(PBASE + "getPartCTsForStation", {"station_id": sid}) or []
    except Exception as e:
        _log_error(MODULE + "::_refresh_station_ct_config/q", message=unicode(e))
        ds2 = []

    ctmap   = {}
    multmap = {}

    try:
        # Dataset path
        for i in range(ds2.getRowCount()):
            try:
                pn_raw = ds2.getValueAt(i, "part_number")
                ct_raw = ds2.getValueAt(i, "cycle_time")
                m_raw  = ds2.getValueAt(i, "overcycle_multiplier")

                pn = unicode(pn_raw) if pn_raw is not None else u""
                ct = float(ct_raw or 0.0)

                m = None
                if m_raw is not None:
                    try:
                        m = float(m_raw)
                    except Exception:
                        m = None

                if ct > 0.0 and pn:
                    ctmap[pn] = ct
                    if m is not None and m > 0.0:
                        multmap[pn] = m
            except Exception as e:
                _log_warn(MODULE + "::_refresh_station_ct_config[row]", message=unicode(e))
    except Exception:
        # Fallback for python list-of-dicts
        for r in (ds2 or []):
            try:
                pn = unicode(r.get("part_number") or u"")
                ct = float(r.get("cycle_time") or 0.0)
                m  = r.get("overcycle_multiplier")
                if ct > 0.0 and pn:
                    ctmap[pn] = ct
                    try:
                        if m is not None and float(m) > 0.0:
                            multmap[pn] = float(m)
                    except Exception as e:
                        _log_warn(MODULE + "::_refresh_station_ct_config[mult]", message=unicode(e))
            except Exception as e:
                _log_warn(MODULE + "::_refresh_station_ct_config[row2]", message=unicode(e))

    _cfg["parts_ct_by_sid"][sid]   = ctmap
    _cfg["parts_mult_by_sid"][sid] = multmap
    return ctmap

# ---------- Manual config refresh hooks ----------
def on_config_changed(station_id=None):
    """
    Call this right after you update config in the DB.
    Ensures next run_targets() uses fresh CTs and opens a new segment immediately.
    """
    try:
        if station_id is None:
            # Full reload: clear caches so next load refreshes all
            _cfg["stations"] = []
            _cfg["parts_ct_by_sid"].clear()
            _cfg["parts_mult_by_sid"].clear()
            _cfg["last_load"] = 0
            _load_stations_if_needed()
        else:
            sid = int(station_id)
            _refresh_station_ct_config(sid)

            # Force a fresh evaluation/open on next tick
            s = _tstate.get(sid)
            if s:
                s["last_parts_key"] = None
                s["stable_ticks"]   = DEBOUNCE_TICKS  # accept immediately
                s["ct_eff"]         = 0.0             # ensure 'changed' comparison triggers
                s["seg_opened"]     = False
                s["pending_seg"]    = None
                s["overcycle_multiplier"] = None
                s["force_open_now"] = True            # open segment immediately on next tick
                s["last_cfg_refresh_ms"] = 0          # avoid throttle
    except Exception as e:
        _log_error(MODULE + "::on_config_changed", message=unicode(e))

# ---------- Shifts ----------
def _load_shifts_if_needed():
    now = system.date.now()
    now_ms = system.date.toMillis(now)
    if (now_ms - _shifts["last_load"] < SHIFT_REFRESH_SEC * 1000) and _shifts["today"] == system.date.format(now, "yyyy-MM-dd"):
        return

    def _grab(day):
        try:
            return list(_nq_select(PBASE + "getShiftScheduleOnDate", {"shift_date": day}) or [])
        except Exception as e:
            _log_error(MODULE + "::_load_shifts_if_needed/_grab", message=unicode(e))
            return []

    today = system.date.format(now, "yyyy-MM-dd")
    yday  = system.date.format(system.date.addDays(now, -1), "yyyy-MM-dd")
    by_line = {}

    for day in (yday, today):
        for r in _grab(day):
            try:
                lid = int(r["line_id"]); shid = int(r["shift_id"])
                st  = r["start_time"];   en   = r["end_time"]
                if not st or not en:
                    continue
                by_line.setdefault(lid, []).append((shid, day, system.date.toMillis(st), system.date.toMillis(en)))
            except Exception as e:
                _log_warn(MODULE + "::_load_shifts_if_needed[row]", message=unicode(e))

    for lid in by_line:
        try:
            by_line[lid].sort(key=lambda t: t[2])
        except Exception as e:
            _log_warn(MODULE + "::_load_shifts_if_needed/sort", message=unicode(e))

    _shifts["by_line"]   = by_line
    _shifts["last_load"] = now_ms
    _shifts["today"]     = today
    _shifts["yday"]      = yday

ShiftWindow = namedtuple("ShiftWindow", "id date start_ms end_ms")

def _active_shift_for_line(line_id, now_ms):
    wins = _shifts["by_line"].get(int(line_id), [])
    for (shid, day, s_ms, e_ms) in wins:
        if s_ms <= now_ms < e_ms:
            return ShiftWindow(shid, day, s_ms, e_ms)
    return ShiftWindow(None, None, None, None)

# ---------- Breaks ----------
def _load_breaks_if_needed():
    now = system.date.now()
    now_ms = system.date.toMillis(now)
    today = _datestr(now)
    yday  = _datestr(system.date.addDays(now, -1))

    if (_breaks["by_line"] and
        (now_ms - _breaks["last_load"] < BREAKS_REFRESH_SEC * 1000) and
        _breaks["today"] == today and _breaks["yday"] == yday):
        return

    by_line = {}
    try:
        for day in (yday, today):
            ds = _nq_select(PBASE + "getBreaksOnDate", {"shift_date": day})
            for r in (ds or []):
                try:
                    if not bool(r["is_active"]):
                        continue
                    lid = int(r["line_id"])
                    bstart = r["break_start_time"]; bend = r["break_end_time"]
                    if not bstart or not bend:
                        continue
                    s = system.date.toMillis(bstart); e = system.date.toMillis(bend)
                    if e <= s:
                        continue
                    by_line.setdefault(lid, []).append((s, e))
                except Exception as e:
                    _log_warn(MODULE + "::_load_breaks_if_needed[row]", message=unicode(e))
    except Exception as e:
        _log_error(MODULE + "::_load_breaks_if_needed/q", message=unicode(e))

    # merge overlaps
    for lid, spans in by_line.items():
        try:
            spans.sort()
            merged = []
            for s, e in spans:
                if not merged or s > merged[-1][1]:
                    merged.append([s, e])
                else:
                    merged[-1][1] = max(merged[-1][1], e)
            by_line[lid] = [(a, b) for (a, b) in merged]
        except Exception as e:
            _log_warn(MODULE + "::_load_breaks_if_needed/merge", message=unicode(e))

    _breaks["by_line"]   = by_line
    _breaks["last_load"] = now_ms
    _breaks["today"]     = today
    _breaks["yday"]      = yday

def _working_ms(start_ms, end_ms, line_id):
    total = max(0, end_ms - start_ms)
    if total == 0:
        return 0
    spans = _breaks["by_line"].get(int(line_id), [])
    if not spans:
        return total
    blocked = 0
    for s, e in spans:
        lo = max(start_ms, s); hi = min(end_ms, e)
        if hi > lo:
            blocked += (hi - lo)
    return max(0, total - blocked)

def _hour_working_seconds(hour_start_ms, line_id):
    return _working_ms(hour_start_ms, hour_start_ms + 3600*1000, line_id) // 1000

# ---------- Tag paths ----------
def _root(st):
    return u"[MagnaDataOps]MagnaStations/%s/%s/%s/%s" % (st["area"], st["subarea"], st["line"], st["station"])

def _station_total_path(st):     # station total (historized)
    return _root(st) + u"/TotalParts"

def _tt_fixture_part_paths(st, side):
    n = _safe_n(st["fixtures_per_side"])
    base = _root(st) + u"/TurntableSide_%s/TurntableFixtures" % side
    return [base + u"/TurntableFixture_%d/Part_Number" % i for i in range(1, n + 1)]

def _non_tt_fixture_part_paths(st):
    n = _safe_n(st["fixtures_per_side"])
    base = _root(st)
    return [base + u"/Fixture_%d/Part_Number" % i for i in range(1, n + 1)]

# ---------- Live parts snapshot (side-agnostic) ----------
def _read_current_parts(st):
    """
    Returns (parts_used:list[str], had_any:bool)
    - For TT: read both sides' fixtures 1..fps and, for each fixture index,
      pick the non-empty value with the newer timestamp; tie -> side 1.
    - For non-TT: read fixtures 1..fps (usually the same part), keep non-empty.
    """
    fps = _safe_n(st["fixtures_per_side"])

    def _pick_value(vq):
        try:
            if hasattr(vq, "quality") and vq.quality.isGood() and vq.value:
                return unicode(vq.value), getattr(vq, "timestamp", None)
        except Exception as e:
            _log_warn(MODULE + "::_read_current_parts/_pick_value", message=unicode(e))
        return None, None

    parts = []

    if st["is_turntable"]:
        paths1 = _tt_fixture_part_paths(st, "1")
        paths2 = _tt_fixture_part_paths(st, "2")
        try:
            vqs1 = system.tag.readBlocking(paths1) if paths1 else []
            vqs2 = system.tag.readBlocking(paths2) if paths2 else []
        except Exception as e:
            _log_error(MODULE + "::_read_current_parts/tt_read", message=unicode(e))
            vqs1, vqs2 = [], []

        for i in range(fps):
            v1, t1 = _pick_value(vqs1[i]) if i < len(vqs1) else (None, None)
            v2, t2 = _pick_value(vqs2[i]) if i < len(vqs2) else (None, None)

            chosen = None
            if v1 and not v2:
                chosen = v1
            elif v2 and not v1:
                chosen = v2
            elif v1 and v2:
                try:
                    if t1 and t2:
                        chosen = v1 if (t1 >= t2) else v2
                    elif t1 and not t2:
                        chosen = v1
                    elif t2 and not t1:
                        chosen = v2
                    else:
                        chosen = v1
                except Exception as e:
                    _log_warn(MODULE + "::_read_current_parts/tt_tiebreak", message=unicode(e))
                    chosen = v1
            if chosen:
                parts.append(chosen)
    else:
        paths = _non_tt_fixture_part_paths(st)
        try:
            vqs = system.tag.readBlocking(paths) if paths else []
        except Exception as e:
            _log_error(MODULE + "::_read_current_parts/non_tt_read", message=unicode(e))
            vqs = []
        for i in range(min(fps, len(vqs))):
            v, _ = _pick_value(vqs[i])
            if v:
                parts.append(v)

    parts = [p for p in parts if p][:fps]
    return parts, (len(parts) > 0)

# ---------- Historian: first increment time ----------
def _first_increment_ts(tag_path, prev_val, start_ms, end_ms):
    """
    Find the first timestamp where the counter rises above prev_val
    between start_ms and end_ms. Returns java.util.Date or None.
    """
    try:
        ds = system.tag.queryTagHistory(
            paths=[tag_path],
            startDate=system.date.fromMillis(start_ms),
            endDate=system.date.fromMillis(end_ms),
            returnAggregated=False,
            returnFormat='Wide',
            includeBoundingValues=True
        )
        if not ds or ds.getRowCount() == 0:
            return None
        base = None if prev_val is None else float(prev_val)
        for i in range(ds.getRowCount()):
            try:
                v = float(ds.getValueAt(i, 1))
                if base is None:
                    base = v
                elif v > base:
                    return ds.getValueAt(i, 0)
                else:
                    if v > base:
                        base = v
            except Exception as e:
                _log_warn(MODULE + "::_first_increment_ts[row]", message=unicode(e))
    except Exception as e:
        _log_error(MODULE + "::_first_increment_ts", message=unicode(e))
    return None

# ---------- Effective CT (with parallelism factor) ----------
def _ct_eff_from_cts(ct_list, fixtures_per_side, parallelism_factor=None):
    """
    Compute effective CT (sec/part) from configured per-part CTs.
      - If only 1 CT: that value.
      - If 2+ CTs:
          parallelism_factor = 0.0 → arithmetic mean
          parallelism_factor = 1.0 → max(cts)/len(cts)
          between → blend of mean and max/len
    """
    cts = [float(ct) for ct in (ct_list or []) if ct and ct > 0.0]
    if not cts:
        return 0.0
    if len(cts) == 1:
        return cts[0]

    k = len(cts)
    mean_ct = sum(cts) / float(k)
    par_ct  = max(cts) / float(k)

    if parallelism_factor is None:
        return mean_ct

    lam = max(0.0, min(1.0, float(parallelism_factor)))
    return (1.0 - lam) * mean_ct + lam * par_ct

def _ct_eff_for_station(sid, fps):
    s = _tstate.get(int(sid))
    if s and s.get("ct_eff") and s["ct_eff"] > 0:
        return float(s["ct_eff"])
    cfg_cts = _cfg["parts_ct_by_sid"].get(int(sid), {})
    if cfg_cts:
        lam = 0.0
        for _st in (_cfg.get("stations") or []):
            if int(_st.get("station_id")) == int(sid):
                lam = float(_st.get("parallelism_factor", 0.0))
                break
        slow_sorted = sorted(cfg_cts.items(), key=lambda kv: kv[1], reverse=True)
        take = [v for (_, v) in slow_sorted[:_safe_n(fps)]]
        ct = _ct_eff_from_cts(take, _safe_n(fps), lam)
        if ct and ct > 0.0:
            return float(ct)
    return 0.0

def _repair_missing_bases():
    try:
        _load_breaks_if_needed()
        _load_shifts_if_needed()
        stations = _load_stations_if_needed()
        if not stations:
            return

        by_sid = {int(st["station_id"]): {"line_id": int(st["line_id"]), "fps": int(st["fixtures_per_side"] or 1)}
                  for st in stations}

        # ----- Hours -----
        try:
            ds_h = _nq_select(PBASE + _NQ_GET_HOURS_MISSING_BASE,
                              {"lookback_hours": int(_REPAIR_HOURLY_LOOKBACK_HRS)})
        except Exception as e:
            _log_error(MODULE + "::_repair_missing_bases/hours_q", message=unicode(e))
            ds_h = []

        up_h = []
        for r in (ds_h or []):
            try:
                sid = int(_col(r, "station_id")); lid = int(_col(r, "line_id"))
                meta = by_sid.get(sid)
                if not meta:
                    continue
                fps = meta["fps"]
                ct  = _ct_eff_for_station(sid, fps)
                if ct <= 0.0:
                    continue

                # Prefer local hour for break math; fall back to utc start
                hloc = _col(r, "hour_local") or _col(r, "hour_start_utc")
                if not hloc:
                    continue
                h_ms = system.date.toMillis(hloc)
                work_sec = int(_hour_working_seconds(h_ms, lid))
                base = int(work_sec / ct) if work_sec > 0 else 0
                if base <= 0:
                    continue

                up_h.append({
                    "station_id":        sid,
                    "line_id":           lid,
                    "hour_start_utc_ms": system.date.toMillis(_col(r, "hour_start_utc")),
                    "target_parts_base": int(base)
                })
            except Exception as e:
                _log_warn(MODULE + "::_repair_missing_bases/hours_row", message=unicode(e))

        if up_h:
            try:
                _nq_update(PPATH + "upsertHourlyTargetsBatch",
                           {"payload": system.util.jsonEncode(up_h)})
            except Exception as e:
                _log_error(MODULE + "::_repair_missing_bases/hours_upsert", message=unicode(e))

        # ----- Shifts -----
        try:
            ds_s = _nq_select(PBASE + _NQ_GET_SHIFTS_MISSING_BASE,
                              {"lookback_days": int(_REPAIR_SHIFT_LOOKBACK_DAYS)})
        except Exception as e:
            _log_error(MODULE + "::_repair_missing_bases/shifts_q", message=unicode(e))
            ds_s = []

        up_s = []
        for r in (ds_s or []):
            try:
                sid = int(_col(r, "station_id")); lid = int(_col(r, "line_id"))
                meta = by_sid.get(sid)
                if not meta:
                    continue
                fps = meta["fps"]
                ct  = _ct_eff_for_station(sid, fps)
                if ct <= 0.0:
                    continue

                st_loc = _col(r, "shift_start_local"); en_loc = _col(r, "shift_end_local")
                if not st_loc or not en_loc:
                    continue
                s_ms = system.date.toMillis(st_loc); e_ms = system.date.toMillis(en_loc)
                if e_ms <= s_ms:
                    continue

                work_sec = _working_ms(s_ms, e_ms, lid) // 1000
                base = int(work_sec / ct) if work_sec > 0 else 0
                if base < 0:
                    base = 0

                up_s.append({
                    "station_id":        sid,
                    "shift_id":          int(_col(r, "shift_id")),
                    "shift_local_date":  _col(r, "shift_local_date"),
                    "target_parts_base": int(base)
                })
            except Exception as e:
                _log_warn(MODULE + "::_repair_missing_bases/shifts_row", message=unicode(e))

        if up_s:
            try:
                _nq_update(PPATH + "upsertShiftTargetsBatch",
                           {"payload": system.util.jsonEncode(up_s)})
            except Exception as e:
                _log_error(MODULE + "::_repair_missing_bases/shifts_upsert", message=unicode(e))

    except Exception as e:
        _log_error(MODULE + "::_repair_missing_bases", message=unicode(e))

# ---------- Core tick ----------
def run_targets():
    """
    Computes CT, writes CT segments (seed + on-change), and upserts:
      - Hourly base target (break-aware)
      - Shift base target  (break-aware)
    """
    try:
        stations = _load_stations_if_needed()
        if not stations:
            return

        now     = system.date.now()
        now_ms  = system.date.toMillis(now)
        hour_u  = _floor_hour_utc(now)      # for DB (UTC)
        hour_l  = _floor_hour_local(now)    # for break math (LOCAL)
        hour_utc_ms   = system.date.toMillis(hour_u)
        hour_local_ms = system.date.toMillis(hour_l)

        _load_breaks_if_needed()
        _load_shifts_if_needed()

        # init / hour rollover
        for st in stations:
            sid = st["station_id"]
            s = _tstate.get(sid)
            if not s:
                _tstate[sid] = {
                    "prev_total_flat": None,   # last /TotalParts value
                    "last_parts_key": None,
                    "stable_ticks": 0,
                    "ct_eff": 0.0,
                    "seg_opened": False,
                    "pending_seg": None,
                    "last_increment_prev": None,
                    "last_increment_ms": 0,
                    "last_cfg_refresh_ms": 0,
                    "last_poll_ms": now_ms,
                    "overcycle_multiplier": None,
                    "force_open_now": False,
                    # hour anchors
                    "hour_start_utc_ms":   hour_utc_ms,
                    "hour_start_local_ms": hour_local_ms,
                    "last_hour_base": None,
                    # shift anchors
                    "shift_id": None,
                    "shift_date": None,
                    "shift_start_ms": None,
                    "shift_end_ms": None,
                    "last_shift_base": None,
                }
            else:
                if s["hour_start_utc_ms"] != hour_utc_ms:
                    s["hour_start_utc_ms"]   = hour_utc_ms
                    s["hour_start_local_ms"] = hour_local_ms
                    s["last_hour_base"] = None  # recompute at hour open

        # --- Increment detection (anchor) using station-level TotalParts only ---
        total_paths, total_idx = [], []
        for st in stations:
            total_paths.append(_station_total_path(st)); total_idx.append(st["station_id"])

        try:
            reads = system.tag.readBlocking(total_paths) if total_paths else []
        except Exception as e:
            _log_error(MODULE + "::run_targets/readBlocking", message=unicode(e))
            reads = []

        for i, vq in enumerate(reads):
            sid = total_idx[i]
            s = _tstate.get(sid)
            if not s:
                continue
            if hasattr(vq, "quality") and vq.quality.isGood():
                try:
                    val = int(vq.value or 0)
                except Exception:
                    val = None
                prev = s.get("prev_total_flat")
                s["prev_total_flat"] = val
                if prev is not None and val is not None and val > prev:
                    s["last_increment_prev"] = prev
                    s["last_increment_ms"]   = now_ms

        # Prepare DB batches
        hourly_rows = []
        shift_rows  = []

        # Per-station CT & base targets
        for st in stations:
            try:
                sid = st["station_id"]
                lid = st["line_id"]
                fps_db = st["fixtures_per_side"]
                fps = _safe_n(fps_db)
                s = _tstate[sid]
                cfg_cts   = _cfg["parts_ct_by_sid"].get(sid, {})
                cfg_mults = _cfg["parts_mult_by_sid"].get(sid, {})

                # Live parts snapshot (side-agnostic)
                parts_used_snapshot, has_any_snapshot = _read_current_parts(st)
                live_parts = list(parts_used_snapshot)

                # On-demand CT config refresh if new parts appear
                if live_parts:
                    unknown = [p for p in live_parts if p not in cfg_cts]
                    if unknown and (now_ms - s.get("last_cfg_refresh_ms", 0) >= CFG_REFRESH_PER_STATION_MS):
                        cfg_cts = _refresh_station_ct_config(sid)
                        cfg_mults = _cfg["parts_mult_by_sid"].get(sid, {})
                        s["last_cfg_refresh_ms"] = now_ms

                # --- Missing configuration detection (forces CT=0 if missing) ---
                missing_parts = []
                is_missing_cfg = False
                eff_mult = 0.0  # default when missing
                parts_used = []
                ct_list = []

                if live_parts:
                    for p in live_parts[:fps]:
                        if p not in cfg_cts or (cfg_cts.get(p, 0.0) or 0.0) <= 0.0:
                            missing_parts.append(p)
                    if missing_parts:
                        ct_eff_new = 0.0
                        parts_used = live_parts[:fps]
                        is_missing_cfg = True
                    else:
                        # Build CT list for effective CT (normal path with live parts)
                        slowest = max(cfg_cts.values()) if cfg_cts else 0.0
                        for p in live_parts[:fps]:
                            ct = cfg_cts.get(p, slowest if slowest > 0 else 0.0)
                            ct_list.append(ct); parts_used.append(p)
                        while len(ct_list) < fps and (cfg_cts or parts_used):
                            pad_ct = max(cfg_cts.values()) if cfg_cts else (ct_list[0] if ct_list else 0.0)
                            ct_list.append(pad_ct); parts_used.append(parts_used[0] if parts_used else u"")
                else:
                    if not cfg_cts:
                        ct_eff_new = 0.0
                        parts_used = []
                        is_missing_cfg = True
                    else:
                        # No live parts; seed from slowest configured parts
                        slow_sorted = sorted(cfg_cts.items(), key=lambda kv: kv[1], reverse=True)
                        take = slow_sorted[:fps]
                        parts_used = [k for (k, _) in take]
                        ct_list    = [v for (_, v) in take]
                        if take and len(take) < fps:
                            slow = take[0][1]
                            while len(ct_list) < fps:
                                ct_list.append(slow); parts_used.append(parts_used[0])

                # Debounce CT changes by parts set (preserve prior CT if parts disappear)
                parts_key = "|".join(parts_used) if parts_used else ""
                if parts_key == s.get("last_parts_key"):
                    s["stable_ticks"] = min(DEBOUNCE_TICKS, s.get("stable_ticks", 0) + 1)
                else:
                    s["last_parts_key"] = parts_key
                    s["stable_ticks"]   = 1 if not is_missing_cfg else DEBOUNCE_TICKS

                # --- CT using station parallelism factor ---
                lam = float(st.get("parallelism_factor", 0.0))
                if not is_missing_cfg:
                    ct_eff_new = s["ct_eff"]
                    if parts_used:
                        if s["stable_ticks"] >= DEBOUNCE_TICKS:
                            ct_eff_new = _ct_eff_from_cts(ct_list, fps, lam)
                    else:
                        if (not s["ct_eff"]) and cfg_cts:
                            slow_sorted = sorted(cfg_cts.items(), key=lambda kv: kv[1], reverse=True)
                            seed = [v for (_, v) in slow_sorted[:fps]]
                            ct_eff_new = _ct_eff_from_cts(seed, fps, lam)

                    # --- Effective overcycle multiplier with same parallelism logic ---
                    if parts_used:
                        mult_vals = []
                        for p in parts_used:
                            mv = cfg_mults.get(p)
                            try:
                                fv = float(mv) if mv is not None else None
                            except Exception:
                                fv = None
                            if fv is not None and fv > 0.0:
                                mult_vals.append(fv)

                        if not mult_vals:
                            eff_mult = 0.0
                        elif len(mult_vals) == 1:
                            eff_mult = mult_vals[0]
                        else:
                            mean_m = sum(mult_vals) / float(len(mult_vals))
                            min_m  = min(mult_vals)
                            lam_m  = max(0.0, min(1.0, float(st.get("parallelism_factor", 0.0))))
                            eff_mult = (1.0 - lam_m) * mean_m + lam_m * min_m
                    else:
                        eff_mult = 0.0
                # else: keep ct_eff_new and eff_mult as set earlier (0.0 and 0.0)

                # Current shift window for line (namedtuple)
                cur_sh = _active_shift_for_line(lid, now_ms)
                if (_tstate[sid]["shift_id"] != cur_sh.id) or (_tstate[sid]["shift_date"] != cur_sh.date):
                    s["shift_id"]       = cur_sh.id
                    s["shift_date"]     = cur_sh.date
                    s["shift_start_ms"] = cur_sh.start_ms
                    s["shift_end_ms"]   = cur_sh.end_ms
                    s["last_shift_base"]= None
                else:
                    s["shift_end_ms"]   = cur_sh.end_ms  # allow schedule edits to update

                # ----- CT segments (seed/change -> buffer; open at increment OR immediately) -----
                if WRITE_CT_SEGMENTS:
                    try:
                        need_new = False
                        if (not s.get("seg_opened", False)) and ((ct_eff_new and ct_eff_new > 0.0) or is_missing_cfg):
                            need_new = True
                        elif s.get("seg_opened", False) and (
                            (ct_eff_new != s["ct_eff"]) or
                            (s.get("overcycle_multiplier") is None or abs(eff_mult - float(s.get("overcycle_multiplier"))) > 0.001)
                        ):
                            need_new = True

                        if need_new:
                            s["pending_seg"] = {
                                "ct_eff_sec": round(ct_eff_new or 0.0, 3),
                                "fixtures_per_side": int(fps_db),
                                "is_turntable": 1 if st["is_turntable"] else 0,
                                "parallelism_factor": round(float(st.get("parallelism_factor", 0.0)), 3),
                                "parts_used": parts_used,
                                "ct_mode": ("missing-config" if is_missing_cfg else ("live-fixtures" if parts_used else "fallback-config")),
                                "poll_ms": now_ms,
                                "overcycle_multiplier": round(float(eff_mult), 3)
                            }

                        pend = s.get("pending_seg")
                        if pend:
                            force_now = bool(s.get("force_open_now"))
                            if pend["ct_mode"] == "missing-config" or force_now:
                                # Open immediately (after config change or missing-config)
                                inc_ts = system.date.now()
                                try:
                                    _nq_update(PPATH + "ctSegmentUpsertOnChange", {
                                        "station_id": sid,
                                        "effective_from_utc": inc_ts,
                                        "ct_eff_sec": pend["ct_eff_sec"],
                                        "fixtures_per_side": pend["fixtures_per_side"],
                                        "is_turntable": pend["is_turntable"],
                                        "parallelism_factor": pend["parallelism_factor"],
                                        "parts_json": system.util.jsonEncode(pend["parts_used"] or []),
                                        "ct_mode": pend["ct_mode"],
                                        "overcycle_multiplier": pend["overcycle_multiplier"]
                                    })
                                    s["seg_opened"] = True
                                    s["pending_seg"] = None
                                    s["overcycle_multiplier"] = pend["overcycle_multiplier"]
                                    s["force_open_now"] = False
                                except Exception as e:
                                    _log_error(MODULE + "::run_targets/ct_upsert_immediate", message=unicode(e))
                            else:
                                # Precise-on-increment anchor path
                                inc_prev = s.get("last_increment_prev")
                                inc_ms   = s.get("last_increment_ms") or 0
                                if (inc_prev is not None) and (inc_ms >= pend["poll_ms"]):
                                    det_tag = _station_total_path(st)  # station-level only
                                    inc_ts = _first_increment_ts(det_tag, inc_prev, pend["poll_ms"], now_ms) or system.date.now()
                                    try:
                                        _nq_update(PPATH + "ctSegmentUpsertOnChange", {
                                            "station_id": sid,
                                            "effective_from_utc": inc_ts,
                                            "ct_eff_sec": pend["ct_eff_sec"],
                                            "fixtures_per_side": pend["fixtures_per_side"],
                                            "is_turntable": pend["is_turntable"],
                                            "parallelism_factor": pend["parallelism_factor"],
                                            "parts_json": system.util.jsonEncode(pend["parts_used"] or []),
                                            "ct_mode": pend["ct_mode"],
                                            "overcycle_multiplier": pend["overcycle_multiplier"]
                                        })
                                        s["seg_opened"] = True
                                        s["pending_seg"] = None
                                        s["overcycle_multiplier"] = pend["overcycle_multiplier"]
                                    except Exception as e:
                                        _log_error(MODULE + "::run_targets/ct_upsert_on_increment", message=unicode(e))
                    except Exception as e:
                        _log_error(MODULE + "::run_targets/ct_segment_logic", message=unicode(e))

                # Commit CT
                s["ct_eff"] = ct_eff_new
                s["last_poll_ms"] = now_ms

                # ---- Hourly BASE (break-aware; LOCAL for breaks, UTC to DB) ----
                if WRITE_HOURLY_BASE_TO_DB and st["is_critical"]:
                    hour_base = 0
                    if s["ct_eff"] and s["ct_eff"] > 0.0:
                        work_sec = int(_hour_working_seconds(s["hour_start_local_ms"], lid))
                        hour_base = int(work_sec / s["ct_eff"]) if work_sec > 0 else 0
                    if (s["last_hour_base"] is None) or (hour_base != s["last_hour_base"]):
                        s["last_hour_base"] = hour_base
                        if hour_base > 0:
                            hourly_rows.append({
                                "station_id":        sid,
                                "line_id":           lid,
                                "hour_start_utc_ms": s["hour_start_utc_ms"],
                                "target_parts_base": int(hour_base)
                            })

                # ---- Shift BASE (break-aware across full shift) ----
                if WRITE_SHIFT_BASE_TO_DB and st["is_critical"] and s["shift_id"] is not None and s["shift_start_ms"] and s["shift_end_ms"]:
                    if s["ct_eff"] and s["ct_eff"] > 0.0:
                        work_sec_shift = _working_ms(s["shift_start_ms"], s["shift_end_ms"], lid) // 1000
                        shift_base = int(work_sec_shift / s["ct_eff"]) if work_sec_shift > 0 else 0
                    else:
                        shift_base = 0
                    if (s["last_shift_base"] is None) or (shift_base != s["last_shift_base"]):
                        s["last_shift_base"] = shift_base
                        shift_rows.append({
                            "station_id":        sid,
                            "shift_id":          s["shift_id"],
                            "shift_local_date":  s["shift_date"],
                            "target_parts_base": int(shift_base)
                        })
            except Exception as e:
                _log_error(MODULE + "::run_targets/station_loop", message=unicode(e))

        # ---- Persist current bases ----
        if WRITE_HOURLY_BASE_TO_DB and hourly_rows:
            try:
                _nq_update(PPATH + "upsertHourlyTargetsBatch",
                           {"payload": system.util.jsonEncode(hourly_rows)})
            except Exception as e:
                _log_error(MODULE + "::run_targets/hourly_upsert", message=unicode(e))
        if WRITE_SHIFT_BASE_TO_DB and shift_rows:
            try:
                _nq_update(PPATH + "upsertShiftTargetsBatch",
                           {"payload": system.util.jsonEncode(shift_rows)})
            except Exception as e:
                _log_error(MODULE + "::run_targets/shift_upsert", message=unicode(e))

        # ---- Backfill any missing bases in the recent window ----
        global _repair_last_run_ms
        now_ms2 = system.date.toMillis(system.date.now())
        if (now_ms2 - _repair_last_run_ms) >= (_REPAIR_PERIOD_SEC * 1000):
            _repair_missing_bases()
            _repair_last_run_ms = now_ms2

        # ---- Persist batches again (dup safeguard kept from original) ----
        if WRITE_HOURLY_BASE_TO_DB and hourly_rows:
            try:
                _nq_update(PPATH + "upsertHourlyTargetsBatch",
                           {"payload": system.util.jsonEncode(hourly_rows)})
            except Exception as e:
                _log_error(MODULE + "::run_targets/hourly_upsert_dup", message=unicode(e))

        if WRITE_SHIFT_BASE_TO_DB and shift_rows:
            try:
                _nq_update(PPATH + "upsertShiftTargetsBatch",
                           {"payload": system.util.jsonEncode(shift_rows)})
            except Exception as e:
                _log_error(MODULE + "::run_targets/shift_upsert_dup", message=unicode(e))

        # (MissingConfiguration tag feature removed)

    except Exception as e:
        # never throw from a timer (but do log)
        _log_error(MODULE + "::run_targets", message=unicode(e))