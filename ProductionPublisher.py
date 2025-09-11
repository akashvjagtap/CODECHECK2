# <summary>
# Module Name : ProductionPublisher
# Description : Publishes hourly, shift, and weekly production payloads to MQTT.
# Author      : Akash J
# Created On  : 2025-06-22
# </summary>

import system
from traceback import format_exc
from MagnaDataOps.LoggerFunctions import log_info as _log_info, log_warn as _log_warn, log_error as _log_error

# -------- local logging helper (consistent with CommonScripts) --------
def _log_exc(where):
    """
    Best-effort logger that never raises.
    Tries Magnolia logger first; falls back to system logger if that fails.
    """
    msg = format_exc()
    try:
        _log_error("ProductionPublisher::%s" % where, message=msg)
        return
    except Exception as e1:
        try:
            system.util.getLogger("ProductionPublisher").error(
                u"Logging failed in %s: %s\n%s" % (where, unicode(e1), unicode(msg))
            )
        except Exception:
            # final fallback: do nothing, but no bare 'pass'
            return

# Optional: Live snapshots from Targets module
try:
    from MagnaDataOps import ProductionTargetsLive as PT
except Exception:
    PT = None

# -------- Config / constants --------
_NQ_BASE = "MagnaDataOps/Dashboard/AllProductionData/DataPublish/"

_DEFAULT_SERVER_NAME = "Local Broker"
_BROKER_TAG_PATH     = "[MagnaDataOps]BrokerName"
_BROKER_TTL_SEC      = 60.0
_ISO_FMT             = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

# Windows
_HOURLY_PUBLISH_LOOKBACK_HRS = 6     # current/open hours window
_HOURLY_CATCHUP_CLOSED_HRS   = 48    # closed+unpublished catch-up window
_BREAKS_REFRESH_SEC          = 120

# Named Queries under _NQ_BASE
_NQ_HIER                  = "getHierarchyForStations"
_NQ_GET_HOURLY_TO_PUBLISH = "getHourlyRowsToPublish"
_NQ_GET_SHIFT_TO_PUBLISH  = "getEndedShiftRowsToPublish"
_NQ_GET_WEEKLY_TO_PUBLISH = "getWeeklyRowsToPublish"
_NQ_MARK_HOURLY_PUBLISHED = "markHourlyPublished"
_NQ_MARK_SHIFT_PUBLISHED  = "markShiftPublished"
_NQ_MARK_WEEKLY_PUBLISHED = "markWeeklyPublished"
_NQ_GET_BREAKS            = "getBreaksOnDate" 

# -------- Small caches --------
_hier         = {}     # sid -> names/ids
_hier_loaded  = set()
_broker_cache = {"t":0.0, "v":_DEFAULT_SERVER_NAME}
_breaks       = {"last_load": 0, "today": None, "yday": None, "by_line": {}}

# -------- Utils --------
def _u(x):
    try:
        return unicode(x)
    except Exception:
        return unicode(str(x) if x is not None else u"")

def _get_version():
    try:
        import MagnaDataOps.CommonScripts as CS
        v = getattr(CS, "payload_version", None)
        return _u(v or "1.0.0")
    except Exception:
        _log_exc("_get_version")
        return u"1.0.0"

def _nq(name, params=None):
    try:
        return system.db.runNamedQuery(_NQ_BASE + name, params or {})
    except Exception:
        _log_exc("_nq:%s" % name)
        return []

def _to_csv(ids):
    return ",".join(str(int(x)) for x in ids)

def _iso_now():
    try:
        return system.date.format(system.date.now(), _ISO_FMT)
    except Exception:
        _log_exc("_iso_now")
        return system.date.format(system.date.now(), "yyyy-MM-dd HH:mm:ss.SSS")

def _rowdicts(ds):
    if not ds:
        return []
    try:
        if isinstance(ds, list):
            return ds
    except Exception:
        # not a native list — fall through
        pass
    out=[]
    try:
        cols=list(ds.getColumnNames())
        for i in range(ds.getRowCount()):
            d={}
            for c in cols:
                try:
                    d[c]=ds.getValueAt(i,c)
                except Exception:
                    _log_exc("_rowdicts:getValueAt")
                    d[c]=None
            out.append(d)
    except Exception:
        _log_exc("_rowdicts:iter_generic")
        for r in ds:
            try:
                out.append(dict(r))
            except Exception:
                _log_exc("_rowdicts:dict(r)")
                out.append({})
    return out

def _fmt_date_local(d):   # "yyyy-MM-dd"
    try:
        return system.date.format(d, "yyyy-MM-dd")
    except Exception:
        _log_exc("_fmt_date_local")
        s=_u(d); return s.split("T",1)[0].split(" ",1)[0]

def _fmt_hour_HH(d):      # "HH:00"
    try:
        return system.date.format(d, "HH")+":00"
    except Exception:
        _log_exc("_fmt_hour_HH")
        return "00:00"

def _hour_bucket_local(d):
    try:
        return int(system.date.format(d,"H"))
    except Exception:
        _log_exc("_hour_bucket_local")
        return 0

def _as_int_bool(v):
    try:
        # handles 1/0, True/False, java.lang.Boolean
        return 1 if (v is True or int(v)==1) else 0
    except Exception:
        return 0

def _now_s():
    try:
        from time import time; return time()
    except Exception:
        _log_exc("_now_s")
        return 0.0

def _datestr(d): 
    try:
        return system.date.format(d, "yyyy-MM-dd")
    except Exception:
        _log_exc("_datestr")
        return "1970-01-01"

def _load_breaks_if_needed():
    now = system.date.now()
    now_ms = system.date.toMillis(now)
    today = _datestr(now)
    yday  = _datestr(system.date.addDays(now, -1))
    if (_breaks["by_line"] and
        (now_ms - _breaks["last_load"] < _BREAKS_REFRESH_SEC*1000) and
        _breaks["today"] == today and _breaks["yday"] == yday):
        return

    by_line = {}
    for day in (yday, today):
        try:
            ds = _nq(_NQ_GET_BREAKS, {"shift_date": day})
        except Exception:
            _log_exc("_load_breaks_if_needed:query")
            ds = []
        for r in _rowdicts(ds):
            try:
                if not int(r.get("is_active") or 0):  # skip inactive
                    continue
                lid = int(r["line_id"])
                bstart = r["break_start_time"]; bend = r["break_end_time"]
                if not bstart or not bend: 
                    continue
                s = system.date.toMillis(bstart); e = system.date.toMillis(bend)
                if e <= s: 
                    continue
                by_line.setdefault(lid, []).append((s, e))
            except Exception:
                _log_exc("_load_breaks_if_needed:row")

    # merge overlaps
    for lid, spans in by_line.items():
        spans.sort()
        merged = []
        for s, e in spans:
            if not merged or s > merged[-1][1]:
                merged.append([s, e])
            else:
                merged[-1][1] = max(merged[-1][1], e)
        by_line[lid] = [(a, b) for (a, b) in merged]

    _breaks["by_line"]  = by_line
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
    
def _get_broker_name():
    now = _now_s()
    if (now - (_broker_cache.get("t") or 0.0)) < _BROKER_TTL_SEC and _broker_cache.get("v"):
        return _broker_cache["v"]
    try:
        res = system.tag.readBlocking([_BROKER_TAG_PATH])[0]
        tag_val = (u"%s" % getattr(res, "value", u"")).strip()
        chosen = tag_val if tag_val else _DEFAULT_SERVER_NAME
        _broker_cache["v"] = chosen
        _broker_cache["t"] = now
        return chosen
    except Exception:
        _log_exc("_get_broker_name:read")
        _broker_cache["t"] = now
        _broker_cache["v"] = _DEFAULT_SERVER_NAME
        return _DEFAULT_SERVER_NAME

def _publish(topic,obj,qos=0,retain=False,server_name=None):
    try:
        server=server_name or _get_broker_name()
        payload=system.util.jsonEncode(obj).encode("utf-8")
        system.cirruslink.engine.publish(server,topic,payload,int(qos),bool(retain))
        return True
    except Exception:
        _log_exc("_publish")
        return False

# -------- Hierarchy + topics --------
def _load_hierarchy_for(station_ids):
    need=[int(s) for s in station_ids if int(s) not in _hier_loaded]
    if not need: 
        return
    try:
        ds=_nq(_NQ_HIER,{"station_ids_csv":_to_csv(need)})
        for d in _rowdicts(ds):
            try:
                sid=int(d.get("station_id"))
                _hier[sid]={
                  "station_name":  _u(d.get("station_name_clean") or d.get("station_name") or ""),
                  "line_id":       int(d.get("line_id") or 0),
                  "line_name":     _u(d.get("line_name_clean") or d.get("line_name") or ""),
                  "subarea_name":  _u(d.get("subarea_name_clean") or d.get("subarea_name") or ""),
                  "area_name":     _u(d.get("area_name_clean") or d.get("area_name") or ""),
                  "plant_name":    _u(d.get("location_clean") or d.get("plant_name") or ""),
                  "division_name": _u(d.get("division_name_clean") or d.get("division_name") or "")
                }
                _hier_loaded.add(sid)
            except Exception:
                _log_exc("_load_hierarchy_for:row")
    except Exception:
        _log_exc("_load_hierarchy_for:query")

def _san(s):
    return (s or u"").replace(" ", "")

def _topic_for(sid, scope_slug):
    h = _hier.get(int(sid), {})
    div   = _san(h.get("division_name") or "NA")
    plant = _san(h.get("plant_name")   or "Plant")
    area  = _san(h.get("area_name")    or "Area")
    sub   = _san(h.get("subarea_name") or "SubArea")
    line  = _san(h.get("line_name")    or "Line")
    return "m/%s/%s/%s/%s/line/%s/%s" % (div, plant, area, sub, line, scope_slug)

# -------- Live snapshot (optional) --------
def _live_targets_snapshot():
    try:
        if PT and hasattr(PT,"safe_copy_live_snapshots"):
            return PT.safe_copy_live_snapshots() or {}
    except Exception:
        _log_exc("_live_targets_snapshot")
    return {}

# -------- NQ param helpers --------
def _params_hourly():
    return {"lookback_hours": int(_HOURLY_PUBLISH_LOOKBACK_HRS),
            "catchup_hours":  int(_HOURLY_CATCHUP_CLOSED_HRS)}

def _params_shift_days():
    today=system.date.format(system.date.now(),"yyyy-MM-dd")
    yday =system.date.format(system.date.addDays(system.date.now(),-1),"yyyy-MM-dd")
    return {"day0_local":today,"day1_local":yday}

def _params_weekly_now():
    return {"now_local": system.date.now()}

# -------- Per-scope publishers --------
def publish_hourly_rows(rows, live_snap_unused=None, qos=0, retain=False):
    rows = _rowdicts(rows)
    if not rows: 
        return 0

    _load_hierarchy_for([int(r["station_id"]) for r in rows])
    _load_breaks_if_needed()

    ver    = _get_version()
    sent   = 0
    now    = system.date.now()
    now_ms = system.date.toMillis(now)

    for r in rows:
        try:
            sid   = int(r["station_id"])
            lid   = int(r.get("line_id") or (_hier.get(sid, {}).get("line_id") or 0))
            hloc  = r.get("hour_local") or r.get("hour_start_utc")
            if not hloc: 
                continue

            prod_date = _fmt_date_local(hloc) + "T00:00:00"
            prod_hour = _fmt_hour_HH(hloc)
            bucket_id = int(r.get("bucket_id") if r.get("bucket_id") is not None else _hour_bucket_local(hloc))

            actual    = int(r.get("total_parts") or 0)
            tgt_base  = int(r.get("target_parts_base") or 0)
            is_closed = _as_int_bool(r.get("is_closed"))
            is_pub    = _as_int_bool(r.get("is_published"))

            # --- break-aware LiveTarget ---
            hour_ms   = system.date.toMillis(hloc)
            hour_end  = hour_ms + 3600*1000
            if is_closed or tgt_base <= 0:
                tgt_live = 0
            else:
                # total working seconds in hour
                work_total_sec = _working_ms(hour_ms, hour_end, lid) // 1000
                if work_total_sec <= 0:
                    tgt_live = 0
                else:
                    # elapsed working seconds so far in hour
                    now_cap = min(now_ms, hour_end)
                    work_elapsed_sec = _working_ms(hour_ms, now_cap, lid) // 1000
                    frac = min(1.0, max(0.0, float(work_elapsed_sec) / float(work_total_sec)))
                    tgt_live = int(tgt_base * frac)  # floor

            payload = {
              "Version":   ver,
              "Timestamp": _iso_now(),
              "HourlyProduction": {
                "ProductionDate": prod_date,
                "ProductionHour": prod_hour,
                "Actual":         actual,
                "HourlyTarget":   tgt_base,
                "LiveTarget":     tgt_live,
                "BucketID":       bucket_id
              }
            }

            _publish(_topic_for(sid, "HourlyProduction"), payload, qos=qos, retain=retain)
            sent += 1

            # mark only closed+unpublished hours
            if is_closed and (is_pub == 0):
                try:
                    _nq(_NQ_MARK_HOURLY_PUBLISHED, {
                        "station_id": sid,
                        "hour_start_utc": r.get("hour_start_utc")
                    })
                except Exception:
                    _log_exc("publish_hourly_rows::mark_published")
        except Exception:
            _log_exc("publish_hourly_rows:row")
            continue

    return sent

def publish_shift_rows(rows, live_snap_unused=None, qos=0, retain=False):
    rows = _rowdicts(rows)
    if not rows:
        return 0

    _load_hierarchy_for([int(r["station_id"]) for r in rows])
    _load_breaks_if_needed()

    ver    = _get_version()
    sent   = 0
    now    = system.date.now()
    now_ms = system.date.toMillis(now)

    for r in rows:
        try:
            sid = int(r["station_id"])
            lid = int(r.get("line_id") or (_hier.get(sid, {}).get("line_id") or 0))

            # required fields from NQ
            sdate   = r.get("shift_local_date")
            s_start = r.get("shift_start_local")
            s_end   = r.get("shift_end_local")
            if not s_start or not s_end:
                continue

            start_ms = system.date.toMillis(s_start)
            end_ms   = system.date.toMillis(s_end)
            is_ended = 1 if (end_ms <= now_ms) else 0
            is_pub   = _as_int_bool(r.get("is_published"))

            actual   = int(r.get("total_parts") or 0)
            tgt_base = int(r.get("target_parts_base") or 0)

            # ---- break-aware LiveTarget for the shift ----
            if is_ended or tgt_base <= 0:
                tgt_live = 0
            else:
                work_total_sec = _working_ms(start_ms, end_ms, lid) // 1000
                cap_ms         = min(now_ms, end_ms)
                work_elapsed   = _working_ms(start_ms, cap_ms, lid) // 1000
                frac           = (float(work_elapsed) / float(work_total_sec)) if work_total_sec > 0 else 0.0
                if frac < 0.0: frac = 0.0
                if frac > 1.0: frac = 1.0
                tgt_live       = int(tgt_base * frac)

            # ---- stable BucketID ----
            anchor_ms = min(now_ms, end_ms) - 1000  # minus 1s so HH:00 points to previous hour
            if anchor_ms < start_ms:
                anchor_ms = start_ms
            bucket_id = int(system.date.format(system.date.fromMillis(anchor_ms), "H"))

            payload = {
              "Version":   ver,
              "Timestamp": _iso_now(),
              "ShiftProduction": {
                "ProductionDate": _fmt_date_local(sdate) + "T00:00:00",
                "Actual":          actual,
                "ProductionTarget":tgt_base,
                "LiveTarget":      tgt_live,
                "BucketID":        bucket_id
              }
            }

            _publish(_topic_for(sid, "ShiftProduction"), payload, qos=qos, retain=retain)
            sent += 1

            # mark only after the final close-out publish
            if is_ended and (is_pub == 0):
                try:
                    _nq(_NQ_MARK_SHIFT_PUBLISHED, {
                        "station_id": sid,
                        "shift_id":   int(r["shift_id"]),
                        "shift_local_date": sdate
                    })
                except Exception:
                    _log_exc("publish_shift_rows::mark_published")
        except Exception:
            _log_exc("publish_shift_rows:row")
            continue

    return sent

def publish_weekly_rows(rows, qos=0, retain=False):
    rows=_rowdicts(rows)
    if not rows: 
        return
    _load_hierarchy_for([int(r["station_id"]) for r in rows])
    ver=_get_version()

    for r in rows:
        try:
            sid    = int(r["station_id"])
            actual = int(r.get("total_parts") or 0)
            payload = {
              "Version":   ver,
              "Timestamp": _iso_now(),
              "ProductionWeekly": {
                "Stn_ID": (_hier.get(sid) or {}).get("station_name") or ("Station_%d" % sid),
                "Value":  actual
              }
            }
            try:
                _publish(_topic_for(sid,"ProductionWeekly"), payload, qos=qos, retain=retain)
            except Exception:
                _log_exc("publish_weekly_rows:_publish")

            try:
                _nq(_NQ_MARK_WEEKLY_PUBLISHED, {
                    "station_id": sid,
                    "week_start_local": r["week_start_local"]
                })
            except Exception:
                _log_exc("publish_weekly_rows::mark_published")
        except Exception:
            _log_exc("publish_weekly_rows:row")
            continue

# -------- Orchestrator --------
def publish_pending():
    snap = _live_targets_snapshot()

    # Hourly (current + closed/unpublished)
    try:
        ds_h = _nq(_NQ_GET_HOURLY_TO_PUBLISH, _params_hourly())
        if ds_h and (ds_h.getRowCount()>0):
            publish_hourly_rows(ds_h, snap)
    except Exception:
        _log_exc("publish_pending::hourly")

    # Shifts
    try:
        ds_s = _nq(_NQ_GET_SHIFT_TO_PUBLISH, _params_shift_days())
        if ds_s and (ds_s.getRowCount()>0):
            publish_shift_rows(ds_s, snap)
    except Exception:
        _log_exc("publish_pending::shifts")

    # Weekly
    try:
        ds_w = _nq(_NQ_GET_WEEKLY_TO_PUBLISH, _params_weekly_now())
        if ds_w and (ds_w.getRowCount()>0):
            publish_weekly_rows(ds_w)
    except Exception:
        _log_exc("publish_pending::weekly")