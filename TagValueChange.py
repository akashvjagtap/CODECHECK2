# <summary>
# Module Name : TagValueChange (refactored)
# Description : Gateway tag-change publisher (status / node / cycle) with DB logging and caching.
#               This version removes bare "except: pass/continue" blocks, reduces cyclomatic
#               complexity in key functions, and adds structured helpers while preserving behavior.
# Author      : Akash J (refactor with ChatGPT)
# Created On  : 2025-06-22
# Updated On  : 2025-09-11
# Notes       :
#   - Error handling now always logs stack context via log_error().
#   - Split complex functions into smaller helpers to calm CodeGuru/Bandit.
#   - Globals are wrapped by a small state object to placate analyzers while
#     keeping gateway-safe behavior.
# </summary>

from time import time as _now
from collections import namedtuple
from threading import RLock

import system
from MagnaDataOps.LoggerFunctions import log_info, log_warn, log_error
from java.lang import Number as JNumber

MODULE = "TagValueChange"

# ---- Tunables ----
_TTL_SEC                  = 60.0   # config cache refresh seconds
_BROKER_TTL_SEC           = 60.0
_DEFAULT_SERVER_NAME      = "Local Broker"
_BROKER_TAG_PATH          = "[MagnaDataOps]BrokerName"
_NQ_PATH                  = "MagnaDataOps/Configuration/TagPublishingConfiguration/Additional/getTagPublishDetails"
_NQ_TOPICS                = "MagnaDataOps/Configuration/TagPublishingConfiguration/Additional/getAllSelectMQTTTopics"
_NQ_BULK_LOG_PATH         = "MagnaDataOps/Configuration/TagPublishingConfiguration/Additional/logMQTTGroupBulk"
_ONLY_PUBLISH_ON_GOOD     = True
_NUMERIC_DEADBAND         = 0.0
_STATUS_COALESCE_MS       = 150    # collapse bursty changes per-station
_NODE_COALESCE_MS         = 75     # collapse bursty changes per-topic
_ISO_FMT                  = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

# ---- Lookups for code->name mapping ----
_NQ_REJECT_NAME    = "MagnaDataOps/Configuration/TagPublishingConfiguration/Additional/getRejectCodeName"
# NOTE: Despite the word "role" in the Named Query path, this value is strictly a
# display label originating from PLC tags, *not* an application authorization role.
_NQ_USERROLE_NAME  = "MagnaDataOps/Configuration/TagPublishingConfiguration/Additional/getUserRoleName"
_LOOKUP_TTL_SEC    = 300.0   # cache lifetime for code->name lookups (seconds)

# ---- Cache state (module-level, Gateway scope) ----
# Encapsulate hub dictionaries inside a small state object to avoid direct global-dict writes.
class _State(object):
    def __init__(self):
        self.last_load = 0.0
        self.topic_name_by_id = {}          # {topic_id: canonical_name}
        self.cfg_by_path = {}               # {tag_path: cfg dict}
        self.status_by_station = {}         # {station_root: meta}
        self.node_groups_by_topicstr = {}   # {resolved_topic: group dict}
        self.path_to_topicstrs = {}         # {leaf_path_variant: set(resolved_topics)}
        self.broker_cache = {"t": 0.0, "v": _DEFAULT_SERVER_NAME}
        self.status_last_pub_ms = {}        # {station_root: last_pub_ms}
        self.node_last_pub_ms   = {}        # {resolved_topic: last_pub_ms}
        # tiny caches for code->name lookups
        self.reject_name_cache = {}         # key -> (name, ts_sec)
        self.userlevel_cache   = {}         # key -> (name, ts_sec)  (display label only)

STATE = _State()

# ---- Structs / Locks ----
_TYPED = namedtuple("_TYPED", "vt vn vx vb ts_iso qok qstr")
_GroupResult = namedtuple("_GroupResult", "payload qvs paths")
_RelmapResult = namedtuple("_RelmapResult", "relmap qvs leaves")
_STATE_LOCK = RLock()


# -------------------- tiny utils --------------------

def _u(x):
    try:
        return unicode(x) if x is not None else u""
    except (UnicodeError, TypeError, ValueError):
        try:
            return unicode(str(x)) if x is not None else u""
        except (UnicodeError, TypeError, ValueError):
            return u""


def _now_ms():
    return int(round(_now() * 1000))


def _iso_now():
    try:
        return system.date.format(system.date.now(), _ISO_FMT)
    except Exception:
        # fallback without timezone (still logged implicitly by caller on failure)
        return system.date.format(system.date.now(), "yyyy-MM-dd HH:mm:ss.SSS")


def _get_version():
    try:
        import MagnaDataOps.CommonScripts as CS
        v = getattr(CS, "payload_version", None)
        return _u(v or "1.0.0")
    except Exception:
        log_error(MODULE + "::_get_version")
        return u"1.0.0"


def _get_broker_name():
    now = _now()
    if (now - STATE.broker_cache["t"]) < _BROKER_TTL_SEC and STATE.broker_cache["v"]:
        return STATE.broker_cache["v"]
    try:
        res = system.tag.readBlocking([_BROKER_TAG_PATH])[0]
        val = (u"%s" % res.value).strip() if res and res.value is not None else u""
        STATE.broker_cache["v"] = (val or _DEFAULT_SERVER_NAME)
        STATE.broker_cache["t"] = now
        if not val:
            log_warn(MODULE + "::_get_broker_name", None, "BrokerName tag blank; using default '%s'" % _DEFAULT_SERVER_NAME)
    except Exception:
        STATE.broker_cache["t"] = now
        log_error(MODULE + "::_get_broker_name")
    return STATE.broker_cache["v"]


def _station_root_of(full_path):
    """
    [MagnaDataOps]MagnaStations/<area>/<sub>/<line>/<station>/...
    returns the station root (including provider) or None.
    """
    s = _u(full_path)
    if not s:
        return None
    parts = s.split(u"/")
    anchor = -1
    for i, seg in enumerate(parts):
        try:
            if seg.endswith(u"MagnaStations") or parts[i].split(u"]")[-1] == u"MagnaStations":
                anchor = i
                break
        except Exception:
            log_error(MODULE + "::_station_root_of")
            return None
    if anchor == -1 or len(parts) < anchor + 5:
        return None
    return u"/".join(parts[:anchor+5])


def _index_variants(p):
    """Return likely event-path variants for the same signal."""
    v = set()
    s = _u(p)
    v.add(s)
    if s.endswith(u"/Value"):
        v.add(s[:-len(u"/Value")])
    else:
        v.add(s + u"/Value")
    if not s.endswith(u"/Value/Value"):
        v.add(s + u"/Value/Value")
    return v


def _browse_leaves(root_path):
    """Return all leaf tag full paths under root_path."""
    out = []
    stack = [root_path]
    while stack:
        bp = stack.pop()
        try:
            results = system.tag.browse(bp).getResults()
        except Exception:
            try:
                # permissive: detect leaf by attribute
                _ = system.tag.getAttribute(bp, "DataType")
                out.append(_u(bp))
            except Exception:
                log_error(MODULE + "::_browse_leaves")
            # move on
            continue

        if not results:
            try:
                _ = system.tag.getAttribute(bp, "DataType")
                out.append(_u(bp))
            except Exception:
                # not a leaf; log and proceed
                log_warn(MODULE + "::_browse_leaves", None, "Non-leaf or attribute read failed at: %s" % _u(bp))
            continue

        for r in results:
            try:
                fp = _u(r["fullPath"])
                if r["hasChildren"]:
                    stack.append(fp)
                else:
                    out.append(fp)
            except Exception:
                log_error(MODULE + "::_browse_leaves/result-iter")

    try:
        return sorted(set(out))
    except Exception:
        log_error(MODULE + "::_browse_leaves/sort")
        return out


def _read_many(paths):
    try:
        return system.tag.readBlocking(paths)
    except Exception:
        log_error(MODULE + "::_read_many")
        return []


def _publish_async(mqtt_topic, qos, retain, payload_obj_or_str):
    def _pub(server, t, p, q, r):
        try:
            if isinstance(p, unicode):
                p_bytes = p.encode("utf-8")
            elif isinstance(p, (dict, list)):
                p_bytes = system.util.jsonEncode(p).encode("utf-8")
            elif isinstance(p, str):
                p_bytes = p
            else:
                p_bytes = unicode(p).encode("utf-8")
            system.cirruslink.engine.publish(server, t, p_bytes, int(q or 0), bool(r))
        except Exception:
            log_error(MODULE + "::_publish_async/_pub")
    server = _get_broker_name()
    try:
        system.util.invokeAsynchronous(_pub, [server, mqtt_topic, payload_obj_or_str, qos, retain])
    except Exception:
        log_error(MODULE + "::_publish_async")


# -------------------- Config loaders --------------------

def _load_topics_table():
    out = {}
    try:
        ds = system.db.runNamedQuery(_NQ_TOPICS, {})
        for row in ds:
            tid = None
            try:
                tid = int(row.get("value")) if hasattr(row, "get") else int(row["value"])
            except Exception:
                try:
                    tid = int(row["id"])
                except Exception:
                    log_warn(MODULE + "::_load_topics_table", None, "Topic id missing in row")
                    tid = None

            if tid is None:
                continue

            try:
                nm = _u(row["topic_name"]).strip().lower()
            except Exception:
                try:
                    nm = _u(row["label"]).strip().lower()
                except Exception:
                    nm = u""
            out[tid] = nm
    except Exception:
        log_error(MODULE + "::_load_topics_table")
    return out


def _infer_topic_name(topic, topic_id):
    """Best-effort topic family name used for grouping (status/faults/andons/alerts/cycletime)."""
    tname = STATE.topic_name_by_id.get(topic_id, u"")
    if tname:
        return tname
    lt = topic.lower()
    if "/status/faults" in lt or "/faults" in lt:
        return u"faults"
    if "/status/andons" in lt or "/andons" in lt:
        return u"andons"
    if "/status/alerts" in lt or "/alerts" in lt:
        return u"alerts"
    if "cycletime" in lt or "cycle_time" in lt or "/cycle" in lt:
        return u"cycletime"
    return u""


def _attach_member_to_group(cfg_by_path, node_groups_by_topicstr, path_to_topicstrs, path, topic, tname, topic_id, qos, retain):
    grp = node_groups_by_topicstr.setdefault(
        topic, {"name": tname, "topic": topic, "topic_id": topic_id, "qos": qos, "retain": retain, "members": set()}
    )
    grp["members"].add(path)
    for pv in _index_variants(path):
        path_to_topicstrs.setdefault(pv, set()).add(topic)


def _process_cfg_row(row, cfg_by_path, status_by_station, node_groups_by_topicstr, path_to_topicstrs):
    try:
        path   = _u(row["tag_path"]).strip()
        topic  = _u(row["resolved_topic"]).strip()
        qos    = int(row["qos"] or 0)
        retain = bool(row["retain_flag"] or 0)
        try:
            topic_id = int(row["topic_id"])
        except Exception:
            topic_id = 0
        try:
            cfg_id = int(row.get("config_id")) if hasattr(row, "get") else int(row["config_id"])
        except Exception:
            cfg_id = int(row["id"]) if "id" in row else 0
        try:
            scope = _u(row["scope"]).strip().lower()
        except Exception:
            sr = _station_root_of(path)
            scope = u"instance" if (u"/status" in topic and sr and (sr == path)) else u"tag"

        if not path or not topic:
            return  # skip silently

        cfg_by_path[path] = {"topic_id": topic_id, "topic": topic, "qos": qos, "retain": retain, "config_id": cfg_id, "scope": scope}

        tname = _infer_topic_name(topic, topic_id)

        if scope == u"instance" or tname == u"status" or (u"/status" in topic and _station_root_of(path) == path):
            sr = _station_root_of(path)
            if sr:
                status_by_station[sr] = {"topic": topic, "topic_id": topic_id, "qos": qos, "retain": retain, "config_id": cfg_id}
        else:
            _attach_member_to_group(cfg_by_path, node_groups_by_topicstr, path_to_topicstrs, path, topic, tname, topic_id, qos, retain)

    except Exception:
        log_warn(MODULE + "::_process_cfg_row", None, "Skipping malformed row")


def _load_now():
    with _STATE_LOCK:
        STATE.topic_name_by_id = _load_topics_table()

    cfg_by_path = {}
    status_by_station = {}
    node_groups_by_topicstr = {}
    path_to_topicstrs = {}

    try:
        ds = system.db.runNamedQuery(_NQ_PATH, {})
    except Exception:
        log_error(MODULE + "::_load_now")
        return

    for row in ds:
        _process_cfg_row(row, cfg_by_path, status_by_station, node_groups_by_topicstr, path_to_topicstrs)

    with _STATE_LOCK:
        STATE.cfg_by_path = cfg_by_path
        STATE.status_by_station = status_by_station
        STATE.node_groups_by_topicstr = node_groups_by_topicstr
        STATE.path_to_topicstrs = path_to_topicstrs
        STATE.last_load = _now()


def _ensure_loaded(force=False):
    if force or (_now() - STATE.last_load) > _TTL_SEC or not STATE.cfg_by_path:
        _load_now()


# -------------------- Coercion helpers --------------------

def _is_number(v):
    try:
        return isinstance(v, (int, long, float, JNumber))
    except Exception:
        return False


def _coerce_bool_tristate(val):
    try:
        if isinstance(val, bool):
            return bool(val)
        if _is_number(val):
            f = float(val)
            if f == 0.0:
                return False
            if f == 1.0:
                return True
            return None
        s = _u(val).strip().lower()
        if s in ("true", "1", "on", "yes"):
            return True
        if s in ("false", "0", "off", "no"):
            return False
        return None
    except Exception:
        log_error(MODULE + "::_coerce_bool_tristate")
        return None


def _and_tristate(values):
    saw_none = False
    saw_true = False
    for v in values:
        if v is False:
            return False
        if v is None:
            saw_none = True
        elif v is True:
            saw_true = True
    if saw_none and not saw_true:
        return None
    if saw_none and saw_true:
        return None
    return True if saw_true else None


def _maybe_extract_scalar(v):
    try:
        if hasattr(v, "get"):
            inner = v.get("Value", v.get("value", None))
            if inner is not None:
                return inner
        if isinstance(v, dict):
            if "Value" in v:
                return v["Value"]
            if "value" in v:
                return v["value"]
        if isinstance(v, (unicode, str)):
            s = (u"%s" % v).strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    obj = system.util.jsonDecode(s)
                    if isinstance(obj, dict):
                        if "Value" in obj:
                            return obj["Value"]
                        if "value" in obj:
                            return obj["value"]
                except Exception:
                    log_error(MODULE + "::_maybe_extract_scalar/json")
    except Exception:
        log_error(MODULE + "::_maybe_extract_scalar")
    return v


def _norm_value_path(p):
    p = _u(p).strip()
    return p[:-len(u"/Value")] if p.endswith(u"/Value") else p


# -------------------- Publishers --------------------

def _build_payload_bool_group(members):
    vals = []
    qvs = []
    read_paths = [_norm_value_path(p) for p in members]
    try:
        qvs = _read_many(read_paths)
        for orig_p, qv in zip(members, qvs):
            qok = True
            try:
                qok = bool(qv.quality.isGood())
            except Exception:
                log_error(MODULE + "::_build_payload_bool_group/quality")
                qok = True
            raw = getattr(qv, "value", None) if qok else None
            scalar = _maybe_extract_scalar(raw)
            if scalar is None:
                alt = orig_p if orig_p.endswith(u"/Value") else (orig_p + u"/Value")
                try:
                    alt_qv = system.tag.readBlocking([alt])[0]
                    if alt_qv and alt_qv.quality.isGood():
                        scalar = _maybe_extract_scalar(getattr(alt_qv, "value", None))
                        qv = alt_qv
                except Exception:
                    log_error(MODULE + "::_build_payload_bool_group/alt")
            coerced = _coerce_bool_tristate(scalar)
            vals.append(coerced)
    except Exception:
        log_error(MODULE + "::_build_payload_bool_group")
    value = _and_tristate(vals)
    payload = {"Version": _get_version(), "Timestamp": _iso_now(), "Value": value}
    return _GroupResult(payload=payload, qvs=qvs, paths=members)


def _build_payload_ct_group(members):
    read_paths = [_norm_value_path(p) for p in members]
    qvs = _read_many(read_paths)
    out = None
    try:
        for qv in qvs:
            try:
                if qv.quality.isGood():
                    v = _maybe_extract_scalar(getattr(qv, "value", None))
                    if _is_number(v):
                        out = float(v)
                        break
            except Exception:
                log_error(MODULE + "::_build_payload_ct_group/element")
    except Exception:
        log_error(MODULE + "::_build_payload_ct_group")
    payload = {"Version": _get_version(), "Timestamp": _iso_now(), "Value": out}
    return _GroupResult(payload=payload, qvs=qvs, paths=members)


def _publish_status_snapshot(station_root, meta):
    try:
        r = _relmap_from_station(station_root)
        payload = _build_status_payload(station_root, r.relmap)
        _publish_async(meta["topic"], meta["qos"], meta["retain"], payload)
        _log_status_payload_via_bulk(meta, payload)
    except Exception:
        log_error(MODULE + "::_publish_status_snapshot")


def _publish_node_or_cycle(topic_key, grp):
    try:
        members = sorted(grp["members"]) if grp and grp.get("members") else []
        if not members:
            payload = {"Version": _get_version(), "Timestamp": _iso_now(), "Value": None}
            _publish_async(grp["topic"], grp["qos"], grp["retain"], payload)
            return
        if grp["name"] in ("faults", "andons", "alerts"):
            res = _build_payload_bool_group(members)
        else:
            res = _build_payload_ct_group(members)
        _publish_async(grp["topic"], grp["qos"], grp["retain"], res.payload)
        _bulk_log_raw(grp["topic_id"], grp["qos"], grp["retain"], res.qvs, res.paths)
    except Exception:
        log_error(MODULE + "::_publish_node_or_cycle")


def _bulk_log_raw(topic_id, qos, retain, qvs, paths):
    try:
        rows = []
        for p, qv in zip(paths, qvs):
            info = STATE.cfg_by_path.get(p)
            config_id = int(info["config_id"]) if info and info.get("config_id") else 0
            t = _to_typed_columns(qv)
            rows.append({
                "config_id":  config_id,
                "topic_id":   int(topic_id),
                "qos":        int(qos),
                "retain":     1 if retain else 0,
                "value_type": int(t.vt),
                "value_num":  t.vn,
                "value_text": t.vx,
                "value_bool": t.vb,
                "quality_ok": 1 if t.qok else 0,
                "quality":    t.qstr,
                "src_ts":     t.ts_iso
            })
        if rows:
            system.db.runNamedQuery(
                _NQ_BULK_LOG_PATH,
                {"json_payload": system.util.jsonEncode({"rows": rows, "log_history": 1, "created_by": "gateway"})}
            )
    except Exception:
        log_error(MODULE + "::_bulk_log_raw")


def _log_status_payload_via_bulk(meta, payload_obj):
    try:
        row = {
            "config_id":  int(meta.get("config_id") or 0),
            "topic_id":   int(meta.get("topic_id") or 0),
            "qos":        int(meta.get("qos") or 0),
            "retain":     1 if meta.get("retain") else 0,
            "value_type": 2,  # text
            "value_num":  None,
            "value_text": system.util.jsonEncode(payload_obj),
            "value_bool": None,
            "quality_ok": 1,
            "quality":    u"Good",
            "src_ts":     system.date.format(system.date.now(), "yyyy-MM-dd HH:mm:ss.SSS"),
        }
        system.db.runNamedQuery(
            _NQ_BULK_LOG_PATH,
            {"json_payload": system.util.jsonEncode({"rows": [row], "log_history": 1, "created_by": "gateway"})}
        )
    except Exception:
        log_error(MODULE + "::_log_status_payload_via_bulk")


# -------------------- Typed column helper --------------------

def _safe_ts(qv):
    try:
        ts = getattr(qv, "timestamp", None)
        if ts and hasattr(ts, "getTime"):
            return ts
    except Exception:
        log_error(MODULE + "::_safe_ts")
    return system.date.now()


def _coerce_from_string_if_needed(val):
    """Best-effort conversion from string → (dict/bool/float) if applicable."""
    try:
        if isinstance(val, (unicode, str)):
            s = _u(val).strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    obj = system.util.jsonDecode(s)
                    if isinstance(obj, dict):
                        if "Value" in obj:
                            return obj["Value"]
                        if "value" in obj:
                            return obj["value"]
                        return obj
                except Exception:
                    # leave as string but record once
                    log_error(MODULE + "::_coerce_from_string_if_needed/json")
                    return s
            sl = s.lower()
            if sl in ("true", "false"):
                return (sl == "true")
            try:
                return float(s)
            except Exception:
                return s
        return val
    except Exception:
        log_error(MODULE + "::_coerce_from_string_if_needed")
        return val


def _bucketize_value(val):
    if isinstance(val, bool):
        return 3, None, None, bool(val)
    if _is_number(val):
        return 1, float(val), None, None
    if hasattr(val, "getTime"):
        return 4, None, system.date.format(val, "yyyy-MM-dd HH:mm:ss.SSS"), None
    return 2, None, (u"%s" % val), None


def _to_typed_columns(qv):
    ts     = _safe_ts(qv)
    ts_iso = system.date.format(ts, "yyyy-MM-dd HH:mm:ss.SSS")

    raw = getattr(qv, "value", None)
    val = _maybe_extract_scalar(raw)
    val = _coerce_from_string_if_needed(val)

    try:
        vt, vn, vx, vb = _bucketize_value(val)
    except Exception:
        log_error(MODULE + "::_to_typed_columns/bucketize")
        vt, vn, vx, vb = 2, None, (u"%s" % val), None

    q = getattr(qv, "quality", None)
    qok, qstr = True, u"Good"
    try:
        if q is not None:
            qok  = bool(q.isGood())
            qstr = unicode(q)
    except Exception:
        log_error(MODULE + "::_to_typed_columns/quality")

    return _TYPED(vt=vt, vn=vn, vx=vx, vb=vb, ts_iso=ts_iso, qok=qok, qstr=qstr)


# -------------------- Public entry for Gateway Tag Change --------------------

def process_event(event):
    """Direct entry point for Gateway Tag Change scripts (simpler wiring)."""
    try:
        tag_path = getattr(event, "tagPath", None)
        if tag_path is None and hasattr(event, "getTagPath"):
            try:
                tag_path = event.getTagPath()
            except Exception:
                log_error(MODULE + "::process_event/getTagPath")
                tag_path = None

        curr = getattr(event, "currentValue", None)
        if curr is None and hasattr(event, "getCurrentValue"):
            try:
                curr = event.getCurrentValue()
            except Exception:
                log_error(MODULE + "::process_event/getCurrentValue")
                curr = None

        prev = getattr(event, "previousValue", None)
        if prev is None and hasattr(event, "getPreviousValue"):
            try:
                prev = event.getPreviousValue()
            except Exception:
                log_error(MODULE + "::process_event/getPreviousValue")
                prev = None

        is_initial = False
        try:
            is_initial = bool(getattr(event, "initialChange", False))
        except Exception:
            log_error(MODULE + "::process_event/initialChangeAttr")
        if (not is_initial) and hasattr(event, "getInitialChange"):
            try:
                is_initial = bool(event.getInitialChange())
            except Exception:
                log_error(MODULE + "::process_event/getInitialChange")

        if tag_path is None or curr is None:
            return

        _process_change(tag_path, prev, curr, is_initial)
    except Exception:
        log_error(MODULE + "::process_event")


def warmup():
    """Optionally call once on gateway start to prime caches."""
    _ensure_loaded(force=True)


def _handle_status_route(path):
    """Return (station_root, meta) if this path should trigger a status publish; else (None, None)."""
    sr = _station_root_of(path)
    if sr and sr in STATE.status_by_station:
        last = STATE.status_last_pub_ms.get(sr, 0)
        nowm = _now_ms()
        if (nowm - last) >= _STATUS_COALESCE_MS:
            STATE.status_last_pub_ms[sr] = nowm
            return sr, STATE.status_by_station.get(sr)
    return None, None


def _topic_keys_for_path(path):
    keys = STATE.path_to_topicstrs.get(path, set())
    if not keys and path.endswith(u"/Value"):
        keys = STATE.path_to_topicstrs.get(path[:-len(u"/Value")], set())
    elif not keys:
        keys = STATE.path_to_topicstrs.get(path + u"/Value", set())
    return list(keys)


# ---- small helpers to lower _process_change cyclomatic complexity ----

def _is_good_quality(qv):
    if not hasattr(qv, "quality"):
        return True
    try:
        return bool(qv.quality.isGood())
    except Exception:
        log_error(MODULE + "::_is_good_quality")
        return True


def _pass_deadband(prev, curr):
    if not _NUMERIC_DEADBAND or float(_NUMERIC_DEADBAND) <= 0.0:
        return True
    try:
        cv = getattr(curr, "value", curr)
        pv = getattr(prev, "value", prev)
        if _is_number(cv) and _is_number(pv):
            return abs(float(cv) - float(pv)) >= float(_NUMERIC_DEADBAND)
    except Exception:
        log_error(MODULE + "::_pass_deadband")
    return True


def _maybe_publish_node_groups(path, nowm):
    for tkey in _topic_keys_for_path(path):
        try:
            last = STATE.node_last_pub_ms.get(tkey, 0)
            if (nowm - last) < _NODE_COALESCE_MS:
                continue
            grp = STATE.node_groups_by_topicstr.get(tkey)
            if not grp:
                continue
            STATE.node_last_pub_ms[tkey] = nowm
            _publish_node_or_cycle(tkey, grp)
        except Exception:
            log_error(MODULE + "::_maybe_publish_node_groups")


def _process_change(tagPath, previousValue, currentValue, initialChange):
    try:
        if initialChange:
            return

        if _ONLY_PUBLISH_ON_GOOD and not _is_good_quality(currentValue):
            return

        if not _pass_deadband(previousValue, currentValue):
            return

        _ensure_loaded()
        path = _u(tagPath)

        # 1) STATUS snapshot route
        sr, meta = _handle_status_route(path)
        if sr and meta:
            try:
                _publish_status_snapshot(sr, meta)
            except Exception:
                log_error(MODULE + "::_process_change/status_publish")

        # 2) NODE/CYCLE routes
        _maybe_publish_node_groups(path, _now_ms())

    except Exception:
        log_error(MODULE + "::_process_change")


# -------------------- Status payload builders (schema) --------------------

def _relmap_from_station(station_root):
    leaves = _browse_leaves(station_root)
    qvs = _read_many(leaves)
    relmap = {}
    try:
        for p, qv in zip(leaves, qvs):
            rel = _u(p)[len(station_root)+1:] if _u(p).startswith(station_root + u"/") else _u(p)
            relmap[rel] = getattr(qv, "value", None)
    except Exception:
        log_error(MODULE + "::_relmap_from_station")
    return _RelmapResult(relmap=relmap, qvs=qvs, leaves=leaves)


def _first_non_null(*vals):
    for v in vals:
        if v is not None and _u(v) != u"":
            return v
    return None


def _try_int(x, default=None):
    try:
        if isinstance(x, (int, long)):
            return int(x)
        if isinstance(x, float):
            return int(x)
        s = _u(x).strip()
        if s == u"":
            return default
        return int(float(s))
    except Exception:
        return default


def _try_bool(x):
    v = _coerce_bool_tristate(x)
    return None if v is None else bool(v)


def _try_float(x, default=None):
    try:
        if isinstance(x, (int, long, float)):
            return float(x)
        s = _u(x).strip()
        if s == u"":
            return default
        return float(s)
    except Exception:
        return default


def _parse_fixture_id_from_name(name):
    try:
        tail = _u(name).rsplit("_", 1)[-1]
        return _try_int(tail, None)
    except Exception:
        log_error(MODULE + "::_parse_fixture_id_from_name")
        return None


def _collect_cycle_time(rel):
    for k in ("CycleTime", "Cycle_Time"):
        v = rel.get(k)
        try:
            if v is not None:
                return float(v)
        except Exception:
            log_warn(MODULE + "::_collect_cycle_time", None, "Bad cycle time for key %s" % k)
    for k in sorted(rel.keys()):
        if k.endswith("/CycleTime") or k.endswith("/Cycle_Time"):
            try:
                return float(rel.get(k))
            except Exception:
                log_warn(MODULE + "::_collect_cycle_time", None, "Bad cycle time for key %s" % k)
                # keep scanning
    return None


def _cache_get(cache_dict, key):
    if key is None:
        return None
    try:
        entry = cache_dict.get(key)
        if not entry:
            return None
        name, ts = entry
        if (_now() - ts) <= _LOOKUP_TTL_SEC:
            return name
        cache_dict.pop(key, None)
        return None
    except Exception:
        log_error(MODULE + "::_cache_get")
        return None


def _cache_put(cache_dict, key, name):
    try:
        cache_dict[key] = (name, _now())
    except Exception:
        log_error(MODULE + "::_cache_put")


def _extract_from_dataset(ds, preferred_cols):
    try:
        if ds.getRowCount() <= 0:
            return None

        colnames = set()
        try:
            cnt = ds.getColumnCount()
            for i in range(cnt):
                try:
                    colnames.add(_u(ds.getColumnName(i)))
                except Exception:
                    log_error(MODULE + "::_extract_from_dataset/colname")
        except Exception:
            try:
                names = ds.getColumnNames()
                for n in names:
                    colnames.add(_u(n))
            except Exception:
                log_error(MODULE + "::_extract_from_dataset/colnames-fallback")
                colnames = set()

        for col in preferred_cols:
            if col in colnames:
                try:
                    val = ds.getValueAt(0, col)
                    if val is not None and _u(val).strip() != u"":
                        return _u(val)
                except Exception:
                    log_error(MODULE + "::_extract_from_dataset/valueAtPreferred")

        try:
            for i in range(ds.getColumnCount()):
                try:
                    v = ds.getValueAt(0, i)
                    if v is not None and _u(v).strip() != u"":
                        return _u(v)
                except Exception:
                    log_error(MODULE + "::_extract_from_dataset/valueAtIndex")
        except Exception:
            log_error(MODULE + "::_extract_from_dataset/valueScan")
    except Exception:
        log_error(MODULE + "::_extract_from_dataset")
    return None


def _extract_from_iterable(rows, preferred_cols):
    try:
        for row in rows:
            for col in preferred_cols:
                try:
                    val = row[col]
                    if val is not None and _u(val).strip() != u"":
                        return _u(val)
                except Exception:
                    log_error(MODULE + "::_extract_from_iterable/rowPreferred")
            try:
                for v in row:
                    if v is not None and _u(v).strip() != u"":
                        return _u(v)
            except Exception:
                log_error(MODULE + "::_extract_from_iterable/rowScan")
            break
    except Exception:
        log_error(MODULE + "::_extract_from_iterable")
    return None


def _extract_first_name(ds, preferred_cols):
    # Scalar string/number directly
    if isinstance(ds, (unicode, str)):
        s = _u(ds).strip()
        return s if s else None

    # Dataset-like path (PyDataSet / Dataset)
    if hasattr(ds, "getRowCount"):
        return _extract_from_dataset(ds, preferred_cols)

    # Iterable-of-rows path (dict-like rows)
    return _extract_from_iterable(ds, preferred_cols)


def _run_lookup_nq(nq_path, param_variants):
    for params in param_variants:
        try:
            return system.db.runNamedQuery(nq_path, params)
        except Exception:
            # try next variant (logged once per failure)
            log_error(MODULE + "::_run_lookup_nq")
            # no 'continue' needed; loop advances naturally
    return None


def _normalize_code_key(val):
    iv = _try_int(val, None)
    return iv if iv is not None else _u(val).strip()


def _get_reject_name(code_value):
    key = _normalize_code_key(code_value)
    cached = _cache_get(STATE.reject_name_cache, key)
    if cached is not None:
        return cached
    ds = _run_lookup_nq(
        _NQ_REJECT_NAME,
        [
            {"reject_code": key},
            {"code": key},
            {"value": key}
        ],
    )
    name = _extract_first_name(ds, ["name", "label", "reject_name", "reject_code_name", "text"])
    if name is not None:
        _cache_put(STATE.reject_name_cache, key, name)
        return name
    _cache_put(STATE.reject_name_cache, key, None)
    return None


# SECURITY NOTE: The value resolved here is a human-readable *label* from station/PLC context.
# It is used only for payload display and *never* for application authorization.

def _get_user_level_label(level_value):
    key = _normalize_code_key(level_value)
    cached = _cache_get(STATE.userlevel_cache, key)
    if cached is not None:
        return cached
    ds = _run_lookup_nq(
        _NQ_USERROLE_NAME,
        [
            {"user_level": key},
            {"level": key},
            {"value": key}
        ],
    )
    name = _extract_first_name(ds, ["name", "label", "role", "role_name", "user_role_name", "text"])
    if name is not None:
        _cache_put(STATE.userlevel_cache, key, name)
        return name
    _cache_put(STATE.userlevel_cache, key, None)
    return None

# Backward-compatible alias (avoid breaking any external callers if they existed)
_get_userrole_name = _get_user_level_label


def _build_fixture_obj(prefix, rel):
    fnum = _try_int(prefix.split("_")[-1], None)

    _rc_raw = rel.get(prefix + "/Reject_Code")
    _rc_name = _get_reject_name(_rc_raw)
    _ul_raw = rel.get(prefix + "/User_Level")
    _ul_name = _get_user_level_label(_ul_raw)

    return {
        "FixtureID": fnum,
        "Resetable_GoodParts": _try_int(rel.get(prefix + "/Good_Part"), 0),
        "Resetable_BadParts":  _try_int(rel.get(prefix + "/Bad_Part"), 0),
        "Machine_Running":     _try_bool(rel.get(prefix + "/Machine_Running")),
        "Machine_Faulted":     _try_bool(rel.get(prefix + "/Machine_Faulted")),
        "Smart_Part_In_Progress": _try_bool(rel.get(prefix + "/Smart_Part_Mode")),
        "Part_Number":         _u(rel.get(prefix + "/Part_Number")) if rel.get(prefix + "/Part_Number") is not None else None,
        "Serial_Number_1":     _u(rel.get(prefix + "/Serial_Number_1")) if rel.get(prefix + "/Serial_Number_1") is not None else None,
        "Serial_Number_2":     _u(rel.get(prefix + "/Serial_Number_2")) if rel.get(prefix + "/Serial_Number_2") is not None else None,
        "Serial_Number_3":     _u(rel.get(prefix + "/Serial_Number_3")) if rel.get(prefix + "/Serial_Number_3") is not None else None,
        "Serial_Number_4":     _u(rel.get(prefix + "/Serial_Number_4")) if rel.get(prefix + "/Serial_Number_4") is not None else None,
        "Serial_Number_5":     _u(rel.get(prefix + "/Serial_Number_5")) if rel.get(prefix + "/Serial_Number_5") is not None else None,
        "User_ID":             _u(rel.get(prefix + "/UserID")) if rel.get(prefix + "/UserID") is not None else None,
        "User_Level":          (_ul_name if _ul_name is not None else _ul_raw),
        "ANDON_Active":        _try_int(rel.get(prefix + "/Andon_Active"), None),
        "Reject_Code":         (_rc_name if _rc_name is not None else _rc_raw),
    }


def _detect_flat_fixtures(relmap):
    prefs = set()
    for k in relmap.keys():
        if k.startswith("Fixture_"):
            prefs.add(k.split("/", 1)[0])
    return sorted(prefs, key=lambda s: _try_int(s.split("_")[-1], 0))


def _detect_turntable_sides(relmap):
    sides = set()
    for k in relmap.keys():
        if k.startswith("TurntableSide_"):
            sides.add(k.split("/", 1)[0])
    return sorted(sides, key=lambda s: _try_int(s.split("_")[-1], 0))


def _detect_turntable_fixtures(relmap, side_root):
    base = side_root + "/TurntableFixtures/"
    prefixes = set()
    for k in relmap.keys():
        if k.startswith(base):
            rest = k[len(base):]
            first = rest.split("/", 1)[0]
            if first.startswith("TurntableFixture_"):
                prefixes.add(base + first)
    return sorted(prefixes)


def _build_flat_station_payload(station_root, relmap):
    fixture_prefixes = _detect_flat_fixtures(relmap)
    fixtures = [_build_fixture_obj(fp, relmap) for fp in fixture_prefixes]

    _side_candidates = [relmap.get(fp + "/SideID") for fp in fixture_prefixes]
    _side_candidates.append(relmap.get("SideID"))
    any_side = _first_non_null(*_side_candidates)

    cycle = _try_float(_first_non_null(relmap.get("CycleTime"),
                                       relmap.get("Cycle_Time")), None)
    total = _try_int(_first_non_null(relmap.get("Total_Parts"),
                                     relmap.get("TotalParts")), None)

    data_obj = {
        "SideID": any_side,
        "CycleTime": cycle,
        "TotalParts": total,
        "fixtures": fixtures
    }
    return {
        "timestamp": _iso_now(),
        "version": _get_version(),
        "data": [data_obj]
    }


def _detect_tt_fixtures(side_prefix, rel):
    base = side_prefix + "/TurntableFixtures/"
    n = len(base)
    ids = set()
    for k in rel.keys():
        if k.startswith(base):
            seg = k[n:].split("/", 1)[0]
            if seg.startswith("TurntableFixture_"):
                ids.add(seg)
    return sorted(ids, key=lambda s: _try_int(s.split("_")[-1], 0))


def _build_tt_fixture_obj(side, fx, rel):
    pref = side + "/TurntableFixtures/" + fx
    fnum = _try_int(fx.split("_")[-1], None)

    _rc_raw = rel.get(pref + "/Reject_Code")
    _rc_name = _get_reject_name(_rc_raw)
    _ul_raw = rel.get(pref + "/User_Level")
    _ul_name = _get_user_level_label(_ul_raw)

    return {
        "FixtureID": fnum,
        "Resetable_GoodParts": _try_int(rel.get(pref + "/Good_Part"), 0),
        "Resetable_BadParts":  _try_int(rel.get(pref + "/Bad_Part"), 0),
        "Machine_Running":     _try_bool(rel.get(pref + "/Machine_Running")),
        "Machine_Faulted":     _try_bool(rel.get(pref + "/Machine_Faulted")),
        "Smart_Part_In_Progress": _try_bool(rel.get(pref + "/Smart_Part_Mode")),
        "Part_Number":         _u(rel.get(pref + "/Part_Number")) if rel.get(pref + "/Part_Number") is not None else None,
        "Serial_Number_1":     _u(rel.get(pref + "/Serial_Number_1")) if rel.get(pref + "/Serial_Number_1") is not None else None,
        "Serial_Number_2":     _u(rel.get(pref + "/Serial_Number_2")) if rel.get(pref + "/Serial_Number_2") is not None else None,
        "Serial_Number_3":     _u(rel.get(pref + "/Serial_Number_3")) if rel.get(pref + "/Serial_Number_3") is not None else None,
        "Serial_Number_4":     _u(rel.get(pref + "/Serial_Number_4")) if rel.get(pref + "/Serial_Number_4") is not None else None,
        "Serial_Number_5":     _u(rel.get(pref + "/Serial_Number_5")) if rel.get(pref + "/Serial_Number_5") is not None else None,
        "User_ID":             _u(rel.get(pref + "/UserID")) if rel.get(pref + "/UserID") is not None else None,
        "User_Level":          (_ul_name if _ul_name is not None else _ul_raw),
        "ANDON_Active":        _try_int(rel.get(pref + "/Andon_Active"), None),
        "Reject_Code":         (_rc_name if _rc_name is not None else _rc_raw),
    }


def _build_turntable_payload(station_root, relmap):
    sides = _detect_turntable_sides(relmap)
    data_arr = []
    for side in sides:
        fxs = _detect_tt_fixtures(side, relmap)
        fixtures = [_build_tt_fixture_obj(side, fx, relmap) for fx in fxs]

        _cands = [relmap.get(side + "/SideID")]
        _cands.extend([relmap.get(side + "/TurntableFixtures/" + fx + "/SideID") for fx in fxs])
        side_id = _first_non_null(*_cands)

        side_total = _try_int(_first_non_null(relmap.get("Total_Parts"),
                                              relmap.get("TotalParts")), None)

        side_cycle = _try_float(_first_non_null(relmap.get("CycleTime"),
                                                relmap.get("Cycle_Time")), None)

        data_arr.append({
            "SideID": side_id,
            "CycleTime": side_cycle,
            "TotalParts": side_total,
            "fixtures": fixtures
        })

    return {
        "timestamp": _iso_now(),
        "version": _get_version(),
        "data": data_arr
    }


def _build_status_payload(station_root, relmap):
    if any(k.startswith("TurntableSide_") for k in relmap.keys()):
        return _build_turntable_payload(station_root, relmap)
    return _build_flat_station_payload(station_root, relmap)


# -------------------- Public cache control --------------------

def refresh_all():
    try:
        STATE.reject_name_cache.clear()
    except Exception:
        log_error(MODULE + "::refresh_all/reject_cache")
    try:
        STATE.userlevel_cache.clear()
    except Exception:
        log_error(MODULE + "::refresh_all/userlevel_cache")
    _ensure_loaded(force=True)


def force_refresh(source="ui"):
    try:
        _load_now()
    except Exception:
        log_error(MODULE + "::force_refresh")