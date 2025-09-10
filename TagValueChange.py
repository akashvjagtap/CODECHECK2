# ---------------------------------------------------------------------
# TagValueChange.py  (Gateway scope)
# ---------------------------------------------------------------------
from time import time as _now
import system
from MagnaDataOps.LoggerFunctions import log_info, log_warn, log_error

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
# --- Code/name lookup NQs + cache TTL ---
# ---- Lookups for code->name mapping ----
_NQ_REJECT_NAME   = "MagnaDataOps/Configuration/TagPublishingConfiguration/Additional/getRejectCodeName"
_NQ_USERROLE_NAME = "MagnaDataOps/Configuration/TagPublishingConfiguration/Additional/getUserRoleName"
_LOOKUP_TTL_SEC   = 300.0   # cache lifetime for code->name lookups (seconds)

# ---- Cache state ----
_last_load = 0.0


# Topic names by id (lowercased): {19: "status", 20:"faults", ...}
_topic_name_by_id = {}

# All rows from NQ (both scopes), keyed by path (for logging DB rows)
_cfg_by_path = {}   # path -> {"topic_id": int, "topic": str, "qos": int, "retain": bool, "config_id": int, "scope": "tag"|"instance"}

# Indexes
# Status (station-scope): {station_root -> {"topic": str, "topic_id": int, "qos": int, "retain": bool}}
_status_by_station = {}

# Node/Cycle groupings:
#   node_groups_by_topic: {topic_id -> {"name": "faults"/"andons"/"alerts"/"cycletime", "topic": str, "qos": int, "retain": bool, "members": set(paths)}}
#   path_to_topics: {leaf_path -> set(topic_id)}
_node_groups_by_topicstr = {} 
_path_to_topicstrs       = {} 

# Broker name cache
_broker_cache = {"t": 0.0, "v": _DEFAULT_SERVER_NAME}

# Coalesce windows
_status_last_pub_ms = {}  # station_root -> epoch ms last publish
_node_last_pub_ms   = {}  # topic_id     -> epoch ms last publish

# ---------- tiny utils ----------
def _u(x):
    try: return unicode(x)
    except Exception:
        try: return unicode(str(x) if x is not None else u"")
        except Exception: return u""

def _now_ms():
    return int(round(_now() * 1000))

def _iso_now():
    try:
        return system.date.format(system.date.now(), _ISO_FMT)
    except:
        # fallback without timezone
        return system.date.format(system.date.now(), "yyyy-MM-dd HH:mm:ss.SSS")

def _get_version():
    try:
        import MagnaDataOps.CommonScripts as CS
        v = getattr(CS, "payload_version", None)
        return _u(v or "1.0.0")
    except Exception:
        return u"1.0.0"

def _get_broker_name():
    now = _now()
    if (now - _broker_cache["t"]) < _BROKER_TTL_SEC and _broker_cache["v"]:
        return _broker_cache["v"]
    try:
        res = system.tag.readBlocking([_BROKER_TAG_PATH])[0]
        val = (u"%s" % res.value).strip() if res and res.value is not None else u""
        _broker_cache["v"] = (val or _DEFAULT_SERVER_NAME)
        _broker_cache["t"] = now
        if not val:
            log_warn("TagValueChange::_get_broker_name", None, "BrokerName tag blank; using default '%s'" % _DEFAULT_SERVER_NAME)
    except Exception as e:
        _broker_cache["t"] = now
        log_error("TagValueChange::_get_broker_name", None, "BrokerName read failed; using cached/default. %s" % e)
    return _broker_cache["v"]

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
        if seg.endswith(u"MagnaStations") or parts[i].split(u"]")[-1] == u"MagnaStations":
            anchor = i
            break
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
            if not results:
                # maybe it's already a leaf
                try:
                    _ = system.tag.getAttribute(bp, "DataType")
                    out.append(_u(bp))
                except:
                    pass
                continue
            for r in results:
                try:
                    fp = _u(r['fullPath'])
                    if r['hasChildren']:
                        stack.append(fp)
                    else:
                        out.append(fp)
                except:
                    continue
        except:
            # permissive: try DataType to detect leaf
            try:
                _ = system.tag.getAttribute(bp, "DataType")
                out.append(_u(bp))
            except:
                pass
    # de-dup
    return sorted(set(out))

def _read_many(paths):
    try:
        return system.tag.readBlocking(paths)
    except Exception as e:
        log_error("TagValueChange::_read_many", None, "readBlocking failed: %s" % e)
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
        except Exception as e:
            log_error("TagValueChange::_publish", None, "publish error: %s" % e)
    server = _get_broker_name()
    system.util.invokeAsynchronous(_pub, [server, mqtt_topic, payload_obj_or_str, qos, retain])

# ---------- Loader ----------
def _load_topics_table():
    """_topic_name_by_id from NQ: expects id,value,label or topic_name column"""
    out = {}
    try:
        ds = system.db.runNamedQuery(_NQ_TOPICS, {})
        for row in ds:
            try:
                tid = int(row.get("value") if hasattr(row, "get") else row["value"])
            except Exception:
                try: tid = int(row["id"])
                except: continue
            # prefer explicit name column; fallback to label
            try:
                nm = _u(row["topic_name"]).strip().lower()
            except Exception:
                try: nm = _u(row["label"]).strip().lower()
                except: nm = u""
            out[tid] = nm
    except Exception as e:
        log_error("TagValueChange::_load_topics_table", None, "NamedQuery failed: %s" % e)
    return out

def _load_now():
    """(Re)load all active+approved rows. NQ must include scope in ('tag','instance')."""
    global _last_load, _cfg_by_path, _topic_name_by_id, _status_by_station
    global _node_groups_by_topicstr, _path_to_topicstrs

    _topic_name_by_id = _load_topics_table()

    cfg_by_path = {}
    status_by_station = {}
    node_groups_by_topicstr = {}   # local build
    path_to_topicstrs = {}         # local build

    try:
        ds = system.db.runNamedQuery(_NQ_PATH, {})
    except Exception as e:
        log_error("TagValueChange::_load_now", None, "NamedQuery failed: %s" % e)
        return

    count = 0
    for row in ds:
        try:
            path   = _u(row["tag_path"]).strip()
            topic  = _u(row["resolved_topic"]).strip()
            qos    = int(row["qos"] or 0)
            retain = bool(row["retain_flag"] or 0)
            try: topic_id = int(row["topic_id"])
            except: topic_id = 0
            try:
                cfg_id = int(row.get("config_id")) if hasattr(row, "get") else int(row["config_id"])
            except:
                try: cfg_id = int(row["id"])
                except: cfg_id = 0
            try:
                scope = _u(row["scope"]).strip().lower()
            except:
                sr = _station_root_of(path)
                scope = u"instance" if ("/status" in topic and sr and (sr == path)) else u"tag"

            if not path or not topic:
                continue

            # record for logging
            cfg_by_path[path] = {"topic_id": topic_id, "topic": topic, "qos": qos, "retain": retain, "config_id": cfg_id, "scope": scope}
            count += 1

            # classify topic name (fallback heuristics)
            tname = _topic_name_by_id.get(topic_id, u"")
            if not tname:
                lt = topic.lower()
                if "/status/faults" in lt or "/faults" in lt:
                    tname = u"faults"
                elif "/status/andons" in lt or "/andons" in lt:
                    tname = u"andons"
                elif "/status/alerts" in lt or "/alerts" in lt:
                    tname = u"alerts"
                elif "cycletime" in lt or "cycle_time" in lt or "/cycle" in lt:
                    tname = u"cycletime"

            # status rows are keyed by station root
            if scope == u"instance" or tname == u"status" or ("/status" in topic and _station_root_of(path) == path):
                sr = _station_root_of(path)
                if sr:
                    status_by_station[sr] = {"topic": topic, "topic_id": topic_id, "qos": qos, "retain": retain,"config_id": cfg_id}
                continue

            # node/cycle groups are keyed by **resolved topic string**
            grp = node_groups_by_topicstr.setdefault(
                topic, {"name": tname, "topic": topic, "topic_id": topic_id, "qos": qos, "retain": retain, "members": set()}
            )
            grp["members"].add(path)

            # index all likely event-path variants for lookup on change
            for pv in _index_variants(path):
                path_to_topicstrs.setdefault(pv, set()).add(topic)

        except Exception as e_row:
            log_warn("TagValueChange::_load_now", None, "Skipping malformed row: %s" % e_row)
            continue

    # publish caches
    _cfg_by_path = cfg_by_path
    _status_by_station = status_by_station
    _node_groups_by_topicstr = node_groups_by_topicstr
    _path_to_topicstrs = path_to_topicstrs
    _last_load = _now()


def _ensure_loaded(force=False):
    if force or (_now() - _last_load) > _TTL_SEC or not _cfg_by_path:
        _load_now()

# ---------- Coercion for node topics ----------
def _coerce_bool_tristate(val):
    """
    Return True/False/None (None = invalid).
    Numbers: 0->False, 1->True, others->None
    Strings: true/on/yes/1 -> True; false/off/no/0 -> False; others->None
    """
    try:
        if isinstance(val, bool):
            return bool(val)
        if isinstance(val, (int, long, float)):
            f = float(val)
            if f == 0.0: return False
            if f == 1.0: return True
            return None
        s = _u(val).strip().lower()
        if s in ("true","1","on","yes"): return True
        if s in ("false","0","off","no"): return False
        return None
    except:
        return None

def _and_tristate(values):
    """
    values: iterable of True/False/None
    - any False -> False
    - else all True -> True
    - else -> None
    """
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
        # Java Map-like objects
        if hasattr(v, "get"):
            inner = v.get("Value", v.get("value", None))
            if inner is not None:
                return inner
        # Python dict
        if isinstance(v, dict):
            if "Value" in v: return v["Value"]
            if "value" in v: return v["value"]
        # JSON string
        if isinstance(v, (unicode, str)):
            s = (u"%s" % v).strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    obj = system.util.jsonDecode(s)
                    if isinstance(obj, dict):
                        if "Value" in obj: return obj["Value"]
                        if "value" in obj: return obj["value"]
                except:
                    pass
    except:
        pass
    return v


# ---------- Publishers ----------
def _publish_status_snapshot(station_root, meta):
    """Publish full snapshot for one station_root using structured schema."""
    try:
        # Build the relative map + capture raw reads for DB logging
        relmap, qvs, leaves = _relmap_from_station(station_root)

        # Build payload (flat or turntable) and publish
        payload = _build_status_payload(station_root, relmap)
        _publish_async(meta["topic"], meta["qos"], meta["retain"], payload)

        # Log every raw value we read
        _log_status_payload_via_bulk(meta, payload)

    except Exception as e:
        log_error("TagValueChange::_publish_status_snapshot", None, "Error: %s" % e)

def _publish_node_or_cycle(topic_key, grp):
    """
    topic_key: resolved topic string (group key)
    grp: {"name","topic","topic_id","qos","retain","members": set(paths)}
    """
    try:
        members = sorted(grp["members"])
        if not members:
            payload = {"Version": _get_version(), "Timestamp": _iso_now(), "Value": None}
            _publish_async(grp["topic"], grp["qos"], grp["retain"], payload)
            return

        # normalize read paths to avoid property nodes
        read_paths = [_norm_value_path(p) for p in members]
        qvs = _read_many(read_paths)

        if grp["name"] in ("faults", "andons", "alerts"):
            vals = []
            invalid_notes = []
            # iterate with original path for logging
            for orig_p, qv in zip(members, qvs):
                # quality check
                qok = True
                try: qok = bool(qv.quality.isGood())
                except: pass

                raw = getattr(qv, "value", None) if qok else None
                scalar = _maybe_extract_scalar(raw)

                # try explicit '/Value' child once if needed
                if scalar is None:
                    alt = orig_p if orig_p.endswith(u"/Value") else (orig_p + u"/Value")
                    try:
                        alt_qv = system.tag.readBlocking([alt])[0]
                        if alt_qv and alt_qv.quality.isGood():
                            scalar = _maybe_extract_scalar(getattr(alt_qv, "value", None))
                            qv = alt_qv  # for logging
                    except:
                        pass

                coerced = _coerce_bool_tristate(scalar)
                vals.append(coerced)
                if coerced is None:
                    invalid_notes.append(u"%s -> raw=%s qok=%s" % (_u(orig_p), _u(raw), qok))

            value = _and_tristate(vals)
            payload = {"Version": _get_version(), "Timestamp": _iso_now(), "Value": value}
            _publish_async(grp["topic"], grp["qos"], grp["retain"], payload)
            _bulk_log_raw(grp["topic_id"], grp["qos"], grp["retain"], qvs, members)
            return

        # cycletime: first good numeric (unwrap dict/JSON if needed)
        out = None
        for qv in qvs:
            try:
                if qv.quality.isGood():
                    v = _maybe_extract_scalar(getattr(qv, "value", None))
                    if isinstance(v, (int, long, float)):
                        out = float(v)
                        break
            except:
                pass
        payload = {"Version": _get_version(), "Timestamp": _iso_now(), "Value": out}
        _publish_async(grp["topic"], grp["qos"], grp["retain"], payload)
        _bulk_log_raw(grp["topic_id"], grp["qos"], grp["retain"], qvs, members)

    except Exception as e:
        log_error("TagValueChange::_publish_node_or_cycle", None, "Error: %s" % e)

def _bulk_log_raw(topic_id, qos, retain, qvs, paths):
    """Write raw values to DB using your existing bulk NQ shape."""
    try:
        rows = []
        for p, qv in zip(paths, qvs):
            info = _cfg_by_path.get(p)
            # If this path wasn't directly in cfg_by_path (e.g., status leaf), still log minimal row using topic_id
            config_id = int(info["config_id"]) if info and info.get("config_id") else 0
            vt, vn, vx, vb, ts_iso, qok, qstr = _to_typed_columns(qv)
            rows.append({
                "config_id":  config_id,
                "topic_id":   int(topic_id),
                "qos":        int(qos),
                "retain":     1 if retain else 0,
                "value_type": int(vt),
                "value_num":  vn,
                "value_text": vx,
                "value_bool": vb,
                "quality_ok": 1 if qok else 0,
                "quality":    qstr,
                "src_ts":     ts_iso
            })
        if rows:
            system.db.runNamedQuery(
                _NQ_BULK_LOG_PATH,
                {"json_payload": system.util.jsonEncode({"rows": rows, "log_history": 1, "created_by": "gateway"})}
            )
    except Exception as e:
        log_error("TagValueChange::_bulk_log_raw", None, "bulk insert failed: %s" % e)


def _log_status_payload_via_bulk(meta, payload_obj):
    try:
        row = {
            "config_id":  int(meta.get("config_id") or 0),
            "topic_id":   int(meta.get("topic_id") or 0),
            "qos":        int(meta.get("qos") or 0),
            "retain":     1 if meta.get("retain") else 0,

            "value_type": 2,  # text
            "value_num":  None,
            "value_text": system.util.jsonEncode(payload_obj),  # EXACT JSON we published
            "value_bool": None,

            "quality_ok": 1,
            "quality":    u"Good",
            "src_ts":     system.date.format(system.date.now(), "yyyy-MM-dd HH:mm:ss.SSS"),
        }
        system.db.runNamedQuery(
            _NQ_BULK_LOG_PATH,
            {"json_payload": system.util.jsonEncode({"rows":[row], "log_history":1, "created_by":"gateway"})}
        )
    except Exception as e:
        log_error("TagValueChange::_log_status_payload_via_bulk", None, "bulk insert failed: %s" % e)


# ---------- Typed column helper ----------
def _safe_ts(qv):
    try:
        ts = getattr(qv, "timestamp", None)
        if ts and hasattr(ts, "getTime"):
            return ts
    except Exception:
        pass
    return system.date.now()

def _to_typed_columns(qv):
    ts     = _safe_ts(qv)
    ts_iso = system.date.format(ts, "yyyy-MM-dd HH:mm:ss.SSS")

    # 1) unwrap to a scalar if value is a dict/JSON string that contains Value/value
    raw = getattr(qv, "value", None)
    val = _maybe_extract_scalar(raw)

    # 2) try to coerce common string forms into proper types
    try:
        if isinstance(val, (unicode, str)):
            s = _u(val).strip()
            # try JSON object again (in case of stringified dict not caught above)
            if s.startswith("{") and s.endswith("}"):
                try:
                    obj = system.util.jsonDecode(s)
                    if isinstance(obj, dict):
                        if "Value" in obj: val = obj["Value"]
                        elif "value" in obj: val = obj["value"]
                        else:                val = obj  # leave as dict → will fall to text below
                except:
                    pass
            # booleans
            if isinstance(val, (unicode, str)):
                sl = s.lower()
                if sl in ("true","false"):
                    val = (sl == "true")
            # numeric
            if isinstance(val, (unicode, str)):
                try:
                    # allow ints, floats, and sci notation
                    val_num = float(s)
                    val = val_num
                except:
                    pass
    except:
        pass

    # 3) type bucket → numeric/text/bool/datetime (vt matches your schema)
    if isinstance(val, bool):
        vt, vn, vx, vb = 3, None, None, bool(val)
    elif isinstance(val, (int, long, float)):
        vt, vn, vx, vb = 1, float(val), None, None
    elif hasattr(val, "getTime"):
        vt, vn, vx, vb = 4, None, system.date.format(val, "yyyy-MM-dd HH:mm:ss.SSS"), None
    else:
        vt, vn, vx, vb = 2, None, (u"%s" % val), None

    q = getattr(qv, "quality", None)
    qok, qstr = True, u"Good"
    try:
        if q is not None:
            qok  = bool(q.isGood())
            qstr = unicode(q)
    except:
        pass

    return vt, vn, vx, vb, ts_iso, qok, qstr

# ---------- Public entry for Gateway Tag Change ----------
def _process_change(tagPath, previousValue, currentValue, initialChange):
    try:
		if initialChange:
		    return
		
		# quality gate on changing tag
		if _ONLY_PUBLISH_ON_GOOD and hasattr(currentValue, "quality"):
		    try:
		        if not currentValue.quality.isGood():
		            return
		    except:
		        pass
		
		# numeric deadband
		try:
		    if _NUMERIC_DEADBAND and float(_NUMERIC_DEADBAND) > 0.0:
		        cv = getattr(currentValue, "value", currentValue)
		        pv = getattr(previousValue, "value", previousValue)
		        if isinstance(cv, (int, long, float)) and isinstance(pv, (int, long, float)):
		            if abs(float(cv) - float(pv)) < float(_NUMERIC_DEADBAND):
		                return
		except:
		    pass
		
		_ensure_loaded()
		
		path = _u(tagPath)
		
		# 1) STATUS routing: if this path lives under a registered station, coalesce & publish snapshot
		sr = _station_root_of(path)
		if sr and sr in _status_by_station:
		    last = _status_last_pub_ms.get(sr, 0)
		    nowm = _now_ms()
		    if (nowm - last) >= _STATUS_COALESCE_MS:
		        _status_last_pub_ms[sr] = nowm
		        meta = _status_by_station.get(sr)
		        if meta:
		            _publish_status_snapshot(sr, meta)
		
		# 2) NODE/CYCLE routing: for every RESOLVED TOPIC that includes this path
		topic_keys = _path_to_topicstrs.get(path, set())
		if not topic_keys and path.endswith(u"/Value"):
		    topic_keys = _path_to_topicstrs.get(path[:-len(u"/Value")], set())
		elif not topic_keys:
		    topic_keys = _path_to_topicstrs.get(path + u"/Value", set())
		
		nowm = _now_ms()
		for tkey in list(topic_keys):  # tkey is the resolved_topic string
		    last = _node_last_pub_ms.get(tkey, 0)
		    if (nowm - last) < _NODE_COALESCE_MS:
		        continue
		    grp = _node_groups_by_topicstr.get(tkey)
		    if not grp:
		        continue
		    _node_last_pub_ms[tkey] = nowm
		    _publish_node_or_cycle(tkey, grp)

    except Exception as e:
        log_error("TagValueChange::_process_change", None, "Exception: %s" % e)
        
        
#--------------------------------------------------------------------------------------------------------------------------------------
# ---------- Status payload builders (schema) ----------
def _relmap_from_station(station_root):
    """Return {relativePathUnderStation: value} for all leaves."""
    leaves = _browse_leaves(station_root)
    qvs = _read_many(leaves)
    relmap = {}
    for p, qv in zip(leaves, qvs):
        rel = _u(p)[len(station_root)+1:] if _u(p).startswith(station_root + u"/") else _u(p)
        relmap[rel] = getattr(qv, "value", None)
    return relmap, qvs, leaves

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
        if s == u"": return default
        return int(float(s))
    except:
        return default


def _try_bool(x):
    v = _coerce_bool_tristate(x)
    return None if v is None else bool(v)
    
def _try_float(x, default=None):
    try:
        if isinstance(x, (int, long, float)):
            return float(x)
        s = _u(x).strip()
        if s == u"": return default
        return float(s)
    except:
        return default    
    
    

def _parse_fixture_id_from_name(name):
    # "Fixture_1" -> 1, "TurntableFixture_2" -> 2
    try:
        tail = _u(name).rsplit("_", 1)[-1]
        return _try_int(tail, None)
    except:
        return None



def _collect_cycle_time(rel):
    # station-level first
    for k in ("CycleTime", "Cycle_Time"):
        v = rel.get(k)
        try:
            if v is not None: return float(v)
        except: pass
    # otherwise first fixture/side CycleTime found
    for k in sorted(rel.keys()):
        if k.endswith("/CycleTime") or k.endswith("/Cycle_Time"):
            try:
                return float(rel.get(k))
            except:
                continue
    return None

def _build_fixture_obj(prefix, rel):
    fnum = _try_int(prefix.split("_")[-1], None)

    # NEW: map coded values to names (fallback to raw)
    _rc_raw = rel.get(prefix + "/Reject_Code")
    _rc_name = _get_reject_name(_rc_raw)
    _ul_raw = rel.get(prefix + "/User_Level")
    _ul_name = _get_userrole_name(_ul_raw)

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

        # CHANGED: send **name** if found, else exact tag value
        "User_Level":          (_ul_name if _ul_name is not None else _ul_raw),
        "ANDON_Active":        _try_int(rel.get(prefix + "/Andon_Active"), None),
        # CHANGED: send **name** if found, else exact tag value
        "Reject_Code":         (_rc_name if _rc_name is not None else _rc_raw),
    }

def _detect_flat_fixtures(relmap):
    prefs = set()
    for k in relmap.keys():
        if k.startswith("Fixture_"):
            prefs.add(k.split("/", 1)[0])
    # sort by numeric suffix if present
    return sorted(prefs, key=lambda s: _try_int(s.split("_")[-1], 0))

def _detect_turntable_sides(relmap):
    sides = set()
    for k in relmap.keys():
        if k.startswith("TurntableSide_"):
            sides.add(k.split("/", 1)[0])
    return sorted(sides, key=lambda s: _try_int(s.split("_")[-1], 0))

def _detect_turntable_fixtures(relmap, side_root):
    """Return sorted fixture prefixes under a side like 'TurntableSide_1/TurntableFixtures/TurntableFixture_1'."""
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

    # Pull SideID as before
    _side_candidates = [relmap.get(fp + "/SideID") for fp in fixture_prefixes]
    _side_candidates.append(relmap.get("SideID"))
    any_side = _first_non_null(*_side_candidates)

    # --- NEW: Station-level only (no fixture math) ---
    cycle = _try_float(_first_non_null(relmap.get("CycleTime"),
                                       relmap.get("Cycle_Time")), None)
    total = _try_int(_first_non_null(relmap.get("Total_Parts"),
                                     relmap.get("TotalParts")), None)

    data_obj = {
        "SideID": any_side,
        "CycleTime": cycle,      # <- from station memory tag
        "TotalParts": total,     # <- from station memory tag
        "fixtures": fixtures     # (kept for detail; not used to compute outer fields)
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

    # NEW: map coded values to names (fallback to raw)
    _rc_raw = rel.get(pref + "/Reject_Code")
    _rc_name = _get_reject_name(_rc_raw)
    _ul_raw = rel.get(pref + "/User_Level")
    _ul_name = _get_userrole_name(_ul_raw)

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

        # CHANGED: send **name** if found, else exact tag value
        "User_Level":          (_ul_name if _ul_name is not None else _ul_raw),
        "ANDON_Active":        _try_int(rel.get(pref + "/Andon_Active"), None),
        # CHANGED: send **name** if found, else exact tag value
        "Reject_Code":         (_rc_name if _rc_name is not None else _rc_raw),
    }







def _build_turntable_payload(station_root, relmap):
    sides = _detect_turntable_sides(relmap)
    data_arr = []
    for side in sides:
        fxs = _detect_tt_fixtures(side, relmap)
        fixtures = [_build_tt_fixture_obj(side, fx, relmap) for fx in fxs]

        # SideID preference unchanged
        _cands = [relmap.get(side + "/SideID")]
        _cands.extend([relmap.get(side + "/TurntableFixtures/" + fx + "/SideID") for fx in fxs])
        side_id = _first_non_null(*_cands)

        # --- NEW: Side-level only (no fixture math) ---
        side_total = _try_int(_first_non_null(relmap.get("Total_Parts"),
                                     relmap.get("TotalParts")), None)

        side_cycle = _try_float(_first_non_null(relmap.get("CycleTime"),
                                                relmap.get("Cycle_Time")), None)

        data_arr.append({
            "SideID": side_id,
            "CycleTime": side_cycle,   # <- from side/station tag
            "TotalParts": side_total,  # <- from side tag
            "fixtures": fixtures       # (kept for detail; not used to compute outer fields)
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




# ---- Small caches for code->name lookups ----
_reject_name_cache = {}   # key -> (name, ts_sec)
_userrole_cache    = {}   # key -> (name, ts_sec)

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
        # stale -> drop
        cache_dict.pop(key, None)
        return None
    except:
        return None

def _cache_put(cache_dict, key, name):
    try:
        cache_dict[key] = (name, _now())
    except:
        pass

def _extract_first_name(ds, preferred_cols):
    """
    Accepts a PyDataSet or plain scalar. Returns a unicode string or None.
    """
    # Scalar string/number directly
    if isinstance(ds, (unicode, str)):
        s = _u(ds).strip()
        return s if s else None
    # Try iterable dataset
    try:
        # Prefer dataset-style
        if hasattr(ds, "getRowCount"):
            if ds.getRowCount() <= 0:
                return None
            # Try each preferred col
            for col in preferred_cols:
                try:
                    val = ds.getValueAt(0, col)
                    if val is not None and _u(val).strip() != u"":
                        return _u(val)
                except:
                    continue
            # Fallback: scan first row’s values
            try:
                # Some gateways allow row-like dict access via iteration
                row0 = list(ds)[0] if hasattr(ds, "__iter__") else None
                if row0:
                    for v in row0:
                        if v is not None and _u(v).strip() != u"":
                            return _u(v)
            except:
                pass
            return None
        # Or iter over rows
        for row in ds:
            # row might be dict-like
            for col in preferred_cols:
                try:
                    val = row[col]
                    if val is not None and _u(val).strip() != u"":
                        return _u(val)
                except:
                    continue
            # fallback: first non-empty in row
            try:
                for v in row:
                    if v is not None and _u(v).strip() != u"":
                        return _u(v)
            except:
                pass
            break
    except:
        pass
    return None

def _run_lookup_nq(nq_path, param_variants):
    """
    Try running the NQ with different param-name variants until one works.
    Returns the dataset/scalar or None.
    """
    for params in param_variants:
        try:
            return system.db.runNamedQuery(nq_path, params)
        except:
            continue
    return None

def _normalize_code_key(val):
    """
    Keys the cache predictably (int if possible, else trimmed string).
    """
    iv = _try_int(val, None)
    return iv if iv is not None else _u(val).strip()
    
def _norm_value_path(p):
    p = _u(p).strip()
    if p.endswith(u"/Value"):
        return p[:-len(u"/Value")]
    return p

def _get_reject_name(code_value):
    """
    Map Reject_Code -> name (cached). If no name found, return None (caller will fallback to raw value).
    """
    key = _normalize_code_key(code_value)
    cached = _cache_get(_reject_name_cache, key)
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
    # Try common column names; adjust if your NQs use a different alias
    name = _extract_first_name(ds, ["name", "label", "reject_name", "reject_code_name", "text"])
    if name is not None:
        _cache_put(_reject_name_cache, key, name)
        return name
    # Negative cache to avoid hammering
    _cache_put(_reject_name_cache, key, None)
    return None

def _get_userrole_name(level_value):
    """
    Map User_Level -> role name (cached). If no name found, return None (caller will fallback).
    """
    key = _normalize_code_key(level_value)
    cached = _cache_get(_userrole_cache, key)
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
        _cache_put(_userrole_cache, key, name)
        return name
    _cache_put(_userrole_cache, key, None)
    return None

def refresh_all():
    """Clear code/name caches and force-reload DB config."""
    try:
        _reject_name_cache.clear()
    except:
        pass
    try:
        _userrole_cache.clear()
    except:
        pass
    _ensure_loaded(force=True)


def force_refresh(source="ui"):
    """Reload config/cache immediately (ignores TTL)."""
    try:
        _load_now()
    except Exception as e:
        log_error("TagValueChange::force_refresh", None, "Reload failed: %s" % e)
