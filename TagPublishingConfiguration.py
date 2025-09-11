# <summary>
# Created By: Akash J
# Creation Date: 07/17/2024
# Comments: OPC UA browse helpers for Tag Publishing Configuration (lazy-load, gateway-safe)
# </summary>

from time import time as _now
import re
import system
from MagnaDataOps.LoggerFunctions import log_info, log_warn, log_error

_OPC_SERVER_NAME    = "Ignition OPC UA Server"
_DEVICES_ROOT       = "ns=1;s=Devices"
_DEVICES_CACHE_TTL  = 10
_CHILDREN_CACHE_TTL = 10

_CACHE_DEVICES  = {}
_CACHE_CHILDREN = {}

# Precompile existing index pattern (no logic change; same pattern as before).
_IDX_RE = re.compile(ur'^\[?\d+(?:,\d+)*\]?$')

def _ustr(x):
    try:
        return unicode(x) if x is not None else u""
    except Exception:
        try:
            return unicode(str(x)) if x is not None else u""
        except Exception:
            return u""

def _nid_str(nid):
    """
    Normalize to a pure UA NodeId string like 'ns=1;s=[WC120A]...'
    Strip '[Ignition OPC UA Server]' if present. Accepts Java objects or strings.
    """
    try:
        if hasattr(nid, "toString"):
            nid = nid.toString()
    except Exception as e:
        log_warn("TagPublishingConfiguration::_nid_str", None, "toString() failed; using raw. %s" % e)
    s = _ustr(nid)
    prefix = u"[" + _OPC_SERVER_NAME + u"]"
    if s.startswith(prefix):
        s = s[len(prefix):]
    return s

def _try_to_string(v):
    try:
        return v.toString()
    except Exception:
        try:
            return _ustr(v)
        except Exception:
            return u""

def _raw_id(node):
    """
    Return an address from a browse result (Gateway/Designer safe).
    Try in order: getServerNodeId, getOpcItemPath, getItemPath; else str(node).
    """
    for getter in ("getServerNodeId", "getOpcItemPath", "getItemPath"):
        try:
            if hasattr(node, getter):
                v = getattr(node, getter)()
                s = _try_to_string(v)
                if s:
                    return s
        except Exception as e:
            log_warn("TagPublishingConfiguration::_raw_id", None, "%s() failed: %s" % (getter, e))
            continue
    return _ustr(node)

def _label(node):
    """
    Safe label across object types (LocalizedText, QualifiedName, etc).
    """
    try:
        if hasattr(node, "getDisplayName"):
            dn = node.getDisplayName()
            try:
                return _ustr(dn.getText())
            except Exception:
                return _ustr(dn)
    except Exception as e:
        log_warn("TagPublishingConfiguration::_label", None, "getDisplayName failed: %s" % e)
    try:
        if hasattr(node, "getName"):
            return _ustr(node.getName())
    except Exception as e:
        log_warn("TagPublishingConfiguration::_label", None, "getName failed: %s" % e)
    return _ustr(node)

def _browse(nid_str):
    try:
        return list(system.opc.browseServer(_OPC_SERVER_NAME, nid_str) or [])
    except Exception as e:
        log_warn("TagPublishingConfiguration::_browse", None, "OPC browse failed for '%s': %s" % (nid_str, e))
        return []

def _looks_like_java_obj(s):
    # e.g. "<com.inductiveautomation.ignition.gateway.script.GatewayOpcUtilities$PyOPCTagEx object at 0x...>"
    return s.startswith("<com.inductiveautomation.")

def _parse_ns_and_tail(node_id_str):
    # "ns=1;s=[WC120A]Program:Side_A" -> ("1", "[WC120A]Program:Side_A")
    s = _nid_str(node_id_str)
    if not s.startswith("ns="):  # fallback defaults
        return ("1", s)
    try:
        ns_part, tail = s.split(";s=", 1)
        ns = ns_part.split("=", 1)[1]
        return (ns, tail)
    except Exception as e:
        log_warn("TagPublishingConfiguration::_parse_ns_and_tail", None, "Parse failed: %s" % e)
        return ("1", s)

def _last_name(tail):
    """
    Return the last symbol in the tail (after device, after dots, after ':').
    e.g. '[WC120A]Program:Side_A.FLD' -> 'FLD'
    """
    after_dev = tail.split(u']', 1)[-1]
    if after_dev.startswith(u'.'):
        after_dev = after_dev[1:]
    if not after_dev:
        return u""
    last = after_dev.split(u'.')[-1]
    return last.split(u':')[-1]

def _synth_child_id(parent_node_id, label):
    """
    Build a child NodeId when browse didn't expose a real id.
    Handles Controller:Global, array indices, and names like 'FLD[0,0]'.
    """
    if not parent_node_id:
        return u"ns=1;s=" + _ustr(label)

    ns, tail = _parse_ns_and_tail(parent_node_id)
    tail = _ustr(tail)
    lbl  = _ustr(label)

    # Strip visual groups such as Controller:Global
    DISPLAY_GROUPS = (u"Controller:Global", u"Controller Tags", u"Controller", u"Global Variables", u"Global")
    for g in DISPLAY_GROUPS:
        if tail.endswith(g):
            tail = tail[: -len(g)]
            break

    # Device-root check: '[WCXXXX]'
    def _is_device_root(t):
        i = t.find(u']')
        return t.startswith(u'[') and i == len(t) - 1

    at_device_root = _is_device_root(tail)

    # Case 1: label is just an index (0 / 0,0 / [0,0]) -> append as brackets
    if _IDX_RE.match(lbl):
        idx = lbl.strip(u'[]')
        return u"ns=%s;s=%s[%s]" % (ns, tail, idx)

    # Case 2: label repeats the parent's base name + index, e.g. 'FLD[0,0]'
    base = _last_name(tail)
    if base and lbl.startswith(base + u"["):
        idx_part = lbl[len(base):]  # starts with '['
        return u"ns=%s;s=%s%s" % (ns, tail, idx_part)

    # Case 3: normal join
    joiner = u"" if (at_device_root or lbl.startswith(u"[") or (u":" in lbl)) else u"."
    return u"ns=%s;s=%s%s%s" % (ns, tail, joiner, lbl)

# ---------- Public API ----------

def device_roots_items(use_cache=True):
    now = _now()
    if use_cache:
        entry = _CACHE_DEVICES.get("root")
        if entry and (now - entry.get("t", 0) < _DEVICES_CACHE_TTL):
            return entry.get("items", [])

    kids = _browse(_DEVICES_ROOT)
    out = []
    for ch in kids:
        label = _label(ch)
        raw   = _nid_str(_raw_id(ch))
        if not raw or _looks_like_java_obj(raw):
            raw = _synth_child_id(None, label)  # devices are top-level
        out.append({"label": label, "id": raw, "items": []})

    _CACHE_DEVICES["root"] = {"t": now, "items": out}
    return out

def children_one_level_items(parent_node_id, use_cache=True, peek_children=True):
    parent_nid = _nid_str(parent_node_id)
    if not parent_nid:
        return []

    now = _now()
    if use_cache:
        entry = _CACHE_CHILDREN.get(parent_nid)
        if entry and (now - entry.get("t", 0) < _CHILDREN_CACHE_TTL):
            return entry.get("items", [])

    kids = _browse(parent_nid) or []

    def _base_sort_key(node):
        lbl = _label(node) or u""
        rid = _nid_str(_raw_id(node)) or u""
        return (lbl.lower(), rid.lower())

    kids_sorted = sorted(kids, key=_base_sort_key)

    entries = []
    for ch in kids_sorted:
        label = _label(ch)
        raw   = _nid_str(_raw_id(ch))
        if not raw or _looks_like_java_obj(raw):
            raw = _synth_child_id(parent_nid, label)

        kind = _kind(ch)
        has_kids = _has_children(raw) if peek_children else True
        entries.append({
            "label": label,
            "id": raw,
            "kind": kind,
            "leaf": (kind == u"VARIABLE"),   # hint only
            "expandable": has_kids,
            "items": []
        })

    # folders first, tags second — each alpha by label
    folders = [e for e in entries if not e["leaf"]]
    tags    = [e for e in entries if e["leaf"]]
    folders.sort(key=lambda e: (e["label"] or u"").lower())
    tags.sort(key=lambda e: (e["label"] or u"").lower())
    out = folders + tags

    _CACHE_CHILDREN[parent_nid] = {"t": now, "items": out}
    return out

# ---------- Debug helpers ----------

def device_roots_items_debug():
    items = device_roots_items(use_cache=False)
    return {"ok": True, "devicesCount": len(items),
            "firstDevice": (items[0]["label"] if items else None),
            "items": items}

def children_one_level_items_debug(parent_node_id):
    items = children_one_level_items(parent_node_id, use_cache=False)
    return {"ok": True, "itemsCount": len(items), "items": items}

def _kind(node):
    """
    Return a coarse kind string. Typical values:
      OBJECT / FOLDER / VARIABLE / UNKNOWN
    Works in Gateway & Designer.
    """
    try:
        if hasattr(node, "getElementType"):
            k = node.getElementType()
            return unicode(k).upper()
    except Exception as e:
        log_warn("TagPublishingConfiguration::_kind", None, "getElementType failed: %s" % e)
    try:
        if hasattr(node, "getType"):
            k = node.getType()
            return unicode(k).upper()
    except Exception as e:
        log_warn("TagPublishingConfiguration::_kind", None, "getType failed: %s" % e)
    return u"UNKNOWN"

_HAS_KIDS_CACHE = {}          # nid -> {"t": ts, "v": bool}
_HAS_KIDS_TTL   = 30          # seconds

def _has_children(nid_str):
    """Return True if node has >=1 child, with short TTL cache."""
    now = _now()
    e = _HAS_KIDS_CACHE.get(nid_str)
    if e and (now - e.get("t", 0) < _HAS_KIDS_TTL):
        return bool(e.get("v"))

    kids = _browse(nid_str) or []
    v = len(kids) > 0
    _HAS_KIDS_CACHE[nid_str] = {"t": now, "v": v}
    return v

def clear_opc_browse_caches():
    _CACHE_CHILDREN.clear()
    _CACHE_DEVICES.clear()
    log_info("TagPublishingConfiguration::clear_opc_browse_caches", None, "Cleared browse caches")