# <summary>
# Module Name : Forms
# Description : HTML/JSON preview helpers (pretty JSON with line numbers, date coercion).
# Author      : Akash J
# Created On  : 2025-06-22
# </summary>

import system
from cgi import escape
from collections import OrderedDict
from json import dumps, loads
from re import compile as re_compile
from system.util import jsonEncode
from java.text import SimpleDateFormat
from java.util import TimeZone, Date
from system.date import now

from MagnaDataOps.LoggerFunctions import log_info, log_warn, log_error

MODULE = "Forms"

# ISO formatter (UTC)
ISO_FMT = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
ISO_FMT.setTimeZone(TimeZone.getTimeZone("UTC"))

# Parse formats (UTC)
PARSE_FMTS = [
    SimpleDateFormat("MMM d, yyyy, h:mm:ss a"),      # Aug 14, 2025, 9:01:18 AM
    SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"),       # 2025-08-14T09:01:18
    SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"),    # 2025-08-14T09:01:18Z
    SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),         # 2025-08-14 09:01:18
]
for fmt in PARSE_FMTS:
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"))

# Local-offset timestamp for header
OFFSET_TS_FMT = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
OFFSET_TS_FMT.setTimeZone(TimeZone.getDefault())

# Recognize already-ISO strings so we don't re-parse (handles .SSS and zones)
ISO_LIKE_RE = re_compile(
    r'^\d{4}-\d{2}-\d{2}T'
    r'\d{2}:\d{2}:\d{2}'
    r'(?:\.\d{3})?'                # optional .SSS
    r'(?:Z|[+\-]\d{2}:\d{2})?$'    # optional zone
)

# ---------- helpers ----------
def to_iso(dt=None):
    """Format a java.util.Date as ISO-8601 UTC string."""
    try:
        return ISO_FMT.format(dt if dt else now())
    except Exception:
        log_error(MODULE + "::to_iso")
        return "1970-01-01T00:00:00.000"

def try_parse_date(val):
    """Try multiple formats; raise ValueError if none match (no 'except/continue')."""
    last_err = None
    for fmt in PARSE_FMTS:
        try:
            return fmt.parse(val)
        except Exception as e:
            last_err = e
            # loop proceeds naturally
    log_error(MODULE + "::try_parse_date | Unparseable date: {}".format(unicode(val)))
    raise ValueError("Unparseable date: " + unicode(val))

# Recursively process structure and convert date strings
MAX_DEPTH = 1000
def convert_dates(node, depth=0):
    if depth > MAX_DEPTH:
        raise RuntimeError("Max recursion depth exceeded")

    try:
        text_types = (basestring,)
    except NameError:
        text_types = (str,)

    if isinstance(node, dict):
        for k, v in node.items():
            kl = k.lower()
            if isinstance(v, text_types) and kl != "timestamp" and ("date" in kl or "time" in kl):
                try:
                    # Leave ISO-like values as-is (prevents ms parse failures)
                    if ISO_LIKE_RE.match(v):
                        node[k] = v
                    else:
                        node[k] = to_iso(try_parse_date(v))
                except Exception as e:
                    log_error(MODULE + "::convert_dates | {}".format(unicode(e)))
                    node[k] = to_iso()
            elif isinstance(v, (dict, list)):
                convert_dates(v, depth + 1)
    elif isinstance(node, list):
        for item in node:
            convert_dates(item, depth + 1)

# ========== Main Entry Point ==========
def renderJsonPreview(payload, desired_order, isStatusPayload=False, isOvercyclePayload=False):
    """
    payload: dict/list OR JSON string
    desired_order: list of keys, with optional nested dicts like:
        {"fixtures": ["FixtureID","Part_Number",...]}                      # reorder fields only
        {"fixtures": {"fields":[...], "sortBy":"FixtureID","sortDir":"asc"}}  # reorder + sequence
        # alias: "sequenceBy" may be used instead of "sortBy"
    isStatusPayload: if True, render payload as-is (no Version/Timestamp injection, no reordering, no date conversion)
    isOvercyclePayload: if True, same as isStatusPayload (as-is rendering), but with title 'Overcycle Payload'
    """
    KEY_COLOR = "#2F54EB"
    VAL_COLOR = "#D97706"
    DEF_COLOR = "#16A34A"
    WEIGHT    = "500"
    INDENT    = 8
    BADGE_CSS = (
        "display:inline-block;width:16px;height:16px;border-radius:50%;"
        "background:#E0E7FF;color:#2F54EB;font-size:10px;line-height:16px;"
        "text-align:center;margin-right:4px;font-weight:{w}".format(w=WEIGHT)
    )

    # Normalize to OrderedDict / list (preserves existing order of incoming JSON)
    incoming = loads(jsonEncode(payload), object_pairs_hook=OrderedDict)
    as_is_mode = bool(isStatusPayload or isOvercyclePayload)

    if as_is_mode:
        # --- AS-IS path: do not mutate content at all ---
        root = incoming
    else:
        # --- Original behavior path ---
        root = OrderedDict([("Version", "1.0.0")])

        # Move timestamp (if any) to the top (keep original value for now)
        ts_key = next((k for k in incoming if k.lower() == "timestamp"), None)
        if ts_key:
            root["Timestamp"] = incoming.pop(ts_key)

        # Parse desired ordering (now supports optional sequencing for lists)
        nested_orders, flat_order = {}, []
        for item in desired_order:
            if isinstance(item, dict):
                # could be {"fixtures":[...]} OR {"fixtures":{"fields":[...],"sortBy":"FixtureID","sortDir":"asc"}}
                for outer_key, spec in item.items():
                    nested_orders[outer_key] = spec
            else:
                flat_order.append(item)

        # Build output structure in desired order
        for k in flat_order:
            if k in incoming:
                root[k] = incoming[k]
        for k in incoming:
            if k not in root:
                root[k] = incoming[k]

        # Reorder and optionally sequence nested lists by desired spec
        for outer_key, spec in nested_orders.items():
            val = root.get(outer_key)
            if not isinstance(val, list):
                continue

            # Normalize spec
            if isinstance(spec, dict):
                fields   = list(spec.get("fields") or [])
                sort_by  = spec.get("sortBy") or spec.get("sequenceBy")
                sort_dir = (spec.get("sortDir") or "asc").lower()
                reverse  = (sort_dir == "desc")
            else:
                fields   = list(spec or [])
                sort_by  = None
                reverse  = False

            # Optional sequencing (sorting) by sort_by/sequenceBy
            if sort_by:
                def _key(it):
                    if isinstance(it, dict) and sort_by in it:
                        v = it.get(sort_by)
                        try:
                            return (0, int(v))
                        except Exception:
                            try:
                                return (0, float(v))
                            except Exception:
                                return (0, unicode(v))
                    return (1, u"")  # items without the key go after those with the key
                try:
                    val = sorted(val, key=_key, reverse=reverse)
                except Exception:
                    log_warn(MODULE + "::renderJsonPreview", None, "Sorting skipped for key '{}'".format(sort_by))

            # Reorder fields inside each dict entry
            if fields:
                new_list = []
                for entry in val:
                    if isinstance(entry, dict):
                        ordered = OrderedDict((nk, entry[nk]) for nk in fields if nk in entry)
                        for kk, vv in entry.items():
                            if kk not in ordered:
                                ordered[kk] = vv
                        new_list.append(ordered)
                    else:
                        new_list.append(entry)
                root[outer_key] = new_list
            else:
                root[outer_key] = val

        # Convert date/time-looking fields (except explicit "timestamp") to ISO
        convert_dates(root)

        # Overwrite/insert Timestamp with local offset format (original behavior)
        root["Timestamp"] = OFFSET_TS_FMT.format(Date())

    # Pretty-print JSON (no content changes)
    pretty = dumps(root, indent=INDENT, separators=(",", ": "))

    # --- syntax highlighting (keys/values) ---
    key_re = re_compile(r'^(\s*)"([^"]+)":')
    val_re = re_compile(r':\s*("(?:[^"\\]|\\.)*"|\d+(?:\.\d+)?|true|false|null)(,?)$')

    badge_ct, html_lines = {}, []
    for ln in pretty.splitlines():
        stripped = ln.strip()
        depth    = (len(ln) - len(stripped)) // INDENT

        if stripped == "{":
            badge_ct[depth] = 0
        elif stripped == "}":
            badge_ct.pop(depth, None)

        km = key_re.match(ln)
        if km:
            key_txt = escape(km.group(2), True)
            num     = badge_ct.setdefault(depth, 0) + 1
            badge_ct[depth] = num
            badge = "<span style='{css}'>{n}</span>".format(css=BADGE_CSS, n=num)
            # Only special-color Version/Timestamp in mutated mode
            special_keys = ("version", "timestamp")
            colour = DEF_COLOR if (not as_is_mode and key_txt.lower() in special_keys) else KEY_COLOR
            key_html = '{b}<span style="color:{c};font-weight:{w};">"{k}"</span>:'.format(
                b=badge, c=colour, w=WEIGHT, k=key_txt
            )
            ln = " " * (depth * INDENT) + key_html + ln[km.end():]

        def repl(m):
            raw, tail = m.group(1), m.group(2)
            safe = escape(raw, True)
            return ': <span style="color:{col};font-weight:{w};">{txt}</span>{t}'.format(
                col=VAL_COLOR, w=WEIGHT, txt=safe, t=tail
            )

        ln = val_re.sub(repl, ln)
        html_lines.append(ln)

    # Gutter with line numbers
    gutter_css = (
        "display:inline-block;width:2em;font-size:0.7em;"
        "color:#AAA;text-align:right;padding-right:6px;"
        "border-right:1px solid #DDD;margin-right:8px;"
    )
    line_css = (
        "line-height:1.8;letter-spacing:0.4px;white-space:pre;"
        "border-bottom:1px solid #EEE;padding:2px 0;"
    )
    rows = [
        "<div style='{lc}'><span style='{gc}'>{r}</span>{code}</div>"
        .format(lc=line_css, gc=gutter_css, r=i, code=code)
        for i, code in enumerate(html_lines, 1)
    ]

    # Titles
    if isStatusPayload:
        title_text = "Status Payload View"
    elif isOvercyclePayload:
        title_text = "Overcycle Payload"
    else:
        title_text = "Payload Preview"

    container = (
        "<div style='font-family:Aptos,monospace;font-size:1.05em;"
        "font-weight:300;color:#333;padding:6px;'>"
        "<div style='font-size:1.1em;font-weight:bold;margin-bottom:6px;'>{title}</div>"
        .format(title=title_text)
        + "\n".join(rows) +
        "</div>"
    )
    return container