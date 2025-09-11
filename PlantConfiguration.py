# <summary>
# Created By: Akash J
# Creation Date: 07/17/2024
# Comments: Functions to scan PLCs and create ignition station instances (refactored for maintainability)
# </summary>

# ───── Imports ───────────────────────────────────────────────────────────
import system
from collections import namedtuple
from system.dataset import toDataSet
from MagnaDataOps.LoggerFunctions import log_info, log_warn, log_error

# Named return types to satisfy CodeGuru "multiple-values-in-return"
StationHierarchy = namedtuple("StationHierarchy", "area subarea line station")
StationStats     = namedtuple("StationStats", "is_turntable side_ids tt_flags fixture_counts notes")

# ───── Small utilities (keep Jython-safe) ────────────────────────────────
_CONN   = "Ignition OPC UA Server"
_OUTTAG = "[MagnaDataOps]InstanceSetup/StationMetaData"

_HEADERS = [
    "Area", "SubArea", "Line", "Station",
    "SideIDs", "IsTurntable", "FixtureCounts", "Status",
    "Device", "StationIDX"
]

def _safe_str(x):
    try:
        return unicode(x) if x is not None else u""
    except Exception:
        return unicode(str(x) if x is not None else u"")

def _read_safe(opc_path):
    """Return value if quality good, else None. Never throws."""
    try:
        qv = system.opc.readValue(_CONN, opc_path)
        return qv.value if qv.quality.isGood() else None
    except Exception:
        return None

def _boolish(v):
    """Loose bool parse used in original code."""
    s = _safe_str(v).strip().lower()
    return s in (u"true", u"1", u"yes")

def _find_valid_a_indices(device, max_a=50, miss_limit=3, probe_b_range=5):
    """Scan FLD[a,b].LineID to find active 'a' rows with early stop on consecutive misses."""
    valid_as = []
    consec_miss = 0
    for a in range(max_a):
        any_here = False
        for b in range(probe_b_range):
            p = "ns=1;s=[%s]FLD[%d,%d].LineID" % (device, a, b)
            if _read_safe(p) is not None:
                any_here = True
                break
        if any_here:
            valid_as.append(a)
            consec_miss = 0
        else:
            consec_miss += 1
            if consec_miss >= miss_limit:
                break
    return valid_as

def _collect_scan_indices(device, valid_as, probe_b_range=5):
    """Collect all (a,b) that have a non-null LineID. (Loop form—no heavy comprehension.)"""
    indices = []
    for a in valid_as:
        for b in range(probe_b_range):
            p = "ns=1;s=[%s]FLD[%d,%d].LineID" % (device, a, b)
            if _read_safe(p) is not None:
                indices.append((a, b))
    return indices

def _group_active_by_a(device, scan_indices):
    """
    Group active FLD roots by 'a' where LineID != 0.
    Returns {a: [(root_path, (a,b)), ...]} and a flag if any non-zero was found.
    """
    groups = {}
    found_non_zero = False
    for (a, b) in scan_indices:
        root = "ns=1;s=[%s]FLD[%d,%d]" % (device, a, b)
        lid = _read_safe(root + ".LineID")
        if lid and _safe_str(lid) != u"0":
            found_non_zero = True
            groups.setdefault(a, []).append((root, (a, b)))
    return groups, found_non_zero

def _resolve_station_hierarchy(station_name):
    """
    Resolve (AREA, SUBAREA, LINE, STATION) via Named Query.
    Returns StationHierarchy namedtuple. Never throws; logs warn on failure.
    """
    AREA = SUBAREA = LINE = u"Unknown"
    STATION = station_name

    try:
        ds = system.db.runNamedQuery(
            "MagnaDataOps/Configuration/PlantConfiguration/HierarchyConfiguration/Additional/getSelectedStationHierarchy",
            {"station_name": station_name}
        )
        if ds and ds.getRowCount() > 0:
            row = system.dataset.toPyDataSet(ds)[0]
            AREA    = row["area_name_clean"]
            SUBAREA = row["subarea_name_clean"]
            LINE    = row["line_name_clean"]
            STATION = row["station_name_clean"]
    except Exception as e:
        log_warn("plantConfiguration::scanStationMetadata.resolveHierarchy", "NA",
                 "Hierarchy lookup failed for '%s': %s" % (_safe_str(station_name), _safe_str(e)))
    return StationHierarchy(area=AREA, subarea=SUBAREA, line=LINE, station=STATION)

def _calc_station_stats(active_roots):
    """
    Detect if turntable, extract sideIDs and fixture counts.
    Mirrors original logic:
    - If not TT → treat as standard with fx = len(active_roots)
    - If TT but total fixtures == 0 → convert to standalone (like original)

    Returns StationStats namedtuple.
    """
    # Turntable detection
    is_tt = False
    for (fld, _) in active_roots:
        if _boolish(_read_safe(fld + ".IsTurnTableStation")):
            is_tt = True
            break

    side_ids, tt_flags, fx_counts, notes = [], [], [], []
    total_fx = 0

    if not is_tt:
        side_ids = ["0"]
        tt_flags = ["False"]
        fx_counts = [str(len(active_roots))]
        notes.append("std fx=%d" % len(active_roots))
        return StationStats(is_turntable=is_tt, side_ids=side_ids, tt_flags=tt_flags,
                            fixture_counts=fx_counts, notes=notes)

    # is turntable → per root side & fixture scan
    for (fld, (xx, yy)) in active_roots:
        side = _read_safe(fld + ".SideID")
        side = _safe_str(side) if side else "0"

        fx = 0
        # cap probe to 5 as in original
        for i in range(5):
            sn = _read_safe(fld + ".Fixtures[%d].Serial_Number_1" % i)
            if sn is not None:
                fx += 1
            else:
                break

        side_ids.append(side)
        tt_flags.append("True")
        fx_counts.append(str(fx))
        notes.append("FLD[%d,%d]:side=%s fx=%d" % (xx, yy, side, fx))
        total_fx += fx

    if total_fx == 0:
        # Convert to standalone exactly like the original
        side_ids = ["0"]
        tt_flags = ["False"]
        fx_counts = [str(len(active_roots))]
        notes = ["converted to standalone"]

    return StationStats(is_turntable=is_tt, side_ids=side_ids, tt_flags=tt_flags,
                        fixture_counts=fx_counts, notes=notes)

def _determine_type_id(is_turntable, fx_counts_csv):
    """
    Compute UDT typeId using the same branching logic as original.
    Caps fixtures per side to 3.
    """
    fx_list = []
    for x in (fx_counts_csv or "").split(","):
        x = x.strip()
        if x.isdigit():
            fx_list.append(int(x))
    side_cnt = len(fx_list)

    if not is_turntable:
        fx_count = min(sum(fx_list), 3)
        return "StationUDT/Station_Flat_%d" % fx_count, side_cnt

    if side_cnt == 1:
        fx = min(fx_list[0], 3)
        return "StationUDT/Station_TT_1_%d" % fx, side_cnt
    if side_cnt == 2:
        fx1, fx2 = min(fx_list[0], 3), min(fx_list[1], 3)
        return "StationUDT/Station_TT_2_%d_%d" % (fx1, fx2), side_cnt

    return None, side_cnt  # invalid side count


# ───── Function: scanStationMetadata ─────────────────────────────────────
def scanStationMetadata(deviceList, user_id):
    """
    Scans each device's FLD[a,b] nodes via OPC UA.
    Resolves hierarchy using station name via Named Query.
    Writes final dataset to configured memory tag.
    Return codes preserved from original:
      903 = wrote rows, good
      904 = nothing created (kept for symmetry)
      906 = partial skip (kept for symmetry)
      901 = warning / no input or no data
      905 = error
    """
    try:
        if not deviceList:
            log_warn("plantConfiguration::scanStationMetadata", user_id, "No device list provided.")
            return 901

        rows = []

        for device in deviceList:
            # quick probe: must have at least FLD[0,0].LineID
            if _read_safe("ns=1;s=[%s]FLD[0,0].LineID" % device) is None:
                rows.append(["Unknown", "Unknown", "Unknown", device, "", "", "", "ERROR: primary missing", device, "0"])
                continue

            valid_as = _find_valid_a_indices(device)
            scan_indices = _collect_scan_indices(device, valid_as)
            if not scan_indices:
                rows.append(["Unknown", "Unknown", "Unknown", device, "", "", "", "ERROR: no FLD[*] found", device, "0"])
                continue

            groups, found_non_zero = _group_active_by_a(device, scan_indices)
            if not found_non_zero:
                rows.append(["Unknown", "Unknown", "Unknown", device, "", "", "", "ERROR: all LineID=0", device, "0"])
                continue

            subCount = 0
            for a in sorted(groups):
                active = groups[a]
                subCount += 1
                stationIDX = _safe_str(a)

                # Prefer StationID from any active root
                stationID = None
                for (fld, _) in active:
                    val = _read_safe(fld + ".StationID")
                    if val and _safe_str(val).strip():
                        stationID = _safe_str(val).strip()
                        break

                # Fallback naming logic mirrors original
                stationName = stationID or (device if len(groups) == 1 else (device + "_" + _safe_str(subCount)))

                # Resolve hierarchy; keep Unknowns on failure
                if stationID:
                    h = _resolve_station_hierarchy(stationName)
                    AREA, SUBAREA, LINE, stationName = h.area, h.subarea, h.line, h.station
                else:
                    AREA, SUBAREA, LINE = "Unknown", "Unknown", "Unknown"

                # Calculate station stats
                stats = _calc_station_stats(active)
                is_tt      = stats.is_turntable
                side_ids   = stats.side_ids
                tt_flags   = stats.tt_flags
                fx_counts  = stats.fixture_counts
                notes      = stats.notes

                status = "OK: " + "; ".join(notes)

                rows.append([
                    AREA, SUBAREA, LINE, stationName,
                    ",".join(side_ids),
                    ",".join(tt_flags),
                    ",".join(fx_counts),
                    status, device, stationIDX
                ])

        if rows:
            ds = toDataSet(_HEADERS, rows)
            system.tag.writeBlocking([_OUTTAG], [ds])
            log_info("plantConfiguration::scanStationMetadata", user_id,
                     "✅ %d rows written to %s" % (len(rows), _OUTTAG))
            return 903

        log_warn("plantConfiguration::scanStationMetadata", user_id, "No station data collected")
        return 901

    except Exception:
        # Our project logger logs stack trace via exc_info internally
        log_error("plantConfiguration::scanStationMetadata", user_id)
        return 905


# ───── Function: createStationUDTs ───────────────────────────────────────
def createStationUDTs(user_id):
    """
    Creates Station UDT instances from StationMetaData dataset.
    Preserves original behavior and return codes.
    """
    basePath     = "[MagnaDataOps]/MagnaStations"
    udtBasePath  = "StationUDT"
    dataTagPath  = _OUTTAG

    try:
        ds = system.tag.readBlocking([dataTagPath])[0].value
        if ds is None or ds.rowCount == 0:
            log_warn("plantConfiguration::StationUDTCreate", user_id, "No station metadata to process.")
            return 901

        total, skipped, created = ds.rowCount, 0, 0

        for row in range(total):
            areaRaw     = ds.getValueAt(row, "Area")
            subAreaRaw  = ds.getValueAt(row, "SubArea")
            lineRaw     = ds.getValueAt(row, "Line")
            station     = ds.getValueAt(row, "Station")
            sideIDs     = ds.getValueAt(row, "SideIDs") or ""
            isTT        = ds.getValueAt(row, "IsTurntable") or ""
            fxCounts    = ds.getValueAt(row, "FixtureCounts") or ""
            device      = ds.getValueAt(row, "Device")
            stationIdx  = ds.getValueAt(row, "StationIDX")

            area    = areaRaw    if _safe_str(areaRaw).lower()    != "unknown" else "Unknown"
            subArea = subAreaRaw if _safe_str(subAreaRaw).lower() != "unknown" else "Unknown"
            line    = lineRaw    if _safe_str(lineRaw).lower()    != "unknown" else "Unknown"

            stationFolderPath = "[%s]" % device
            if area == "Unknown":
                tagPath = "%s/Unknown/%s" % (basePath, station)
            else:
                tagPath = "%s/%s/%s/%s/%s" % (basePath, area, subArea, line, station)

            tagExists = system.tag.exists(tagPath)

            is_turntable = ("true" in _safe_str(isTT).lower())
            typeId, side_cnt = _determine_type_id(is_turntable, fxCounts)

            if typeId is None:
                log_warn("plantConfiguration::StationUDTCreate", user_id,
                         "Skipping station %s due to invalid side count (%d)." % (station, side_cnt))
                skipped += 1
                continue

            tagTree = {
                "name": _safe_str(station),
                "tagType": "UdtInstance",
                "typeId": typeId,
                "parameters": {
                    "NS": "1",
                    "StationFolderPath": stationFolderPath,
                    "StationIdx": _safe_str(stationIdx)
                }
            }

            # Build folders exactly as before
            if area == "Unknown":
                folderTree = {"name": "Unknown", "tagType": "Folder", "tags": [tagTree]}
            else:
                folderTree = {
                    "name": area, "tagType": "Folder", "tags": [{
                        "name": subArea, "tagType": "Folder", "tags": [{
                            "name": line, "tagType": "Folder", "tags": [tagTree]
                        }]
                    }]
                }

            system.tag.configure(basePath=basePath, tags=[folderTree], collisionPolicy="m")

            if not tagExists:
                log_info("plantConfiguration::createStationUDTs", user_id,
                         "Created UDT: %s (%s)" % (station, typeId))
                created += 1
            else:
                skipped += 1

        log_info("plantConfiguration::createStationUDTs", user_id,
                 "UDT creation summary: %d created, %d skipped, out of %d" % (created, skipped, total))

        if created == 0:
            return 904
        elif skipped > 0:
            return 906
        else:
            return 903

    except Exception:
        log_error("plantConfiguration::createStationUDTs", user_id)
        return 905