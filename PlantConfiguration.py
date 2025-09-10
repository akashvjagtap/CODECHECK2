# <summary>
# Created By: Akash J
# Creation Date: 07/17/2024
# Comments: Functions to scan PLCs and create ignition station instances
# </summary>

# ───── Imports ───────────────────────────────────────────────────────────
import system
from system.dataset import toDataSet
from MagnaDataOps.LoggerFunctions import log_info, log_warn, log_error


# ───── Function: scanStationMetadata ─────────────────────────────────────
def scanStationMetadata(deviceList,user_id):
    """
    Scans each device's FLD[a,b] nodes via OPC UA.
    Resolves hierarchy using station name via Named Query.
    Writes final dataset to configured memory tag.
    """
    CONN = "Ignition OPC UA Server"
    OUT_TAG = "[MagnaDataOps]InstanceSetup/StationMetaData"
    headers = [
        "Area", "SubArea", "Line", "Station",
        "SideIDs", "IsTurntable", "FixtureCounts", "Status",
        "Device", "StationIDX"
    ]
    rows = []

    def readSafe(path, desc, recordDiag=True):
        try:
            qv = system.opc.readValue(CONN, path)
            return qv.value if qv.quality.isGood() else None
        except:
            return None

    try:
        if not deviceList:
            log_warn("plantConfiguration::scanStationMetadata",user_id, "No device list provided.")
            return 901

        for device in deviceList:
            diag = []
            primaryPath = "ns=1;s=[%s]FLD[0,0].LineID" % device
            if readSafe(primaryPath, "primary LineID") is None:
                rows.append(["Unknown", "Unknown", "Unknown", device, "", "", "", "ERROR: primary missing", device, "0"])
                continue

            validAs, consecMiss, MAX_A, MISS_LIMIT = [], 0, 50, 3
            for a in range(MAX_A):
                anyHere = any(readSafe("ns=1;s=[%s]FLD[%d,%d].LineID" % (device, a, b), "", False) for b in range(5))
                if anyHere:
                    validAs.append(a)
                    consecMiss = 0
                else:
                    consecMiss += 1
                    if consecMiss >= MISS_LIMIT:
                        break

            scanIndices = [
                (a, b) for a in validAs for b in range(5)
                if readSafe("ns=1;s=[%s]FLD[%d,%d].LineID" % (device, a, b), "", False) is not None
            ]

            if not scanIndices:
                rows.append(["Unknown", "Unknown", "Unknown", device, "", "", "", "ERROR: no FLD[*] found", device, "0"])
                continue

            groups, foundNonZero = {}, False
            for (a, b) in scanIndices:
                root = "ns=1;s=[%s]FLD[%d,%d]" % (device, a, b)
                lid = readSafe(root + ".LineID", "")
                if lid and str(lid) != "0":
                    foundNonZero = True
                    groups.setdefault(a, []).append((root, (a, b)))

            if not foundNonZero:
                rows.append(["Unknown", "Unknown", "Unknown", device, "", "", "", "ERROR: all LineID=0", device, "0"])
                continue

            subCount = 0
            for a in sorted(groups):
                active = groups[a]
                subCount += 1
                stationIDX = str(a)
                stationID = None
                for (fld, _) in active:
                    val = readSafe(fld + ".StationID", "")
                    if val and str(val).strip():
                        stationID = str(val).strip()
                        break

                stationName = stationID or (device if len(groups) == 1 else device + "_" + str(subCount))
                AREA, SUBAREA, LINE = "Unknown", "Unknown", "Unknown"

                if stationID:
                    try:
                        hierarchy = system.db.runNamedQuery(
                            "MagnaDataOps/Configuration/PlantConfiguration/HierarchyConfiguration/Additional/getSelectedStationHierarchy",
                            {"station_name": stationName}
                        )
                        if hierarchy.getRowCount() > 0:
                            row = system.dataset.toPyDataSet(hierarchy)[0]
                            AREA = row["area_name_clean"]
                            SUBAREA = row["subarea_name_clean"]
                            LINE = row["line_name_clean"]
                            stationName = row["station_name_clean"]
                    except:
                        pass

                isTT = any(readSafe(fld + ".IsTurnTableStation", "") in [True, 1, "True", "true"] for (fld, _) in active)
                sideIDs, ttFlags, fxCounts, notes, totFx = [], [], [], [], 0

                if not isTT:
                    sideIDs = ["0"]
                    ttFlags = ["False"]
                    fxCounts = [str(len(active))]
                    notes.append("std fx=%d" % len(active))
                else:
                    for (fld, (xx, yy)) in active:
                        side = readSafe(fld + ".SideID", "") or "0"
                        fx = 0
                        for i in range(5):
                            if readSafe(fld + ".Fixtures[%d].Serial_Number_1" % i, "", i == 0):
                                fx += 1
                            else:
                                break
                        sideIDs.append(str(side))
                        ttFlags.append("True")
                        fxCounts.append(str(fx))
                        notes.append("FLD[%d,%d]:side=%s fx=%d" % (xx, yy, side, fx))
                        totFx += fx

                    if totFx == 0:
                        sideIDs = ["0"]
                        ttFlags = ["False"]
                        fxCounts = [str(len(active))]
                        notes = ["converted to standalone"]

                rows.append([
                    AREA, SUBAREA, LINE, stationName,
                    ",".join(sideIDs), ",".join(ttFlags),
                    ",".join(fxCounts), "OK: " + "; ".join(notes),
                    device, stationIDX
                ])

        if rows:
            ds = toDataSet(headers, rows)
            system.tag.writeBlocking([OUT_TAG], [ds])
            log_info("plantConfiguration::scanStationMetadata",user_id,"✅ %d rows written to %s" % (len(rows), OUT_TAG))
            return 903
        else:
            log_warn("plantConfiguration::scanStationMetadata",user_id,"No station data collected")
            return 901

    except:
        log_error("plantConfiguration::scanStationMetadata",user_id)
        return 905


# ───── Function: createStationUDTs ───────────────────────────────────────
def createStationUDTs(user_id):
    basePath = "[MagnaDataOps]/MagnaStations"
    udtBasePath = "StationUDT"
    dataTagPath = "[MagnaDataOps]InstanceSetup/StationMetaData"
    screenName = "StationUDTCreate"
    moduleName = "MagnaDataOps"

    try:
        ds = system.tag.readBlocking([dataTagPath])[0].value
        if ds.rowCount == 0:
            log_warn("plantConfiguration::StationUDTCreate",user_id, "No station metadata to process.")
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
            stationIdx  = ds.getValueAt(row, "stationIDX")

            area    = areaRaw if str(areaRaw).lower() != "unknown" else "Unknown"
            subArea = subAreaRaw if str(subAreaRaw).lower() != "unknown" else "Unknown"
            line    = lineRaw if str(lineRaw).lower() != "unknown" else "Unknown"

            stationFolderPath = "[%s]" % device
            tagPath = "%s/%s/%s/%s/%s" % (basePath, area, subArea, line, station) if area != "Unknown" else "%s/Unknown/%s" % (basePath, station)
            tagExists = system.tag.exists(tagPath)

            isTurntable = "True" in isTT.split(",")
            fxList = [int(x) for x in fxCounts.split(",") if x.isdigit()]
            sideCnt = len(fxList)

            if not isTurntable:
                fxCount = min(sum(fxList), 3)
                typeId = "%s/Station_Flat_%d" % (udtBasePath, fxCount)
            elif sideCnt == 1:
                fx = min(fxList[0], 3)
                typeId = "%s/Station_TT_1_%d" % (udtBasePath, fx)
            elif sideCnt == 2:
                fx1, fx2 = min(fxList[0], 3), min(fxList[1], 3)
                typeId = "%s/Station_TT_2_%d_%d" % (udtBasePath, fx1, fx2)
            else:
                log_warn("plantConfiguration::StationUDTCreate",user_id, "Skipping station %s due to invalid side count (%d)." % (station, sideCnt))
                skipped += 1
                continue

            tagTree = {
                "name": station,
                "tagType": "UdtInstance",
                "typeId": typeId,
                "parameters": {
                    "NS": "1",
                    "StationFolderPath": stationFolderPath,
                    "StationIdx": stationIdx
                }
            }

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
                log_info("plantConfiguration::createStationUDTs",user_id, "Created UDT: %s (%s)" % (station, typeId))
                created += 1
            else:
                skipped += 1

        log_info("plantConfiguration::createStationUDTs",user_id, "UDT creation summary: %d created, %d skipped, out of %d" % (created, skipped, total))

        if created == 0:
            return 904
        elif skipped > 0:
            return 906
        else:
            return 903

    except:
        log_error("plantConfiguration::createStationUDTs",user_id)
        return 905