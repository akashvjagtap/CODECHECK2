# OvercyclePublisher.py — shift-to-date OCT, cumulative per station/shift
# Publishes every run. Closes & publishes just-ended shifts first (incl. overnight),
# then publishes current active shift. Uses MagnaDataOps logger utilities.

import system
from MagnaDataOps.LoggerFunctions import log_info, log_warn, log_error

MBASE = "MagnaDataOps/Dashboard/AllProductionData/OvercycleInsertPublish/"

WINDOW_MIN = 15
_MAX_TOP   = 5
EPSILON    = 0.00
_FINAL_GRACE_MIN = 18 * 60   # finalize shifts that ended within this many minutes

# ----------------------- small utils -----------------------
def _u(x):
    try: return unicode(x)
    except: return unicode(str(x) if x is not None else u"")

def _iso_sql(d): return system.date.format(d, "yyyy-MM-dd HH:mm:ss.SSS")
def _iso_off(d): return system.date.format(d, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
def _fmt_mmss(sec):
    sec = int(max(0, round(float(sec or 0))));  return "%d:%02d" % (sec//60, sec%60)

def _rowdicts(ds):
    if not ds: return []
    try:
        cols=list(ds.getColumnNames()); out=[]
        for i in range(ds.getRowCount()):
            d={}; 
            for c in cols: d[c]=ds.getValueAt(i,c)
            out.append(d)
        return out
    except: return list(ds or [])

def _san(s): return (s or u"").replace(" ","")

def _LI(where, msg): 
    try: log_info(where, 0, _u(msg))
    except: pass

def _LW(where, msg):
    try: log_warn(where, 0, _u(msg))
    except: pass

def _LE(where, e):
    try: log_error(where, 0, "Exception: %s" % _u(e))
    except: pass

# ----------------------- hierarchy / topics -----------------------
def _get_hierarchy(station_ids):
    try:
        ds = system.db.runNamedQuery(MBASE + "getHierarchyForStations",
                                     {"station_ids_csv": ",".join(str(int(s)) for s in station_ids)})
        H={}
        for r in _rowdicts(ds):
            sid=int(r["station_id"])
            H[sid]={
              "division":  _u(r.get("division_name_clean") or r.get("division_name") or "NA"),
              "plant":     _u(r.get("location_clean")     or r.get("plant_name")   or "Plant"),
              "area":      _u(r.get("area_name_clean")    or r.get("area_name")    or "Area"),
              "subarea":   _u(r.get("subarea_name_clean") or r.get("subarea_name") or "SubArea"),
              "line":      _u(r.get("line_name_clean")    or r.get("line_name")    or "Line"),
              "line_id":   int(r.get("line_id") or 0),
              "station":   _u(r.get("station_name_clean") or r.get("station_name") or ("Station_%d"%sid)),
            }
        return H
    except Exception as e:
        _LE("OCT::_get_hierarchy", e);  return {}

def _san_name(h, key, default): return _san((h.get(key) if h else None) or default)

def _topic_for_line(h, scope_slug):
    return "m/%s/%s/%s/%s/line/%s/%s" % (
        _san_name(h,"division","NA"), _san_name(h,"plant","Plant"),
        _san_name(h,"area","Area"), _san_name(h,"subarea","SubArea"),
        _san_name(h,"line","Line"), scope_slug
    )

def _publish(topic, obj, qos=0, retain=False):
    try:
        import MagnaDataOps.ProductionPublisher as PUB
        if hasattr(PUB,"_publish"):
            PUB._publish(topic,obj,qos,retain,None)
            _LI("OCT::publish", "Published via ProductionPublisher to %s" % topic)
            return
    except Exception as e:
        _LW("OCT::publish", "ProductionPublisher not used: %s" % _u(e))
    try:
        payload=system.util.jsonEncode(obj).encode("utf-8")
        system.cirruslink.engine.publish("Local Broker", topic, payload, int(qos), bool(retain))
        _LI("OCT::publish", "Published via CirrusLink to %s" % topic)
    except Exception as e:
        _LE("OCT::publish(CirrusLink)", e)

# ----------------------- stations & history -----------------------
def _load_stations():
    try:
        ds = system.db.runNamedQuery(MBASE + "getActiveStationsForOvercycle", {})
    except Exception as e:
        _LE("OCT::_load_stations", e);  ds=[]
    out=[]
    for r in _rowdicts(ds):
        try:
            out.append({
                "station_id": int(r["station_id"]),
                "line_id":    int(r["line_id"]),
                "area":       _u(r["area_name"]),
                "subarea":    _u(r["subarea_name"]),
                "line":       _u(r["line_name"]),
                "station":    _u(r["station_name"]),
            })
        except: continue
    return out

def _root(st):
    return u"[MagnaDataOps]MagnaStations/%s/%s/%s/%s" % (st["area"], st["subarea"], st["line"], st["station"])

def _tag_exists(p):
    try: return bool(system.tag.exists(p))
    except: return False

def _merge_histories(datasets):
    """Merge multiple 'Wide' datasets with same 2 columns [ts, value] into a single time-ordered list of (ts, value)."""
    rows=[]
    for ds in datasets:
        if not ds: continue
        try:
            for i in range(ds.getRowCount()):
                rows.append((ds.getValueAt(i,0), ds.getValueAt(i,1)))
        except: pass
    rows.sort(key=lambda t: system.date.toMillis(t[0]))
    # back into a Dataset (optional) – but we can just return the list and handle it in compute
    return rows

def _query_ct_history(st, start, end):
    """
    Returns the raw (Wide) history dataset for <station>/CycleTime between
    [start, end]. If the tag doesn't exist or has 0 rows, returns None.
    """
    try:
        sid = int(st["station_id"])
        p = _root(st) + u"/CycleTime"

        if not _tag_exists(p):
            _LW("OCT::_query_ct_history", "sid=%d path missing: %s" % (sid, _u(p)))
            return None

        ds = system.tag.queryTagHistory(
            paths=[p],
            startDate=start,
            endDate=end,
            returnAggregated=False,
            returnFormat='Wide',
            includeBoundingValues=False
        )

        if ds and ds.getRowCount() > 0:
            _LI("OCT::_query_ct_history", "sid=%d found %d rows at %s" % (sid, ds.getRowCount(), _u(p)))
            return ds

        _LW("OCT::_query_ct_history",
            "sid=%d no rows for %s in [%s → %s]" %
            (sid, _u(p), _iso_sql(start), _iso_sql(end)))
        return None

    except Exception as e:
        _LE("OCT::_query_ct_history", e)
        return None

def _ct_segments(sid, start_ts, end_ts):
    try:
        ds = system.db.runNamedQuery(MBASE + "getCtSegmentsForStationBetween",
                                     {"station_id": int(sid), "start_utc": start_ts, "end_utc": end_ts})
    except Exception as e:
        _LE("OCT::_ct_segments", e); ds=None
    segs=[]
    for r in _rowdicts(ds):
        try:
            ct   = float(r.get("ct_eff_sec") or 0.0)
            mult = float(r.get("overcycle_multiplier") or 2.0)
            segs.append((r["effective_from_utc"], ct, mult))
        except: continue
    segs.sort(key=lambda t: system.date.toMillis(t[0]))
    return segs

def _ct_at(ts, segs, i_hint):
    tms = system.date.toMillis(ts)
    i = i_hint if 0 <= i_hint < len(segs) else 0
    while i+1 < len(segs) and system.date.toMillis(segs[i+1][0]) <= tms: i += 1
    while i > 0 and system.date.toMillis(segs[i][0]) > tms: i -= 1
    if not segs: return (0.0, 2.0, i)
    _, ct, mult = segs[i]; return (ct, mult, i)

# ----------------------- shifts (yesterday + today) -----------------------
_SHIFT_REFRESH_SEC = 60
_shifts = {"last_load": 0, "today": None, "yday": None, "by_line": {}}
def _datestr(d): return system.date.format(d, "yyyy-MM-dd")

def _load_shifts_if_needed():
    now = system.date.now()
    now_ms = system.date.toMillis(now)
    today = _datestr(now); yday = _datestr(system.date.addDays(now,-1))
    if (now_ms - _shifts["last_load"] < _SHIFT_REFRESH_SEC*1000) and _shifts["today"] == today: return
    by_line={}
    def _grab(day):
        try: return list(system.db.runNamedQuery(MBASE + "getShiftScheduleOnDate", {"shift_date": day}) or [])
        except Exception as e: _LE("OCT::_load_shifts_if_needed(%s)"%day, e); return []
    for day in (yday, today):
        for r in _grab(day):
            try:
                lid=int(r["line_id"]); shid=int(r["shift_id"])
                st=r["start_time"]; en=r["end_time"]
                if not st or not en: continue
                by_line.setdefault(lid, []).append((shid, day, system.date.toMillis(st), system.date.toMillis(en)))
            except: pass
    for lid in by_line: by_line[lid].sort(key=lambda t: t[2])
    _shifts["by_line"]=by_line; _shifts["last_load"]=now_ms; _shifts["today"]=today; _shifts["yday"]=yday
    _LI("OCT::_load_shifts_if_needed", "Loaded shifts for %d lines" % len(by_line))

def _active_shift_for_line(line_id, now_ms):
    wins = _shifts["by_line"].get(int(line_id), [])
    for (shid, day, s_ms, e_ms) in wins:
        if s_ms <= now_ms < e_ms:
            return (shid, day, s_ms, e_ms)
    return (None, None, None, None)

def _last_ended_shift_for_line(line_id, now_ms, grace_ms):
    wins = _shifts["by_line"].get(int(line_id), []); last=None
    for (shid, day, s_ms, e_ms) in wins:
        if e_ms <= now_ms: last=(shid, day, s_ms, e_ms)
        else: break
    if last and (now_ms - last[3]) <= grace_ms: return last
    return (None, None, None, None)

# ----------------------- delta anchors & fallbacks -----------------------
def _line_last_asof(lid, shid, shift_start):
    """
    Try NQ with (line_id, shift_id). If that fails or returns NULL,
    fall back to max(as_of_local) from existing per-station rows for the same shift.
    """
    # primary
    try:
        ds = system.db.runNamedQuery(MBASE + "getLineLastAsOfForShift", {"line_id": lid, "shift_id": shid})
        for r in _rowdicts(ds):
            last = r.get("last_as_of")
            if last:
                return last
    except Exception as e:
        _LW("OCT::_line_last_asof", "Primary NQ failed; will try fallback. %s" % _u(e))

    # fallback: derive from station cum rows
    try:
        ds2 = system.db.runNamedQuery(MBASE + "getStationCumForShiftByLine",
                                      {"line_id": lid, "shift_id": shid, "shift_start_local": shift_start})
        best = None
        for r in _rowdicts(ds2):
            t = r.get("as_of_local")  # <<<<<< changed from window_end_time
            if t and (best is None or system.date.toMillis(t) > system.date.toMillis(best)):
                best = t
        return best or shift_start
    except Exception as e:
        _LW("OCT::_line_last_asof", "Fallback NQ failed; defaulting to shift_start. %s" % _u(e))
        return shift_start

def _existing_station_rows(lid, shid, shift_start):
    try:
        ds = system.db.runNamedQuery(MBASE + "getStationCumForShiftByLine",
                                     {"line_id": lid, "shift_id": shid, "shift_start_local": shift_start})
    except Exception as e:
        _LE("OCT::_existing_station_rows", e); ds=None
    out=set()
    for r in _rowdicts(ds):
        try: out.add(int(r["station_id"]))
        except: pass
    return out

# ----------------------- delta compute -----------------------
def _compute_deltas_for_line(stations_on_line, shift_id, shift_date, shift_start, a, b, shift_end, include_zero_for=set()):
    """
    Computes OCT deltas between [a, b] for every station on the line.

    - Reads ONLY <station>/CycleTime history.
    - Uses CT segments to gate valid OCT (act > ct and act <= ct*mult).
    - Emits a row (with delta counts/sums) when there is any OCT in [a, b].
    - Also emits a **zero** row when either:
        * the station id is in include_zero_for  (e.g., already has a cum row), OR
        * the station simply has a CycleTime tag (so we create an initial cum row
          even if the very first delta window has no OCT).

    Returned rows are shaped as “cumulative anchors” (no window_* fields):
    as_of_local       = b          (this run’s as_of)
    slot_duration_min = minutes from shift_start → b
    """
    rows   = []
    scanned = kept = 0

    for st in stations_on_line:
        scanned += 1
        sid = int(st["station_id"])
        lid = int(st["line_id"])

        # Segments just for [a, b]
        segs = _ct_segments(sid, a, b)

        # History for <station>/CycleTime
        ds = _query_ct_history(st, a, b)
        has_ct_tag = _tag_exists(_root(st) + u"/CycleTime")
        seed_zero  = (sid in include_zero_for) or has_ct_tag  # <-- important

        cnt = 0
        mx  = 0.0
        sum_over = 0.0
        idx = 0

        if segs and ds:
            for i in range(ds.getRowCount()):
                ts = ds.getValueAt(i, 0)
                try:
                    act = float(ds.getValueAt(i, 1))
                except:
                    continue

                ct, mult, idx = _ct_at(ts, segs, idx)
                if ct <= 0.0:
                    continue
                if act <= ct:
                    continue
                if act > ct * mult:
                    # Treat very long cycles as non-OCT (idle / changeover)
                    continue
                if EPSILON > 0.0 and act < ct * (1.0 + EPSILON):
                    continue

                over = act - ct
                cnt += 1
                sum_over += over
                if over > mx:
                    mx = over

        # Decide whether to emit a row
        if cnt > 0 or sum_over > 0.0 or seed_zero:
            # dynamic minutes from shift start → this run's as_of (b)
            window_minutes = int(round((system.date.toMillis(b) - system.date.toMillis(shift_start)) / 60000.0))

            rows.append({
                "line_id": lid,
                "station_id": sid,
                "shift_id": int(shift_id),
                "shift_date": _u(shift_date),
                "shift_start_local": _iso_sql(shift_start),
                "shift_end_local":   _iso_sql(shift_end),               # optional hint; may equal as_of
                "as_of_local":       _iso_sql(b),

                # IMPORTANT: names expected by the SP:
                "inc_over_cnt":      int(cnt),                  # DELTA
                "inc_over_sec":      round(float(sum_over), 3), # DELTA
                "inc_max_over_sec":  round(float(mx), 3),       # DELTA max

                # dynamic minutes (used by SP instead of hard-coded 15)
                "slot_duration_min": window_minutes
            })
            kept += 1

        _LI("OCT::_station_debug",
            "lid=%d sid=%d segs=%d hist_rows=%s window=[%s → %s] -> kept=%s cnt=%d sum=%.3f mx=%.3f" %
            (lid, sid, len(segs),
             (ds.getRowCount() if ds else "0"),
             _iso_sql(a), _iso_sql(b),
             "Y" if (cnt>0 or sum_over>0.0 or seed_zero) else "N",
             cnt, sum_over, mx))

    _LI("OCT::_compute_deltas_for_line",
        "Scanned %d stations, produced %d delta rows for [%s → %s]" %
        (scanned, kept, _iso_sql(a), _iso_sql(b)))
    return rows

# ----------------------- main -----------------------
def run_overcycle():
    	
        _LI("OCT::run_overcycle", "START")
        _load_shifts_if_needed()

        stations = _load_stations()
        if not stations:
            _LW("OCT::run_overcycle", "No stations returned; nothing to do.")
            return

        now    = system.date.now()
        now_ms = system.date.toMillis(now)
        ver    = u"1.0.0"
        ts_iso = _iso_off(now)
        grace_ms = _FINAL_GRACE_MIN * 60 * 1000

        # hierarchy (for MQTT topic paths / station names)
        H = _get_hierarchy([st["station_id"] for st in stations])

        # group stations by line
        by_line = {}
        for st in stations:
            by_line.setdefault(int(st["line_id"]), []).append(st)

        _LI("OCT::run_overcycle", "Processing %d lines" % len(by_line))

        # helper to choose some hierarchy for the line even if accum list is empty
        def _any_h_for_line(acc_list, sts_list):
            try:
                if acc_list:
                    sid = acc_list[0]["sid"]
                    h = H.get(int(sid))
                    if h: 
                        return h
                if sts_list:
                    st0 = sts_list[0]
                    return (H.get(int(st0["station_id"])) or
                            {"area": _u(st0["area"]), "subarea": _u(st0["subarea"]), "line": _u(st0["line"])})
            except:
                pass
            return None

        for lid, sts in by_line.items():

            # ---------- 1) finalize a prior shift that just ended ----------
            prev = _last_ended_shift_for_line(lid, now_ms, grace_ms)
            if prev[0] is not None:
                shid, sday, s_ms, e_ms = prev
                shift_start = system.date.fromMillis(s_ms)
                shift_end   = system.date.fromMillis(e_ms)
                last_asof   = _line_last_asof(lid, shid, shift_start) or shift_start

                _LI("OCT::finalize", "Line %d shift %d last_as_of=%s end=%s" %
                    (lid, shid, _iso_sql(last_asof), _iso_sql(shift_end)))

                # catch-up delta to the shift end (if anything left)
                if system.date.toMillis(last_asof) < system.date.toMillis(shift_end):
                    existed = _existing_station_rows(lid, shid, shift_start)
                    delta_rows = _compute_deltas_for_line(
					    sts, shid, sday, shift_start, last_asof, shift_end, shift_end, include_zero_for=existed
					)
                    for r in delta_rows: r["is_final"] = 1
                    if delta_rows:
                        try:
                            system.db.runNamedQuery(MBASE + "upsertSlotStationBatch", {
                                "payload": system.util.jsonEncode(delta_rows),
                                "created_by": "OvercyclePublisher"
                            })
                            _LI("OCT::finalize", "Upserted %d station cum rows (final)" % len(delta_rows))
                        except Exception as e:
                            _LE("OCT::upsertSlotStationBatch(final)", e)

                # build totals from DB accum (can be empty) and publish
                try:
                    try:
                        ds_acc = system.db.runNamedQuery(MBASE + "getShiftAccumForLine", {
                            "line_id": lid, "shift_id": int(shid),
                            "shift_start_local": shift_start, "as_of_local": shift_end
                        })
                    except Exception as e1:
                        _LW("OCT::getShiftAccumForLine(final)", "Retrying without shift_id: %s" % _u(e1))
                        ds_acc = system.db.runNamedQuery(MBASE + "getShiftAccumForLine", {
                            "line_id": lid, "shift_start_local": shift_start, "as_of_local": shift_end
                        })

                    acc = []
                    for r in _rowdicts(ds_acc):
                        try:
                            acc.append({
                                "sid": int(r["station_id"]),
                                "sum_over": float(r.get("over_sec_sum_shift") or 0.0),
                                "sum_cnt":  int(r.get("over_count_shift") or 0)
                            })
                        except:
                            pass

                    # top lists (possibly empty)
                    top_tim = sorted(acc, key=lambda x: (x["sum_over"], x["sum_cnt"]), reverse=True)[:_MAX_TOP] if acc else []
                    top_tot = sorted(acc, key=lambda x: (x["sum_cnt"], x["sum_over"]), reverse=True)[:_MAX_TOP] if acc else []

                    h_any = _any_h_for_line(acc, sts)
                    if h_any:
                        def _name(sid):
                            return (H.get(sid, {}) or {}).get("station") or u"Station_%d" % sid

                        pay_tim = {
                          "Version": ver, "Timestamp": ts_iso,
                          "TopOvercycles": {
                            "Overcycles": [
                              {"ID": i+1, "StnID": _name(r["sid"]), "Value": _fmt_mmss(r["sum_over"])}
                              for i, r in enumerate(top_tim)
                            ]
                          }
                        }
                        pay_tot = {
                          "Version": ver, "Timestamp": ts_iso,
                          "TopOvercycles": {
                            "Overcycles": [
                              {"ID": i+1, "StnID": _name(r["sid"]), "Value": int(r["sum_cnt"])}
                              for i, r in enumerate(top_tot)
                            ]
                          }
                        }

                        _publish(_topic_for_line(h_any, "TopOvercycleTotals"), pay_tot, qos=0, retain=False)
                        _publish(_topic_for_line(h_any, "TopOvercycleTimes"),  pay_tim, qos=0, retain=False)
                    else:
                        _LW("OCT::finalize", "No hierarchy for line %d; skipping MQTT publish." % lid)

                    # snapshot row (stores empty JSON arrays if necessary)
                    try:
                    	slot_minutes_final = int(round((system.date.toMillis(shift_end) - system.date.toMillis(shift_start)) / 60000.0))
                        system.db.runNamedQuery(MBASE + "upsertSlotLineBatch", {
                            "payload": system.util.jsonEncode([{
                              "line_id": lid, "shift_id": int(shid), "shift_date": _u(sday),
                              "shift_start_local": _iso_sql(shift_start),
                              "shift_end_local":   _iso_sql(shift_end),
                              "as_of_local":       _iso_sql(shift_end),
                              "is_final":          1,
                              "slot_duration_min": slot_minutes_final,
                              "is_published":      0,
                              "top_totals_json":   system.util.jsonEncode(
                                  [{"id": i+1, "station": (H.get(r["sid"], {}) or {}).get("station", u"Station_%d" % r["sid"]), "value": int(r["sum_cnt"])}
                                   for i, r in enumerate(top_tot)]
                              ),
                              "top_times_json":    system.util.jsonEncode(
                                  [{"id": i+1, "station": (H.get(r["sid"], {}) or {}).get("station", u"Station_%d" % r["sid"]), "value": _fmt_mmss(r["sum_over"])}
                                   for i, r in enumerate(top_tim)]
                              )
                            }]),
                            "created_by": "OvercyclePublisher"
                        })
                        _LI("OCT::finalize", "Inserted final line snapshot for line %d shift %d" % (lid, shid))
                    except Exception as e:
                        _LE("OCT::upsertSlotLineBatch(final)", e)

                except Exception as e:
                    _LE("OCT::final_snapshot", e)

            # ---------- 2) current active shift ----------
            cur = _active_shift_for_line(lid, now_ms)
            if cur[0] is not None:
                shid, sday, s_ms, e_ms = cur
                shift_start = system.date.fromMillis(s_ms)
                shift_end   = system.date.fromMillis(e_ms)
                as_of       = now if now_ms < e_ms else shift_end
                last_asof   = _line_last_asof(lid, shid, shift_start) or shift_start

                _LI("OCT::current", "Line %d shift %d delta [%s → %s]" %
                    (lid, shid, _iso_sql(last_asof), _iso_sql(as_of)))

                # delta since last_asof
                if system.date.toMillis(as_of) > system.date.toMillis(last_asof):
                    existed = _existing_station_rows(lid, shid, shift_start)
                    delta_rows = _compute_deltas_for_line(
					    sts, shid, sday, shift_start, last_asof, as_of, shift_end, include_zero_for=existed
					)
                    for r in delta_rows: r["is_final"] = 0
                    if delta_rows:
                        try:
                            system.db.runNamedQuery(MBASE + "upsertSlotStationBatch", {
                                "payload": system.util.jsonEncode(delta_rows),
                                "created_by": "OvercyclePublisher"
                            })
                            _LI("OCT::current", "Upserted %d station cum rows (current)" % len(delta_rows))
                        except Exception as e:
                            _LE("OCT::upsertSlotStationBatch(current)", e)

                # build current snapshot from DB accum (possibly empty) and publish
                try:
                    try:
                        ds_acc = system.db.runNamedQuery(MBASE + "getShiftAccumForLine", {
                            "line_id": lid, "shift_id": int(shid),
                            "shift_start_local": shift_start, "as_of_local": as_of
                        })
                    except Exception as e1:
                        _LW("OCT::getShiftAccumForLine(current)", "Retrying without shift_id: %s" % _u(e1))
                        ds_acc = system.db.runNamedQuery(MBASE + "getShiftAccumForLine", {
                            "line_id": lid, "shift_start_local": shift_start, "as_of_local": as_of
                        })

                    acc = []
                    for r in _rowdicts(ds_acc):
                        try:
                            acc.append({
                                "sid": int(r["station_id"]),
                                "sum_over": float(r.get("over_sec_sum_shift") or 0.0),
                                "sum_cnt":  int(r.get("over_count_shift") or 0)
                            })
                        except:
                            pass

                    top_tim = sorted(acc, key=lambda x: (x["sum_over"], x["sum_cnt"]), reverse=True)[:_MAX_TOP] if acc else []
                    top_tot = sorted(acc, key=lambda x: (x["sum_cnt"], x["sum_over"]), reverse=True)[:_MAX_TOP] if acc else []

                    h_any = _any_h_for_line(acc, sts)
                    if h_any:
                        def _name(sid):
                            return (H.get(sid, {}) or {}).get("station") or u"Station_%d" % sid

                        pay_tim = {
                          "Version": u"1.0.0", "Timestamp": _iso_off(now),
                          "TopOvercycles": {
                            "LineId": u"%d" % lid, "ShiftId": int(shid),
                            "Overcycles": [
                              {"ID": i+1, "StnID": _name(r["sid"]), "Value": _fmt_mmss(r["sum_over"])}
                              for i, r in enumerate(top_tim)
                            ]
                          }
                        }
                        pay_tot = {
                          "Version": u"1.0.0", "Timestamp": _iso_off(now),
                          "TopOvercycles": {
                            "LineId": u"%d" % lid, "ShiftId": int(shid),
                            "Overcycles": [
                              {"ID": i+1, "StnID": _name(r["sid"]), "Value": int(r["sum_cnt"])}
                              for i, r in enumerate(top_tot)
                            ]
                          }
                        }

                        _publish(_topic_for_line(h_any, "TopOvercycleTotals"), pay_tot, qos=0, retain=False)
                        _publish(_topic_for_line(h_any, "TopOvercycleTimes"),  pay_tim, qos=0, retain=False)
                    else:
                        _LW("OCT::current", "No hierarchy for line %d; skipping MQTT publish." % lid)

                    # snapshot row (stores empty JSON arrays if necessary)
                    try:
                    	slot_minutes_current = int(round((system.date.toMillis(as_of) - system.date.toMillis(shift_start)) / 60000.0))
                        system.db.runNamedQuery(MBASE + "upsertSlotLineBatch", {
                            "payload": system.util.jsonEncode([{
                              "line_id": lid, "shift_id": int(shid), "shift_date": _u(sday),
                              "shift_start_local": _iso_sql(shift_start),
                              "shift_end_local":   _iso_sql(shift_end),
                              "as_of_local":       _iso_sql(as_of),
                              "is_published":      1,
                              "is_final":          0,
                              "slot_duration_min": slot_minutes_current,
                              "top_totals_json":   system.util.jsonEncode(
                                  [{"id": i+1, "station": (H.get(r["sid"], {}) or {}).get("station", u"Station_%d" % r["sid"]), "value": int(r["sum_cnt"])}
                                   for i, r in enumerate(top_tot)]
                              ),
                              "top_times_json":    system.util.jsonEncode(
                                  [{"id": i+1, "station": (H.get(r["sid"], {}) or {}).get("station", u"Station_%d" % r["sid"]), "value": _fmt_mmss(r["sum_over"])}
                                   for i, r in enumerate(top_tim)]
                              )
                            }]),
                            "created_by": "OvercyclePublisher"
                        })
                        _LI("OCT::current", "Inserted current line snapshot for line %d shift %d" % (lid, shid))
                    except Exception as e:
                        _LE("OCT::upsertSlotLineBatch(current)", e)

                except Exception as e:
                    _LE("OCT::current_snapshot", e)

        _LI("OCT::run_overcycle", "END")
