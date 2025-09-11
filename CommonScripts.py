# <summary>
# Module Name : CommonScripts
# Description : Global utility functions used across MagnaDataOps Perspective project.
# Author      : Akash J
# Created On  : 2025-06-22
# </summary>

import system
from MagnaDataOps import ProjectLists
from MagnaDataOps import LoggerFunctions as Log

MODULE = "CommonScripts"

#Payload Version used for all mqtt publishing payload (Do Not Move)
payload_version = '1.0.0'


def call_notification_popup(notifMsgCode, popupIndex, timersp=8):
    """
    Show a non-modal notification popup (top-right, stacked).

    Args:
        notifMsgCode (int|str): Code or raw message to display.
        popupIndex (int): Stack index (0..N) to offset vertically.
        timersp (int): Auto-dismiss seconds (default: 8).
    """
    try:
        # Resolve message + type from response code mapping
        result = ProjectLists.get_response_code_mapping(notifMsgCode)
        message = result.get("message")
        notifType = result.get("type_id")

        popupKey = system.date.format(system.date.now(), "MMddHHmmss")
        topOffset = 70 + (int(popupIndex) * 130)

        system.perspective.openPopup(
            popupKey,
            "MagnaDataOps/Common/Popups/NotificationPopup",
            params={
                "notifType": notifType,
                "notifMsg": message,
                "timersp": timersp,
                "popupKey": popupKey,
                "popupIndex": popupIndex,
            },
            position={"right": 20, "top": topOffset},
            draggable=False,
            resizable=False,
            modal=False,
            showCloseIcon=False,
            viewportBound=True,
        )
    except Exception:
        Log.log_error("{}/call_notification_popup".format(MODULE))


def openLoader(isVisible):
    """
    Open or close the blocking loader popup.

    Args:
        isVisible (bool): True to open, False to close.
    """
    try:
        popupPath = "MagnaDataOps/Common/Popups/Loader/LoaderMain"
        if bool(isVisible):
            system.perspective.openPopup(
                "loaderPopUp",
                popupPath,
                showCloseIcon=False,
                overlayDismiss=False,
                modal=True,
            )
        else:
            system.perspective.closePopup("loaderPopUp")
    except Exception:
        Log.log_error("{}/openLoader".format(MODULE))


def get_next_popup_slot(stackDict, max_slots=5):
    """
    Determine next free popup slot index (0..max_slots-1).

    Policy:
      - If both 0 and 1 are free, reuse from 0.
      - Else, allocate next free index > current max(used).
      - Fallback scan from 0..max_slots-1.

    Args:
        stackDict (dict): Current slot usage map (keys may be strings or longs).
        max_slots (int): Upper bound (default 5).

    Returns:
        int|None: Next slot or None if full.
    """
    try:
        raw = dict(stackDict) if stackDict is not None else {}
    except Exception:
        raw = {}

    used = set()
    for k in raw:
        ks = str(k).strip()
        if ks.endswith("L"):  # clean Jython long repr like '3L'
            ks = ks[:-1]
        try:
            num = int(ks)
        except Exception:
            # Log and skip this key (avoid try/except/continue pattern)
            Log.log_error("{}/get_next_popup_slot/int".format(MODULE))
        else:
            used.add(num)

    if 0 not in used and 1 not in used:
        return 0

    if used:
        max_used = max(used)
        for i in range(max_used + 1, int(max_slots)):
            if i not in used:
                return i

    for i in range(int(max_slots)):
        if i not in used:
            return i

    return None


# Project Library Script: StringUtils.py
def to_sentence_case(text):
    """
    Trim and convert to sentence case.
      '  test ANDON CODE  ' → 'Test andon code'
    """
    try:
        if text is None:
            return ""
        trimmed = text.strip()
        if not trimmed:
            return ""
        return trimmed[0].upper() + trimmed[1:].lower()
    except Exception:
        Log.log_error("{}/to_sentence_case".format(MODULE))
        return text


def apply_screen_exclusions(self, code_list, component_map=None, view_name="UnknownSubview"):
    """
    Disable/hide components per exclusion IDs resolved from session roles.

    Args:
        self: Perspective component (has session + view context).
        code_list (dict): {screen_key: exclusion_id}
        component_map (dict|None): Optional {screen_key: component_name}
        view_name (str): View name for log context.
    """
    try:
        hidden_class = "MagnaDataOps/Common/HomePageTiles/invisible_bkg_clr"

        user_roles = list(self.session.props.auth.user.roles)
        role_access_ds = self.session.custom.userAccess
        role_to_excl = {role: set() for role in user_roles}

        if hasattr(role_access_ds, "rowCount") and role_access_ds.rowCount:
            for row in role_access_ds:
                sid = row["screen_exclusion_id"]
                role_name = row["user_role_name"]
                for r in user_roles:
                    if str(r).strip().lower() == str(role_name).strip().lower() and sid is not None:
                        role_to_excl[r].add(sid)

            intersection = None
            for excl_set in role_to_excl.values():
                intersection = excl_set if intersection is None else intersection.intersection(excl_set)
            if intersection is None:
                intersection = set()

            for screen_key, exclusion_id in code_list.items():
                if component_map and screen_key in component_map:
                    comp_name = component_map[screen_key]
                else:
                    comp_name = "cnt" + "".join([part.capitalize() for part in screen_key.split("_")])

                try:
                    comp = self.getChild("root").getChild(comp_name)
                    if exclusion_id in intersection:
                        comp.custom.enabled = False
                        comp.props.style.classes = hidden_class
                except Exception:
                    Log.log_error("{}/apply_screen_exclusions".format(MODULE))

    except Exception:
        Log.log_error("{}/apply_screen_exclusions".format(MODULE))


def apply_individual_screen_access_return_flag(viewRef, screen_id, view_name):
    """
    Return write-access flag for a screen based on role exclusions.

    Args:
        viewRef: Perspective component or view object (for session access).
        screen_id (int): Screen exclusion ID to check.
        view_name (str): For log context.

    Returns:
        bool: True if write allowed; False otherwise.
    """
    try:
        user_roles = list(viewRef.session.props.auth.user.roles)
        role_access_ds = viewRef.session.custom.userAccess

        if not hasattr(role_access_ds, "getRowCount") or role_access_ds.getRowCount() == 0:
            Log.log_error("{}/apply_individual_screen_access_return_flag::NoAccess".format(MODULE))
            return False

        write_allowed = False
        all_roles_excluded = True

        for row in system.dataset.toPyDataSet(role_access_ds):
            role_name = row["user_role_name"]
            exclusion_id = row["screen_exclusion_id"]
            write_access = row["write_access"]

            for r in user_roles:
                if str(r).strip().lower() == str(role_name).strip().lower():
                    if exclusion_id is None or exclusion_id != screen_id:
                        all_roles_excluded = False
                        if write_access == 1:
                            write_allowed = True

        if all_roles_excluded:
            return False

        return write_allowed

    except Exception:
        Log.log_error("{}/apply_individual_screen_access_return_flag::Error".format(MODULE))
        return False


def generate_csv_template(self, headers, filename="Upload_Template.csv"):
    """
    Generate a one-line CSV (with headers) and trigger browser download.

    Args:
        self: Perspective component (for download API).
        headers (List[str]): CSV header columns.
        filename (str): Suggested download filename (default: 'Upload_Template.csv').
    """
    try:
        data = [["" for _ in headers]]
        template_dataset = system.dataset.toDataSet(headers, data)

        # system.dataset.toCSV returns CSV text; set correct MIME
        csv_text = system.dataset.toCSV(showHeaders=True, dataset=template_dataset)

        system.perspective.download(
            fileName=filename,
            data=csv_text,
            mimeType="text/csv",
        )
    except Exception:
        Log.log_error("{}/generate_csv_template".format(MODULE))