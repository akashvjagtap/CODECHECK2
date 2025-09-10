#<summary> 
#Module Name: Global
#Screen Name: Project Library - CommonScript Functions
#Comments: Global utility Functions
#</summary>

from datetime import datetime, timedelta
from os import path
from json import dumps, loads 
from base64 import b64encode, b64decode
from system.tag import read
from java.util import Date
from java.sql import Timestamp
from java.time import ZoneId, ZonedDateTime
from java.time.format import DateTimeFormatter
from StringIO import StringIO
from javax.crypto import Cipher
from javax.crypto.spec import SecretKeySpec
from system.net import httpClient
from system.net import httpPost
from csv import DictReader,reader, writer
from MagnaDataOps.SecureInputUtils import sanitize_for_logging

payload_version = '1.0.0'


def sanitize_csv_field(value):
	"""
	Prefixes potentially dangerous values in CSV fields to prevent Excel formula injection.
	Targets values starting with =, +, -, or @.
	"""
	if isinstance(value, str) and value.startswith(('=', '+', '-', '@')):
		return "'" + value
	return value


			

# <summary> 
# Created By: Aksh J
# Creation Date: 06/22/2025
# Modified By: 
# Modified Date: 
# Comments: Shows a non-modal notification popup with message and result type.
# </summary>
def call_notification_popup(notifMsgCode, popupIndex, timersp=8):
	"""
	Displays a non-modal notification popup in the MES UI,
	stacked top-right with vertical offset per popup.

	Parameters:
	- notifMsgCode (int or str): 
		Message code returned by DB procedure (or raw text).
	- timersp (int): 
		Auto-dismiss time in seconds (default = 8)
	"""
	try:
		# Determine message + type from your shared dictionary
		result = MagnaDataOps.ProjectLists.get_response_code_mapping(notifMsgCode)
		message = result.get("message")
		notifType = result.get("type_id")
		
		# Unique popup ID
		popupKey = str(system.date.format(system.date.now(), "MMddHHmmss"))
		topOffset = 70 + (popupIndex * 130)

		system.perspective.openPopup(
			popupKey,
			'MagnaDataOps/Common/Popups/NotificationPopup',
			params={
				"notifType": notifType,
				"notifMsg": message,
				"timersp": timersp,
				"popupKey": popupKey,
				"popupIndex": popupIndex
			},
			position={
				"right": 20,
				"top": topOffset
			},
			draggable=False,
			resizable=False,
			modal=False,
			showCloseIcon=False,
			viewportBound=True
		)
	except:
		MagnaDataOps.CommonScripts.log_error("CommonScripts/call_notification_popup")
		
			
# <summary> 
# Created By: Aksh J
# Creation Date: 06/22/2025
# Modified By: 
# Modified Date: 
# Comments: Opens or closes the loader popup based on the visibility flag.
# </summary>
def openLoader(isVisible):
	"""
	Opens or closes a modal loader popup in the MES UI.

	Parameters:
	- isVisible (bool): 
		True to show the loader popup, False to hide it.
	"""
	try:
		popupPath = 'MagnaDataOps/Common/Popups/Loader/LoaderMain'

		if isVisible:
			# Open modal loader popup
			system.perspective.openPopup(
				'loaderPopUp',
				popupPath,
				showCloseIcon=False,
				overlayDismiss=False,
				modal=True
			)
		else:
			# Close the loader popup
			system.perspective.closePopup('loaderPopUp')
	except:
		MagnaDataOps.CommonScripts.log_error("CommonScripts/openLoader")






# <summary>
# Created By: 
# Creation Date: 
# Modified By:
# Modified Date:
# Comments: Uploads import template metadata to the external API.
# </summary>
def importUploadTemplate(FilemyObj, TemplateName, filePath, RequestFrom, userID):
	try:
		apiName = 'UploadFiles'
		URLPath = system.tag.read("[default]MES_Tags/API/urlAPI").value
		apiPath = "UploadFiles"
		url = URLPath + apiPath

		jsonParams = {
			"TemplateName": TemplateName,
			"FilePath": filePath,
			"RequestFrom": RequestFrom,
			"CreatedBy": userID
		}

		try:
			resultDetails = system.net.httpPost(url, jsonParams)
			return resultDetails
		except:
			CommonScripts.log_error("CommonScripts/importUploadTemplate - inner block")
			return None

	except:
		CommonScripts.log_error("CommonScripts/importUploadTemplate - outer block")
		return None
		
# <summary>
# Created By: 
# Creation Date: 
# Modified By:
# Modified Date:
# Comments: Fetches the upload file path from external API used for import templates.
# </summary>
def getUploadFilePathImport():
	try:
		apiName = 'GetUploadFilePath'
		URLPath = system.tag.read("[default]MES_Tags/API/urlAPI").value
		apiPath = "UploadFiles/GetUploadFilePath"
		url = URLPath + apiPath

		client = system.net.httpClient()
		data = client.get(url)

		if data.good:
			return data.json['Data']
		else:
			return None

	except:
		CommonScripts.log_error("CommonScripts/getUploadFilePathImport")
		return None


# <summary>
# Created By: 
# Creation Date: 
# Modified By:
# Modified Date:
# Comments: Attempts to decode misencoded Latin-1 text into UTF-8.
# </summary>
def refine_text(input_text):
	try:
		return input_text.encode('latin1').decode('utf-8')
	except (UnicodeEncodeError, UnicodeDecodeError):
		return input_text


# <summary>
# Created By: 
# Creation Date: 
# Modified By:
# Modified Date:
# Comments: Converts timestamp string to datetime and adds 5:30 hours offset.
# </summary>
def convert_string_to_datetime_a(value_str):
	try:
		if value_str in (None, '', 'None'):
    # handle empty or null-like values
			return None

		dt = datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S.%f")
		adjusted_dt = dt + timedelta(hours=5, minutes=30)
		return adjusted_dt
	except Exception:
		CommonScripts.log_error("CommonScripts/convert_string_to_datetime_a")
		return None




def to_python_datetime(value):
	try:
		if isinstance(value, Date):
			return datetime.fromtimestamp(value.getTime() / 1000.0)
		return None
	except:
		CommonScripts.log_error("CommonScripts/to_python_datetime")
		return None
		
		


# <summary>
# Created By: Akash J		
# Creation Date: 04/10/2025	
# Modified By:
# Modified Date:
# Comments: below two codes encrypt and decrypt the plain text 
# </summary>
	
def encrypt_text(plain_text, key_string):
    key = key_string.encode('utf-8')[:16]  # Use first 16 bytes for AES-128
    cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    secretKey = SecretKeySpec(key, "AES")
    cipher.init(Cipher.ENCRYPT_MODE, secretKey)
    encrypted = cipher.doFinal(plain_text.encode('utf-8'))
    return b64encode(encrypted).decode('utf-8')

def decrypt_text(encrypted_text, key_string):
    key = key_string.encode('utf-8')[:16]  # AES-128 requires 16-byte key
    cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    secretKey = SecretKeySpec(key, "AES")
    cipher.init(Cipher.DECRYPT_MODE, secretKey)
    decoded = b64decode(encrypted_text)
    decrypted = cipher.doFinal(decoded)
    return decrypted.tostring()  
 





def get_next_popup_slot(stackDict, max_slots=5):
    """
    Returns the next slot index for notification popup.
    
    Rules:
    - Slots range from 0 to max_slots-1 (default 5).
    - Restart from 0 only if both 0 and 1 are free.
    - Otherwise, continue allocating the next unused index >= max(used)+1.
    """
    try:
        raw = dict(stackDict)
    except:
        raw = {}

    # Normalize keys to integers
    used = set()
    for k in raw:
        ks = str(k).strip()
        if ks.endswith('L'):
            ks = ks[:-1]
        try:
            used.add(int(ks))
        except:
            continue


    # Step 1: Restart condition — both 0 and 1 are free
    if 0 not in used and 1 not in used:
        return 0

    # Step 2: Otherwise, find next index above current max(used)
    if used:
        max_used = max(used)
        for i in range(max_used + 1, max_slots):
            if i not in used:
                return i

    # Step 3: Fallback — linear search from 0
    for i in range(max_slots):
        if i not in used:
            return i

    # All slots are full
    return None
    
# Project Library Script: StringUtils.py

def to_sentence_case(text):
	"""
	Trims the string and converts it to sentence case.
	Example: '  test ANDON CODE  ' → 'Test andon code'
	"""
	try:
		if text is None:
			return ""
		
		trimmed = text.strip()
		if not trimmed:
			return ""

		return trimmed[0].upper() + trimmed[1:].lower()
	except Exception as e:
		system.perspective.print("Error in to_sentence_case: " + str(e))
		return text  # fallback
	
		
def apply_screen_exclusions(self, code_list, component_map=None, view_name="UnknownSubview"):
	"""
	Disables and hides components based on exclusion IDs from userAccess dataset.

	Parameters:
	    self: current component context (e.g., passed from onStartup or event source)
	    code_list (dict): screen_key → exclusion_id
	    component_map (dict or None): screen_key → component_name (optional)
	    view_name (str): used for logging

	Expected Component Name Format:
	    - If component_map is None: inferred as "cnt" + Capitalized screen_key
	"""
	try:
		hidden_class = "MagnaDataOps/Common/HomePageTiles/invisible_bkg_clr"

		# 1. Collect user roles and access dataset
		user_roles = list(self.session.props.auth.user.roles)
		role_access_ds = self.session.custom.userAccess
		role_to_excl = {role: set() for role in user_roles}

		# 2. Build mapping role → excluded screen IDs
		if hasattr(role_access_ds, "rowCount") and role_access_ds.rowCount:
			for row in role_access_ds:
				sid = row["screen_exclusion_id"]
				role_name = row["user_role_name"]

				for r in user_roles:
					if str(r).strip().lower() == str(role_name).strip().lower() and sid is not None:
						role_to_excl[r].add(sid)

			# 3. Compute intersection of exclusions across all roles
			intersection = None
			for excl_set in role_to_excl.values():
				intersection = excl_set if intersection is None else intersection.intersection(excl_set)
			if intersection is None:
				intersection = set()

			# 4. Apply access logic to components
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
				except:
					location = MagnaDataOps.LoggerFunctions.get_full_path_with_context(self, view_name, "apply_screen_exclusions")
					MagnaDataOps.LoggerFunctions.log_error(location)

	except:
		location = MagnaDataOps.LoggerFunctions.get_full_path_with_context(self, view_name, "apply_screen_exclusions")
		MagnaDataOps.LoggerFunctions.log_error(location)		
		
		


# <summary>
# Created By: Akash J
# Created On: 2025-07-29
# Comments: Return write access flag (True/False) for screen-specific access control using intersection exclusion logic
# </summary>


def apply_individual_screen_access_return_flag(viewRef, screen_id, view_name):
	try:
		user_roles = list(viewRef.session.props.auth.user.roles)
		role_access_ds = viewRef.session.custom.userAccess

		if not hasattr(role_access_ds, "getRowCount") or role_access_ds.getRowCount() == 0:
			location = MagnaDataOps.LoggerFunctions.get_full_path_with_context(viewRef, view_name, "apply_individual_screen_access_return_flag :: NoAccess")
			MagnaDataOps.LoggerFunctions.log_error(location)
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

	except Exception as e:
		location = MagnaDataOps.LoggerFunctions.get_full_path_with_context(viewRef, view_name, "apply_individual_screen_access_return_flag :: Error")
		MagnaDataOps.LoggerFunctions.log_error(location + " | Exception: " + str(e))
		return False
		
# <summary>
# Created By: Akash J
# Created Date: 2025-07-31
# Comments: Generate and download Excel template with dynamic headers
# </summary>

def generate_csv_template(self, headers, filename="Upload_Template.csv"):
	try:
		# Create a blank row
		data = [["" for _ in headers]]
		template_dataset = system.dataset.toDataSet(headers, data)

		# Convert to Excel binary
		excel_bytes = system.dataset.toCSV(
			showHeaders=True,
			dataset=template_dataset
		)

		# Trigger download in Perspective
		system.perspective.download(
			fileName=filename,
			data=excel_bytes,
			mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
		)
	except:
		location = MagnaDataOps.LoggerFunctions.get_full_path_with_context(self, "GenerateExcelTemplate", "onActionPerformed")
		MagnaDataOps.LoggerFunctions.log_error(location)	
		
		
		
		