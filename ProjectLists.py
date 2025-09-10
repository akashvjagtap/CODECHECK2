# <summary>
# Created By: Akash J
# Creation Date: 05/08/2025
# Modified By:
# Modified Date:
# Comments: Constant list of whitelisted folders, tables and extensions
# </summary>


#Specify the extensions for file upload below
ALLOWED_EXTENSIONS = {
    '.pdf', '.jpg', '.jpeg', '.png', '.prn', '.zpl', '.bitmap', '.bmp', '.xlsx', '.mp4', '.pptx',
    '.ppt', '.xls', '.docx', '.doc', '.txt', '.csv', '.bmp', '.tif', '.tiff', '.gif', '.dib','.ipl'
}

DANGEROUS_EXTENSIONS = {'.exe', '.bat', '.cmd', '.com', '.scr', '.js',
                         '.vbs', '.ps1', '.jar', '.msi'}


# Query response code mapping list

# Type ID List: standard notification type codes
TYPE_ID_LIST = {
    "Error": 0,
    "Success": 1,
    "Info": 2,
    "Warning": 3
}

# Response Code Mapping: code -> message + type_id
RESPONSE_CODE_MAP = {
    # General
    0:   {"message": "Operation failed. Please retry or contact support.", "type_id": TYPE_ID_LIST["Error"]},
    100: {"message": "Unknown response code or unexpected condition.", "type_id": TYPE_ID_LIST["Warning"]},

    # Create
    1:   {"message": "Record(s) created successfully.", "type_id": TYPE_ID_LIST["Success"]},
    101: {"message": "Creation failed due to validation errors/invalid file format.", "type_id": TYPE_ID_LIST["Error"]},
    102: {"message": "Duplicate(s) found. Cannot create record.", "type_id": TYPE_ID_LIST["Warning"]},
    103: {"message": "Invalid file format or inconsistent hierarchy.", "type_id": TYPE_ID_LIST["Error"]},
    104: {"message": "Partial success: One or more records skipped.", "type_id": TYPE_ID_LIST["Warning"]},
    
    #Payload Related
    105: {"message": "Payload(s) published successfully.", "type_id": TYPE_ID_LIST["Success"]},
    106: {"message": "Publish failed.Please retry or contact support.", "type_id": TYPE_ID_LIST["Error"]},
    107: {"message": "Publish failed. Shift schedule not published for the selected date.", "type_id": TYPE_ID_LIST["Error"]},
    108: {"message": "Publish failed: Missing shifts for some lines on this date.", "type_id": TYPE_ID_LIST["Error"]},
    109: {"message": "Publish failed: KPI Target Entry missing for one or more lines under the selected line on the selected date.", "type_id": TYPE_ID_LIST["Error"]},

    
    # Publish Control (Start/Stop)
	110: {"message": "Tag(s) set for publishing.",                 "type_id": TYPE_ID_LIST["Success"]},
	111: {"message": "No eligible tag(s) to start.",               "type_id": TYPE_ID_LIST["Warning"]},
	112: {"message": "Publishing stopped for tag(s).",             "type_id": TYPE_ID_LIST["Success"]},
	113: {"message": "No eligible tag(s) to stop.",                "type_id": TYPE_ID_LIST["Warning"]},
	114: {"message": "Partial update: some tag(s) were skipped.",  "type_id": TYPE_ID_LIST["Warning"]},
	115: {"message": "Failed to change publish state.",            "type_id": TYPE_ID_LIST["Error"]},
	116: {"message": "Invalid or empty selection.",                "type_id": TYPE_ID_LIST["Warning"]},
	
	117: {"message": "Tag(s) added successfully.", "type_id": TYPE_ID_LIST["Success"]},
	118: {"message": "Duplicate found. Cannot add tag(s).", "type_id": TYPE_ID_LIST["Warning"]},
	119: {"message": "Tag(s) removed successfully.", "type_id": TYPE_ID_LIST["Success"]},
	120: {"message": "Failed to remove tag(s).Please retry or contact support.","type_id": TYPE_ID_LIST["Error"]},
	121: {"message": "Tag(s) added successfully. Duplicate/Invalid skipped.", "type_id": TYPE_ID_LIST["Warning"]},
	122: {"message": "Folder selected. Select a tag node to add.", "type_id": TYPE_ID_LIST["Error"]},
	123: {"message": "Only 1 station cycle time tag allowed. Remove added tag and replace.", "type_id": TYPE_ID_LIST["Error"]},


    # New, more specific cases (subdivide the old 108)
	130: {"message": "Publish blocked: Shift window differs across selected lines for the selected date.", "type_id": TYPE_ID_LIST["Error"]},
	131: {"message": "Publish blocked: Breaks overlap. Adjust times to remove overlaps.", "type_id": TYPE_ID_LIST["Error"]},
	132: {"message": "Publish blocked: One or more breaks fall outside the shift window.", "type_id": TYPE_ID_LIST["Error"]},
	133: {"message": "Publish blocked: No lines found under the selected area.", "type_id": TYPE_ID_LIST["Error"]},
	134: {"message": "Publish blocked: Break payload is empty or invalid.", "type_id": TYPE_ID_LIST["Error"]},
	135: {"message": "Publish blocked: One or more selected Break IDs are inactive or invalid.", "type_id": TYPE_ID_LIST["Error"]},
	136: {"message": "Publish blocked: Breaks already published in area for this date. Use Republish.", "type_id": TYPE_ID_LIST["Error"]},
	137: {"message": "Publish blocked: One or more shifts overlapping.", "type_id": TYPE_ID_LIST["Error"]},

    # Read
    2:   {"message": "Records fetched successfully.", "type_id": TYPE_ID_LIST["Info"]},
    201: {"message": "No records found for the criteria.", "type_id": TYPE_ID_LIST["Warning"]},
	202: {"message": "No lines found for selected area.", "type_id": TYPE_ID_LIST["Error"]},
    # Update
    3:   {"message": "Record(s) updated successfully.", "type_id": TYPE_ID_LIST["Success"]},
    301: {"message": "Update failed. Record not found or already modified.", "type_id": TYPE_ID_LIST["Error"]},
    302: {"message": "Duplicate constraint. Cannot update record.", "type_id": TYPE_ID_LIST["Warning"]},
    303:   {"message": "Record(s) create/updated successfully.", "type_id": TYPE_ID_LIST["Success"]},

    # Delete
    4:   {"message": "Record(s) deleted successfully.", "type_id": TYPE_ID_LIST["Success"]},
    401: {"message": "Delete failed. Record not found or protected.", "type_id": TYPE_ID_LIST["Error"]},
    402:   {"message": "Tag node deleted successfully.", "type_id": TYPE_ID_LIST["Success"]},
    403: {"message": "Delete failed. Part number(s) in use.", "type_id": TYPE_ID_LIST["Error"]},
    404: {"message": "Partial success: Part number(s) in use skipped deletion.", "type_id": TYPE_ID_LIST["Warning"]},

    # DB / transaction
    500: {"message": "Database transaction failed. Contact support.", "type_id": TYPE_ID_LIST["Error"]},
    501: {"message": "Foreign key constraint. Operation blocked.", "type_id": TYPE_ID_LIST["Error"]},

	
    # Ignition / system
    600: {"message": "Script execution failed. Check logs.", "type_id": TYPE_ID_LIST["Error"]},
    601: {"message": "Ignition gateway connection lost.", "type_id": TYPE_ID_LIST["Warning"]},
    602: {"message": "Tag write failed or returned bad quality.", "type_id": TYPE_ID_LIST["Error"]},
    603: {"message": "Script execution timed out.", "type_id": TYPE_ID_LIST["Error"]},
    604: {"message": "OPC UA communication error detected.", "type_id": TYPE_ID_LIST["Warning"]},

    # Informational / skips
    700: {"message": "No changes detected. Record unchanged.", "type_id": TYPE_ID_LIST["Info"]},
    701: {"message": "Operation skipped due to business rules.", "type_id": TYPE_ID_LIST["Info"]},

    # Permissions
    800: {"message": "Permission denied. Access not allowed.", "type_id": TYPE_ID_LIST["Error"]},
    801: {"message": "Operation requires higher privileges.", "type_id": TYPE_ID_LIST["Warning"]},

    # Device Scan / Tag / UDT Operations
    900: {"message": "Device(s) scanned successfully.", "type_id": TYPE_ID_LIST["Success"]},
    901: {"message": "No stations found for selected device.", "type_id": TYPE_ID_LIST["Warning"]},
    902: {"message": "Station metadata read failed.", "type_id": TYPE_ID_LIST["Error"]},
    903: {"message": "UDT instance(s) created successfully.", "type_id": TYPE_ID_LIST["Success"]},
    904: {"message": "Instance creation skipped: Instance already exists.", "type_id": TYPE_ID_LIST["Warning"]},
    905: {"message": "UDT creation failed due to exception.", "type_id": TYPE_ID_LIST["Error"]},
    906: {"message": "Partial success: One or more instance skipped.", "type_id": TYPE_ID_LIST["Warning"]},
    907: {"message": "Device tags browsed successfully.", "type_id": TYPE_ID_LIST["Info"]},
    908: {"message": "Device tag quality is bad or missing.", "type_id": TYPE_ID_LIST["Warning"]},
    909: {"message": "UDT deleted successfully.", "type_id": TYPE_ID_LIST["Success"]},
    1000: {"message": "Part number scanned successfully.", "type_id": TYPE_ID_LIST["Success"]}
    
    
    

}
# -----------------------------------------------------
# Function to get message + type_id for a given code
# -----------------------------------------------------
def get_response_code_mapping(code):

    return RESPONSE_CODE_MAP.get(
        code,
        {"message": "Unknown response code.", "type_id": TYPE_ID_LIST["Info"]}
    )