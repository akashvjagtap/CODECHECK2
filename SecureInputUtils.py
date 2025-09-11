# <summary>
# Module Name : SecureInputUtils
# Created By  : Akash J
# Creation Date: 05/08/2025
# Modified By :
# Modified Date:
# Comments    : Functions to sanitize folder/file/table names and validate file uploads.
#               Minimal, conservative changes to address static analysis warnings.
# </summary>

import system
from traceback import format_exc
from os.path import basename, splitext
from re import compile as re_compile

from MagnaDataOps.ProjectLists import (
    ALLOWED_EXTENSIONS,
    DANGEROUS_EXTENSIONS,
)

# NOTE:
# WHITELISTED_FOLDERS, WHITELISTED_FOLDERS_FILEUPLOAD, WHITELISTED_TABLES
# are expected to be provided by the project (e.g., ProjectLists or a gateway script).
# These functions remain whitelist-first; we only add extra input guards.

# --- local logger (never throws) ---
def _log_warn(where, msg):
    try:
        system.util.getLogger("SecureInputUtils").warn(u"%s | %s" % (where, msg))
    except Exception:
        # do not raise from utility logging
        return

def _log_error(where):
    try:
        system.util.getLogger("SecureInputUtils").error(u"%s\n%s" % (where, format_exc()))
    except Exception:
        return

# --- Precompiled patterns (simple char classes; no nested groups → no backtracking risk) ---
_SAFE_FILENAME_RE = re_compile(r'[^A-Za-z0-9_.-]+')           # replace any non-safe char
_LOG_SANITIZE_RE  = re_compile(r'[\r\n\t\x00-\x1f\x7f]+')      # strip control chars & CRLF
# SQL-ish identifier guard: start with letter/underscore; then letters/digits/underscore (≤64)
_SQL_IDENT_RE     = re_compile(r'^[A-Za-z_][A-Za-z0-9_]{0,63}$')
# Folder key guard (identical semantics to table identifiers for safety)
_FOLDER_KEY_RE    = _SQL_IDENT_RE

# --- Sanitization Utilities ---

def sanitize_filename(filename):
    """
    Remove potentially dangerous characters from a filename component.
    Preserves dots, underscores, and hyphens. Does not touch directory parts.
    """
    base = basename(u"" if filename is None else filename)
    # Use a single substitution pass with a character class (no catastrophic backtracking)
    return _SAFE_FILENAME_RE.sub('_', base)

def sanitize_for_logging(text):
    """
    Remove CR/LF, tabs, and ASCII control characters to avoid log injection.
    """
    s = u"" if text is None else unicode(text)
    return _LOG_SANITIZE_RE.sub(' ', s).strip()

# --- Whitelist Checks ---

def is_valid_folder(folder):
    """
    Check that the folder key is syntactically safe AND whitelisted.
    """
    try:
        # Strong guard against injection-y keys (even if a whitelist exists)
        if folder is None or not _FOLDER_KEY_RE.match(unicode(folder)):
            return False
        return folder in WHITELISTED_FOLDERS
    except Exception:
        # If the whitelist is missing/not imported, fail safe.
        return False

def is_valid_folder_FileUpload(folder):
    """
    Check that the folder key (for uploads) is syntactically safe AND whitelisted.
    """
    try:
        if folder is None or not _FOLDER_KEY_RE.match(unicode(folder)):
            return False
        return folder in WHITELISTED_FOLDERS_FILEUPLOAD
    except Exception:
        return False

def is_valid_table(table_name):
    """
    Check that the table identifier is syntactically safe AND whitelisted.
    This prevents unsafe identifiers being used downstream in dynamic SQL.
    """
    try:
        if table_name is None or not _SQL_IDENT_RE.match(unicode(table_name)):
            return False
        return table_name in WHITELISTED_TABLES
    except Exception:
        return False

# --- File Validation ---

def is_file_size_valid(file_obj, max_size_mb=20):
    """
    Check that file size is within allowed limit.
    `file_obj` must have `.getBytes()` (Ignition upload object).
    """
    if file_obj is None:
        return False
    try:
        size_bytes = len(file_obj.getBytes())
    except Exception:
        return False
    return size_bytes <= int(max_size_mb) * 1024 * 1024

def has_double_extension(filename):
    """
    Detect suspicious multi-extension patterns (e.g., 'image.jpg.exe').
    Returns (is_suspicious: bool, sanitized_basename_or_None)
    """
    name = basename(u"" if filename is None else filename).lower()
    parts = name.split('.')

    if len(parts) < 2:
        return True, None  # No extension at all → treat as suspicious

    last_ext = '.' + parts[-1]
    if last_ext not in ALLOWED_EXTENSIONS:
        return True, None  # Final extension not allowed

    # If any *inner* token is a dangerous extension, block
    for inner in parts[:-1]:
        if '.' + inner in DANGEROUS_EXTENSIONS:
            return True, None  # e.g., resume.pdf.exe

    # Collapse repeated endings, keep last extension as-is
    sanitized = '.'.join(parts[:-1]) + last_ext
    return False, sanitized

def is_extension_allowed(filename):
    """
    Ensure the last extension is explicitly allowed.
    """
    _, ext = splitext((u"" if filename is None else filename).lower())
    return ext in ALLOWED_EXTENSIONS

# --- MIME Type Validation ---

def get_mime_type(file_obj):
    """
    Attempt to extract a MIME type using Java activation map.
    Returns a string or None. Purely advisory (do not trust MIME alone).
    """
    if file_obj is None:
        return None
    try:
        from java.io import ByteArrayInputStream
        from javax.activation import MimetypesFileTypeMap

        b = file_obj.getBytes()
        stream = ByteArrayInputStream(b)
        try:
            mime_map = MimetypesFileTypeMap()
            return mime_map.getContentType(stream)
        finally:
            try:
                stream.close()
            except Exception as e:
                _log_warn("get_mime_type:stream.close", unicode(e))
    except Exception:
        _log_error("get_mime_type")
        return None

def is_mime_type_allowed(mime_type):
    """
    Whitelist MIME types that are safe to store/view in this project.
    """
    return mime_type in {
        'application/pdf',
        'image/jpeg',
        'image/png',
        'image/gif',
        'image/tiff',
        'image/bmp',
        'image/x-ms-bmp',
        'image/x-dib',
        'text/plain',
        'text/csv',
        'application/msword',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/vnd.ms-excel',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.ms-powerpoint',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'video/mp4',
        'application/octet-stream',
    }

# --- ID Validation ---

def validate_user_id(user_id):
    """
    Ensure user_id is a positive integer (server-side authority check is expected elsewhere).
    Raises ValueError if invalid.
    """
    if not isinstance(user_id, (int, long)) or int(user_id) <= 0:
        raise ValueError("Unauthorized: invalid user ID.")
    return True