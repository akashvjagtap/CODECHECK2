# <summary>
# Created By: Akash J
# Creation Date: 07/17/2024
# Comments: Project-level logger utilities for error/info/warn logging.
# </summary>

from MagnaDataOps.SecureInputUtils import sanitize_for_logging
from sys import exc_info


def log_error(location, user_id=0, logger_name="MagnaDataOps", isDBWrite=False):
	"""
	Logs an error to system logs and optionally to DB.
	"""

	logger = system.util.getLogger(sanitize_for_logging(logger_name))
	exc_type, exc_obj, tb = exc_info()
	lineno = tb.tb_lineno if tb else "Unknown"

	full_msg = "Error at {} |(Line {}) | Exception: {}".format(
		sanitize_for_logging(location),
		sanitize_for_logging(lineno),
		sanitize_for_logging(exc_obj)
	)

	logger.error(full_msg)
	custom_print(full_msg)

	if isDBWrite:
		short_msg = "Line {} | Exception: {}".format(
			sanitize_for_logging(lineno),
			sanitize_for_logging(exc_obj)
		)
		log_to_db("ERROR", location, short_msg, user_id)
			

def log_warn(location, user_id=0, message="Potential issue", logger_name="MagnaDataOps", isDBWrite=False):
	"""
	Logs a warning to system logs and optionally to DB.
	"""
	logger = system.util.getLogger(sanitize_for_logging(logger_name))

	full_msg = "Warning at {}: {}".format(
		sanitize_for_logging(location),
		sanitize_for_logging(message)
	)

	logger.warn(full_msg)
	custom_print(full_msg)

	if isDBWrite:
		log_to_db("WARN", location, sanitize_for_logging(message), user_id)


def log_info(location, user_id=0, message="Operation completed successfully", logger_name="MagnaDataOps", isDBWrite=False):
	"""
	Logs an info message to system logs and optionally to DB.
	"""
	logger = system.util.getLogger(sanitize_for_logging(logger_name))

	full_msg = "Notification at {}: {}".format(
		sanitize_for_logging(location),
		sanitize_for_logging(message)
	)

	logger.info(full_msg)
	custom_print(full_msg)

	if isDBWrite:
		log_to_db("INFO", location, sanitize_for_logging(message), user_id)


def custom_print(message):
	"""
	Prints to Perspective or Gateway console, whichever is valid.
	"""
	try:
		system.perspective.print(message)
	except:
		print(message)


def get_full_path_with_context(component, viewPath="UnknownView", scriptContext=""):
	"""
	Builds a full path like: ViewPath:component/path :: onActionPerformed
	"""
	try:
		def build_path(comp):
			path = comp.name
			if hasattr(comp, 'parent') and comp.parent is not None:
				path = build_path(comp.parent) + "/" + path
			return path

		component_path = build_path(component)
		full_path = "{}:{}".format(viewPath, component_path)
		if scriptContext:
			full_path += " :: {}".format(scriptContext)

		return full_path
	except:
		return "UnknownView:UnknownComponent"


def log_to_db(log_level, location, message,user_id):
	"""
	Internal DB writer for structured logs.
	"""
	try:
		base, script_context = location.split("::")
		view_path, component_path = base.split(":")

		params = {
			"log_level": log_level,
			"view_path": view_path.strip(),
			"component_path": component_path.strip(),
			"script_context": script_context.strip(),
			"message": message,
			"created_by": None
		}

		try:
			params["created_by"] = user_id
		except:
			params["created_by"] = None

		system.db.runNamedQuery("MagnaDataOps/Common/insertLogActivity", params)

	except Exception as e:
		system.util.getLogger("MagnaDataOps").error("Failed to log to DB: " + str(e))