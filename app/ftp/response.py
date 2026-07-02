"""FTP reply code constants and response formatting utilities (RFC 959)."""


def reply(code: str, text: str) -> str:
    """Format a single-line FTP response."""
    return f"{code} {text}"


# ------------------------------------------------------------------
# Positive Preliminary Reply (1xx)
# ------------------------------------------------------------------
R_DATA_OPEN_ALREADY = "125"  # Data connection already open; transfer starting
R_DATA_OPEN = "150"  # File status okay; about to open data connection

# ------------------------------------------------------------------
# Positive Completion Reply (2xx)
# ------------------------------------------------------------------
R_READY = "220"  # Service ready for new user
R_CLOSING = "221"  # Service closing control connection
R_TRANSFER_OK = "226"  # Closing data connection; transfer OK
R_PASSIVE_MODE = "227"  # Entering Passive Mode (h1,h2,h3,h4,p1,p2)
R_LOGGED_IN = "230"  # User logged in
R_ACTION_OK = "250"  # Requested file action okay
R_PATH_CREATED = "257"  # "PATHNAME" created
R_SYSTEM_STATUS = "211"  # System status
R_FILE_STATUS = "213"  # File status
R_HELP = "214"  # Help message
R_SYSTEM_TYPE = "215"  # System type

# ------------------------------------------------------------------
# Positive Intermediate Reply (3xx)
# ------------------------------------------------------------------
R_NEED_PASSWORD = "331"  # noqa: S105  # User name okay, need password
R_PENDING_INFO = "350"  # Requested file action pending further information

# ------------------------------------------------------------------
# Transient Negative Completion Reply (4xx)
# ------------------------------------------------------------------
R_NO_DATA_CONN = "425"  # Can't open data connection
R_TRANSFER_ABORTED = "426"  # Connection closed; transfer aborted
R_LOCAL_ERROR = "451"  # Local error in processing

# ------------------------------------------------------------------
# Permanent Negative Completion Reply (5xx)
# ------------------------------------------------------------------
R_SYNTAX_ERROR = "501"  # Syntax error in parameters
R_NOT_IMPLEMENTED = "502"  # Command not implemented
R_BAD_SEQUENCE = "503"  # Bad sequence of commands
R_NOT_LOGGED_IN = "530"  # Not logged in
R_NOT_AVAILABLE = "550"  # File unavailable / access denied
