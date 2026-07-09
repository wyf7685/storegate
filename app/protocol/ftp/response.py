"""FTP reply code constants and response formatting utilities (RFC 959)."""

from enum import Enum


class R(int, Enum):
    """FTP reply codes as an enumeration."""

    # ------------------------------------------------------------------
    # Positive Preliminary Reply (1xx)
    # ------------------------------------------------------------------
    DATA_OPEN_ALREADY = 125  # Data connection already open; transfer starting
    DATA_OPEN = 150  # File status okay; about to open data connection

    # ------------------------------------------------------------------
    # Positive Completion Reply (2xx)
    # ------------------------------------------------------------------
    SUCCESS = 200
    READY = 220  # Service ready for new user
    CLOSING = 221  # Service closing control connection
    TRANSFER_OK = 226  # Closing data connection; transfer OK
    PASSIVE_MODE = 227  # Entering Passive Mode (h1,h2,h3,h4,p1,p2)
    LOGGED_IN = 230  # User logged in
    ACTION_OK = 250  # Requested file action okay
    PATH_CREATED = 257  # "PATHNAME" created
    SYSTEM_STATUS = 211  # System status
    FILE_STATUS = 213  # File status
    HELP = 214  # Help message
    SYSTEM_TYPE = 215  # System type

    # ------------------------------------------------------------------
    # Positive Intermediate Reply (3xx)
    # ------------------------------------------------------------------
    NEED_PASSWORD = 331  # User name okay, need password
    PENDING_INFO = 350  # Requested file action pending further information

    # ------------------------------------------------------------------
    # Transient Negative Completion Reply (4xx)
    # ------------------------------------------------------------------
    NO_DATA_CONN = 425  # Can't open data connection
    TRANSFER_ABORTED = 426  # Connection closed; transfer aborted
    LOCAL_ERROR = 451  # Local error in processing

    # ------------------------------------------------------------------
    # Permanent Negative Completion Reply (5xx)
    # ------------------------------------------------------------------
    SYNTAX_ERROR = 501  # Syntax error in parameters
    NOT_IMPLEMENTED = 502  # Command not implemented
    BAD_SEQUENCE = 503  # Bad sequence of commands
    NOT_LOGGED_IN = 530  # Not logged in
    NOT_AVAILABLE = 550  # File unavailable / access denied

    def __call__(self, text: str) -> str:
        """Format a single-line FTP response."""
        return f"{self.value} {text}"
