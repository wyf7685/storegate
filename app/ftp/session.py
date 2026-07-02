"""Per-client FTP session state."""

from dataclasses import dataclass

from anyio.abc import SocketListener


@dataclass(slots=True)
class FTPSession:
    """Mutable per-client session state.

    Attributes:
        user: The username provided via USER command.
        authenticated: Whether the client has successfully logged in.
        cwd: Current working directory (always starts and ends with "/").
        transfer_type: "I" (binary/image) or "A" (ASCII). Default "I".
        rename_from: Source path stored by RNFR, consumed by RNTO.
        data_mode: "pasv" or "port" when a data channel is configured.
        port_addr: Client-side (host, port) for active (PORT) mode.
        pasv_listener: Listener for passive (PASV) mode data connections.
    """

    user: str | None = None
    authenticated: bool = False
    cwd: str = "/"
    transfer_type: str = "I"
    rename_from: str | None = None

    # Data channel state
    data_mode: str | None = None
    port_addr: tuple[str, int] | None = None
    pasv_listener: SocketListener | None = None

    def reset_data_state(self) -> None:
        """Clear data channel state after a transfer completes or is aborted."""
        self.data_mode = None
        self.port_addr = None
        self.pasv_listener = None
