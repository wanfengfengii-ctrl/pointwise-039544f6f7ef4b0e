from dataclasses import dataclass
from typing import Protocol
from capture.source.codec import canonical_bytes, decode_canonical


class SessionConflictError(ValueError):
    pass


@dataclass(frozen=True)
class SourceSession:
    session_id: str
    event_version: int
    tables: dict

    def as_dict(self):
        return {"session_id": self.session_id, "event_version": self.event_version,
                "tables": self.tables}


@dataclass(frozen=True)
class PollResult:
    state: str
    segment: object = None

    def __post_init__(self):
        if self.state not in ("segment", "idle", "end"):
            raise ValueError("unknown poll state")
        if (self.state == "segment") != (self.segment is not None):
            raise ValueError("segment required only for segment state")


class SourcePort(Protocol):
    def describe(self) -> SourceSession: ...
    def poll(self, after_position: int) -> PollResult: ...
    def acknowledge(self, position: int) -> None: ...
    def close(self) -> None: ...


def validate_session(session):
    if not isinstance(session, SourceSession):
        raise SessionConflictError("describe must return SourceSession")
    if type(session.session_id) is not str or not session.session_id:
        raise SessionConflictError("nonempty session_id required")
    if type(session.event_version) is not int or session.event_version != 1:
        raise SessionConflictError("unsupported event format")
    if type(session.tables) is not dict or not session.tables:
        raise SessionConflictError("fixed tables required")
    tables = decode_canonical(canonical_bytes(session.tables))
    return SourceSession(session.session_id, 1, tables)
