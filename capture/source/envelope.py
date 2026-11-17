from dataclasses import dataclass
from capture.source.codec import canonical_bytes, decode_canonical


class EnvelopeError(ValueError):
    pass


KINDS = frozenset(("begin", "branch", "row", "seal", "rollback", "commit", "abort"))
FIELDS = frozenset(("session_id", "source_position", "event_id", "top_id",
                    "branch_id", "kind", "payload"))


@dataclass(frozen=True)
class EventEnvelope:
    session_id: str
    source_position: int
    event_id: str
    top_id: str
    branch_id: str
    kind: str
    encoded: bytes
    parent_id: str | None = None

    @property
    def payload(self):
        return self.as_dict()["payload"]

    def as_dict(self):
        return decode_canonical(self.encoded)


def validate_event_fields(value):
    if type(value) is not dict:
        raise EnvelopeError("event must be an object")
    kind = value.get("kind")
    if type(kind) is not str or kind not in KINDS:
        raise EnvelopeError("unknown event kind")
    required = FIELDS | ({"parent_id"} if kind == "branch" else set())
    if set(value) != required:
        raise EnvelopeError("unexpected or missing envelope fields")
    for name in ("session_id", "event_id", "top_id", "branch_id"):
        if type(value[name]) is not str or not value[name]:
            raise EnvelopeError("nonempty identifier required: " + name)
    if type(value["source_position"]) is not int or value["source_position"] < 1:
        raise EnvelopeError("source_position must be positive integer")
    if type(value["payload"]) is not dict:
        raise EnvelopeError("payload must be object")
    if kind != "row" and value["payload"]:
        raise EnvelopeError("control payload must be empty")
    if kind == "branch" and (type(value["parent_id"]) is not str or
                             not value["parent_id"]):
        raise EnvelopeError("parent_id required")


def parse_envelope(value):
    if isinstance(value, EventEnvelope):
        value = value.as_dict()
    validate_event_fields(value)
    encoded = canonical_bytes(value)
    return EventEnvelope(value["session_id"], value["source_position"],
                         value["event_id"], value["top_id"], value["branch_id"],
                         value["kind"], encoded, value.get("parent_id"))
