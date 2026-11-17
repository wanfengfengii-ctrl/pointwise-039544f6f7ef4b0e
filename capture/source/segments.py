from dataclasses import dataclass
from capture.source.envelope import parse_envelope


class SegmentPositionError(ValueError):
    pass


@dataclass(frozen=True)
class Segment:
    events: tuple

    @property
    def last_position(self):
        return self.events[-1].source_position if self.events else None


def parse_segment(value):
    if isinstance(value, Segment):
        raw = value.events
    elif type(value) is dict and set(value) == {"events"}:
        raw = value["events"]
    else:
        raise SegmentPositionError("segment must contain events")
    if not isinstance(raw, (list, tuple)) or not raw:
        raise SegmentPositionError("nonempty event list required")
    events = tuple(parse_envelope(item) for item in raw)
    for left, right in zip(events, events[1:]):
        if right.source_position != left.source_position + 1:
            raise SegmentPositionError("segment positions must be consecutive")
    return Segment(events)


def split_accepted_prefix(segment, accepted):
    split = 0
    while split < len(segment.events):
        if segment.events[split].source_position > accepted:
            break
        split += 1
    prefix, suffix = segment.events[:split], segment.events[split:]
    if suffix and suffix[0].source_position != accepted + 1:
        raise SegmentPositionError("new suffix must follow durable prefix")
    return prefix, suffix
