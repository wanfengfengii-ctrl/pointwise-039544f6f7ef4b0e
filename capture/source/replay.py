from pathlib import Path
from capture.source.codec import decode_canonical
from capture.source.session import PollResult, validate_session
from capture.source.segments import Segment, parse_segment


def load_replay_events(path):
    data = decode_canonical(Path(path).read_bytes())
    if type(data) is not list:
        raise ValueError("replay file must be an event list")
    return parse_segment({"events": data}).events


class ReplaySource:
    def __init__(self, session, events=(), batch_size=3, idle_polls=(),
                 lose_ack_once=(), truncate_on_ack=False):
        self.session = validate_session(session)
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("positive batch_size required")
        self.events = tuple(events)
        if self.events:
            self.events = parse_segment({"events": self.events}).events
            if self.events[0].source_position != 1:
                raise ValueError("replay must start at position 1")
            if any(e.session_id != self.session.session_id for e in self.events):
                raise ValueError("replay session mismatch")
        self.batch_size = batch_size
        self.idle_polls = set(idle_polls)
        self.lose_ack_once = set(lose_ack_once)
        self.truncate_on_ack = truncate_on_ack
        self.acknowledged = 0
        self.ack_calls = []
        self.poll_count = 0
        self.closed = False
        self.maximum = self.events[-1].source_position if self.events else 0

    def _require_open(self):
        if self.closed:
            raise RuntimeError("source closed")

    def describe(self):
        self._require_open()
        return validate_session(self.session)

    def poll(self, after_position):
        self._require_open()
        if type(after_position) is not int or after_position < 0:
            raise ValueError("invalid poll position")
        if after_position < self.acknowledged and self.truncate_on_ack:
            raise ValueError("requested prefix was acknowledged and discarded")
        self.poll_count += 1
        if self.poll_count in self.idle_polls:
            return PollResult("idle")
        events = tuple(e for e in self.events if e.source_position > after_position)
        if not events:
            return PollResult("end")
        return PollResult("segment", Segment(events[:self.batch_size]))

    def acknowledge(self, position):
        self._require_open()
        if type(position) is not int or not self.acknowledged <= position <= self.maximum:
            raise ValueError("invalid acknowledgement")
        self.ack_calls.append(position)
        self.acknowledged = position
        if self.truncate_on_ack:
            self.events = tuple(e for e in self.events if e.source_position > position)
        if position in self.lose_ack_once:
            self.lose_ack_once.remove(position)
            raise OSError("acknowledgement response lost")

    def close(self):
        self.closed = True
