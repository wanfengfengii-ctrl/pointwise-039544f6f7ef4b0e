import base64
import binascii
import hashlib
from capture.source.envelope import EnvelopeError


def _require_object_id(value):
    if type(value) is not str or not value:
        raise EnvelopeError("object_id must be a nonempty string")
    return value


def _require_nonnegative_int(value, name):
    if type(value) is not int or value < 0:
        raise EnvelopeError(name + " must be a nonnegative integer")
    return value


def decode_part_data(data):
    if type(data) is not str:
        raise EnvelopeError("part data must be a base64 string")
    try:
        raw = data.encode("ascii")
    except UnicodeEncodeError as exc:
        raise EnvelopeError("part data must be ASCII base64") from exc
    try:
        decoded = base64.b64decode(raw, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise EnvelopeError("part data is not standard base64") from exc
    if base64.b64encode(decoded) != raw:
        raise EnvelopeError("part data is not canonical base64")
    return decoded


def parse_object_part_payload(payload):
    if type(payload) is not dict or set(payload) != {"object_id", "index", "data"}:
        raise EnvelopeError("object_part payload must be exactly object_id, index, data")
    object_id = _require_object_id(payload["object_id"])
    index = _require_nonnegative_int(payload["index"], "index")
    return object_id, index, decode_part_data(payload["data"])


def parse_object_seal_payload(payload):
    if type(payload) is not dict or set(payload) != {"object_id", "parts", "size", "sha256"}:
        raise EnvelopeError("object_seal payload must be exactly object_id, parts, size, sha256")
    object_id = _require_object_id(payload["object_id"])
    parts = _require_nonnegative_int(payload["parts"], "parts")
    size = _require_nonnegative_int(payload["size"], "size")
    digest = payload["sha256"]

    if (type(digest) is not str or len(digest) != 64 or
            any(c not in "0123456789abcdef" for c in digest)):
        raise EnvelopeError("sha256 must be 64 lowercase hex characters")
    return object_id, parts, size, digest


def parse_object_event(event):
    if event.kind == "object_part":
        return ("part",) + parse_object_part_payload(event.payload)
    if event.kind == "object_seal":
        return ("seal",) + parse_object_seal_payload(event.payload)
    raise EnvelopeError("not an object event")


class ObjectStore:
    """Durable acceptance of object parts and seals for one top transaction.

    Rows in `objects`/`object_parts` are the intake staging copies of object
    content. Part and content digests survive reclamation as dedup evidence;
    byte content is scrubbed by the retention manager once confirmed and no
    longer needed by an unpublished transaction.
    """

    def __init__(self, connection):
        self.connection = connection

    def _row(self, top_id, object_id):
        return self.connection.execute(
            "SELECT parts,size,sha256,content,complete,reclaimed FROM objects "
            "WHERE top_id=? AND object_id=?", (top_id, object_id)).fetchone()

    def _ensure(self, top_id, object_id):
        self.connection.execute(
            "INSERT INTO objects(top_id,object_id,parts,size,sha256,content,complete,reclaimed) "
            "VALUES(?,?,NULL,NULL,NULL,NULL,0,0) ON CONFLICT(top_id,object_id) DO NOTHING",
            (top_id, object_id))

    def accept(self, event):
        parsed = parse_object_event(event)
        if parsed[0] == "part":
            self.accept_part(event.top_id, parsed[1], parsed[2], parsed[3])
        else:
            self.accept_seal(event.top_id, parsed[1], parsed[2], parsed[3], parsed[4])

    def accept_part(self, top_id, object_id, index, data):
        row = self._row(top_id, object_id)
        if row is not None and row[0] is not None and index >= row[0]:
            raise EnvelopeError("fragment index outside sealed part count")
        digest = hashlib.sha256(data).hexdigest()
        existing = self.connection.execute(
            "SELECT data,digest FROM object_parts WHERE top_id=? AND object_id=? AND idx=?",
            (top_id, object_id, index)).fetchone()
        if existing is not None:
            if existing[0] is not None:
                if bytes(existing[0]) != data:
                    raise EnvelopeError("fragment bytes conflict with accepted part")
            elif existing[1] != digest:
                raise EnvelopeError("fragment bytes conflict with accepted part")
        else:
            self.connection.execute(
                "INSERT INTO object_parts(top_id,object_id,idx,data,digest) VALUES(?,?,?,?,?)",
                (top_id, object_id, index, data, digest))
        self._ensure(top_id, object_id)
        # A newly accepted fact is an unscrubbed persistent copy again.
        self.connection.execute(
            "UPDATE objects SET reclaimed=0 WHERE top_id=? AND object_id=? AND reclaimed=1",
            (top_id, object_id))
        self._maybe_complete(top_id, object_id)

    def accept_seal(self, top_id, object_id, parts, size, sha256):
        row = self._row(top_id, object_id)
        if row is not None and row[0] is not None:
            if (row[0], row[1], row[2]) != (parts, size, sha256):
                raise EnvelopeError("seal declaration conflicts with accepted seal")
        else:
            out_of_range = self.connection.execute(
                "SELECT 1 FROM object_parts WHERE top_id=? AND object_id=? AND idx>=? LIMIT 1",
                (top_id, object_id, parts)).fetchone()
            if out_of_range is not None:
                raise EnvelopeError("sealed part count below accepted fragment index")
            self._ensure(top_id, object_id)
            self.connection.execute(
                "UPDATE objects SET parts=?,size=?,sha256=? WHERE top_id=? AND object_id=?",
                (parts, size, sha256, top_id, object_id))
        self._maybe_complete(top_id, object_id)

    def _maybe_complete(self, top_id, object_id):
        row = self._row(top_id, object_id)
        if row is None or row[0] is None:
            return
        parts, size, sha256 = row[0], row[1], row[2]
        stored = self.connection.execute(
            "SELECT idx,data FROM object_parts WHERE top_id=? AND object_id=? ORDER BY idx",
            (top_id, object_id)).fetchall()
        if len(stored) != parts:
            return
        if any(index != expected for expected, (index, _) in enumerate(stored)):
            raise EnvelopeError("fragment indexes are not contiguous")
        if any(data is None for _, data in stored):
            return  # content was reclaimed; digests remain as evidence
        body = b"".join(bytes(data) for _, data in stored)
        if len(body) != size:
            raise EnvelopeError("assembled object length differs from seal")
        if hashlib.sha256(body).hexdigest() != sha256:
            raise EnvelopeError("assembled object digest differs from seal")
        try:
            body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise EnvelopeError("assembled object is not strict UTF-8") from exc
        self.connection.execute(
            "UPDATE objects SET content=?,complete=1,reclaimed=0 "
            "WHERE top_id=? AND object_id=?", (body, top_id, object_id))

    def content(self, top_id, object_id):
        row = self.connection.execute(
            "SELECT content FROM objects WHERE top_id=? AND object_id=? AND complete=1",
            (top_id, object_id)).fetchone()
        return None if row is None or row[0] is None else bytes(row[0])

    def missing(self, top_id, object_ids):
        return tuple(object_id for object_id in sorted(object_ids)
                     if self.content(top_id, object_id) is None)

    def resolve(self, top_id, row):
        for name, value in row.items():
            if type(value) is dict:
                body = self.content(top_id, value["$object"])
                if body is None:
                    raise EnvelopeError("referenced object content is unavailable")
                row[name] = body.decode("utf-8")
        return row
