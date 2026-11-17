import hashlib
import json
import math


class CanonicalEncodingError(ValueError):
    pass


def _validate(value):
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float and math.isfinite(value):
        return
    if type(value) is list:
        for item in value:
            _validate(item)
        return
    if type(value) is dict and all(type(key) is str for key in value):
        for item in value.values():
            _validate(item)
        return
    raise CanonicalEncodingError("value is not a finite JSON value")


def canonical_bytes(value):
    _validate(value)
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise CanonicalEncodingError(str(exc)) from exc


def content_digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def decode_canonical(data):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise CanonicalEncodingError("duplicate JSON member")
            result[key] = value
        return result
    try:
        value = json.loads(data, object_pairs_hook=unique)
        _validate(value)
        return value
    except (ValueError, TypeError, UnicodeError) as exc:
        raise CanonicalEncodingError(str(exc)) from exc
