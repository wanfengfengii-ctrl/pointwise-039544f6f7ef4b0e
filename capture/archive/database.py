import sqlite3
from pathlib import Path
from contextlib import contextmanager
from capture.source.codec import canonical_bytes, decode_canonical
from capture.source.session import SessionConflictError


ARCHIVE_VERSION = 1


class StorageVersionError(ValueError):
    pass


_DDL = (
    "CREATE TABLE metadata(name TEXT PRIMARY KEY,value BLOB NOT NULL)",
    "CREATE TABLE progress(singleton INTEGER PRIMARY KEY CHECK(singleton=1),accepted INTEGER NOT NULL,confirmed INTEGER NOT NULL,publication INTEGER NOT NULL)",
    "CREATE TABLE identities(event_id TEXT PRIMARY KEY,position INTEGER NOT NULL UNIQUE,canonical BLOB,digest TEXT NOT NULL,session_id TEXT NOT NULL,top_id TEXT NOT NULL,branch_id TEXT NOT NULL,kind TEXT NOT NULL,object_id TEXT,object_index INTEGER)",
    "CREATE TABLE tops(top_id TEXT PRIMARY KEY,root_id TEXT NOT NULL,state TEXT NOT NULL,completion_id TEXT)",
    "CREATE TABLE branches(top_id TEXT NOT NULL,branch_id TEXT NOT NULL,parent_id TEXT,state TEXT NOT NULL,PRIMARY KEY(top_id,branch_id),FOREIGN KEY(top_id) REFERENCES tops(top_id))",
    "CREATE TABLE payloads(payload_id INTEGER PRIMARY KEY,last_position INTEGER NOT NULL,body BLOB NOT NULL)",
    "CREATE TABLE journal(position INTEGER PRIMARY KEY,top_id TEXT NOT NULL,branch_id TEXT NOT NULL,payload_id INTEGER NOT NULL,event_index INTEGER NOT NULL,FOREIGN KEY(top_id) REFERENCES tops(top_id),FOREIGN KEY(payload_id) REFERENCES payloads(payload_id))",
    "CREATE INDEX journal_top ON journal(top_id,position)",
    "CREATE INDEX journal_payload ON journal(payload_id)",
    "CREATE TABLE objects(top_id TEXT NOT NULL,object_id TEXT NOT NULL,parts INTEGER,size INTEGER,sha256 TEXT,content BLOB,complete INTEGER NOT NULL,reclaimed INTEGER NOT NULL,PRIMARY KEY(top_id,object_id))",
    "CREATE TABLE object_parts(top_id TEXT NOT NULL,object_id TEXT NOT NULL,idx INTEGER NOT NULL,data BLOB,digest TEXT NOT NULL,PRIMARY KEY(top_id,object_id,idx))",
    "CREATE TABLE pending(top_id TEXT PRIMARY KEY,commit_position INTEGER NOT NULL UNIQUE,completion_digest TEXT NOT NULL)",
    "CREATE TABLE pending_refs(top_id TEXT NOT NULL,object_id TEXT NOT NULL,PRIMARY KEY(top_id,object_id))",
    "CREATE TABLE archive_rows(table_name TEXT NOT NULL,key_data TEXT NOT NULL,row_data BLOB NOT NULL,PRIMARY KEY(table_name,key_data))",
    "CREATE TABLE receipts(top_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,event_id TEXT NOT NULL UNIQUE,digest TEXT NOT NULL,publication INTEGER NOT NULL UNIQUE)"
)


class ArchiveDatabase:
    def __init__(self, path, session=None):
        self.closed = False
        self.read_only = session is None
        if self.read_only:
            uri = Path(path).resolve().as_uri() + "?mode=ro"
            self.connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        else:
            self.connection = sqlite3.connect(str(path), isolation_level=None)
        connection = self.connection
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if version == 0 and not tables and session is not None:
                with self.write_unit():
                    for statement in _DDL:
                        connection.execute(statement)
                    connection.execute("INSERT INTO metadata VALUES('session',?)",
                                       (canonical_bytes(session.as_dict()),))
                    connection.execute("INSERT INTO progress VALUES(1,0,0,0)")
                    connection.execute("PRAGMA user_version=1")
            elif version != ARCHIVE_VERSION:
                raise StorageVersionError("unsupported archive version")
            row = connection.execute("SELECT value FROM metadata WHERE name='session'").fetchone()
            if row is None:
                raise StorageVersionError("archive has no session binding")
            self.session_data = decode_canonical(row[0])
            if session is not None and canonical_bytes(self.session_data) != canonical_bytes(session.as_dict()):
                raise SessionConflictError("archive is bound to another session or schema")
        except BaseException:
            connection.close()
            self.closed = True
            raise

    @contextmanager
    def write_unit(self):
        if self.closed:
            raise RuntimeError("database closed")
        if self.read_only or self.connection.in_transaction:
            raise RuntimeError("write unit unavailable")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    @contextmanager
    def read_snapshot(self):
        if self.closed:
            raise RuntimeError("database closed")
        nested = self.connection.in_transaction
        if not nested:
            self.connection.execute("BEGIN")
        try:
            yield self.connection
        finally:
            if not nested:
                self.connection.rollback()

    def close(self):
        if not self.closed:
            self.connection.close()
            self.closed = True
