CREATE TABLE workflow_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    payload     TEXT NOT NULL,
    created_at  INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);
CREATE INDEX idx_events_instance_seq ON workflow_events(instance_id, id);

CREATE TABLE workflow_snapshots (
    instance_id TEXT PRIMARY KEY,
    state       TEXT NOT NULL,
    version     INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

CREATE TABLE workflow_instances (
    instance_id   TEXT PRIMARY KEY,
    workflow_id   TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    INTEGER DEFAULT (strftime('%s', 'now') * 1000),
    updated_at    INTEGER DEFAULT (strftime('%s', 'now') * 1000),
    completed_at  INTEGER
);
CREATE INDEX idx_instances_status ON workflow_instances(status);
CREATE INDEX idx_instances_workflow ON workflow_instances(workflow_id);
