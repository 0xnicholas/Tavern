CREATE TABLE workflow_events (
    id          BIGSERIAL PRIMARY KEY,
    instance_id TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_events_instance_seq ON workflow_events(instance_id, id);

CREATE TABLE workflow_snapshots (
    instance_id TEXT PRIMARY KEY,
    state       JSONB NOT NULL,
    version     INTEGER NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE workflow_instances (
    instance_id   TEXT PRIMARY KEY,
    workflow_id   TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now(),
    completed_at  TIMESTAMPTZ
);
CREATE INDEX idx_instances_status ON workflow_instances(status);
CREATE INDEX idx_instances_workflow ON workflow_instances(workflow_id);
