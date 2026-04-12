-- ============================================================
-- Enterprise Distributed Task Scheduler — Database Schema
-- ============================================================

-- Clean up existing tables (for dev resets)
DROP TABLE IF EXISTS execution_history CASCADE;
DROP TABLE IF EXISTS task_schedule CASCADE;
DROP TABLE IF EXISTS workflow_steps CASCADE;
DROP TABLE IF EXISTS workflows CASCADE;
DROP TABLE IF EXISTS jobs CASCADE;
DROP TYPE IF EXISTS job_status CASCADE;

-- Job status lifecycle enum
CREATE TYPE job_status AS ENUM (
    'PENDING',
    'SCHEDULED',
    'QUEUED',
    'RUNNING',
    'COMPLETED',
    'FAILED',
    'RETRY_WAIT',
    'DEAD_LETTER'
);

-- ============================================================
-- WORKFLOWS: A workflow is a collection of steps forming a DAG
-- ============================================================
CREATE TABLE workflows (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    status          VARCHAR(50) NOT NULL DEFAULT 'RUNNING',
    total_steps     INT NOT NULL DEFAULT 0,
    completed_steps INT NOT NULL DEFAULT 0,
    failed_steps    INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- WORKFLOW STEPS: Each step is a node in the DAG
-- ============================================================
CREATE TABLE workflow_steps (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id   UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_id       VARCHAR(100) NOT NULL,
    type          VARCHAR(50) NOT NULL,
    config        JSONB NOT NULL DEFAULT '{}',
    depends_on    TEXT[] NOT NULL DEFAULT '{}',
    status        job_status NOT NULL DEFAULT 'PENDING',
    result        JSONB,
    error         TEXT,
    attempt       INT NOT NULL DEFAULT 0,
    max_retries   INT NOT NULL DEFAULT 3,
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workflow_id, step_id)
);

-- ============================================================
-- JOBS: Standalone single-step jobs (backward compatible)
-- ============================================================
CREATE TABLE jobs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       VARCHAR(255),
    type          VARCHAR(50) NOT NULL,
    payload       JSONB NOT NULL DEFAULT '{}',
    status        job_status NOT NULL DEFAULT 'PENDING',
    attempt       INT NOT NULL DEFAULT 0,
    max_retries   INT NOT NULL DEFAULT 3,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- TASK SCHEDULE: Time-based dispatch for both jobs and steps
-- ============================================================
CREATE TABLE task_schedule (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                UUID REFERENCES jobs(id) ON DELETE CASCADE,
    workflow_step_id      UUID REFERENCES workflow_steps(id) ON DELETE CASCADE,
    execution_time_minute BIGINT NOT NULL,
    segment               INT NOT NULL,
    status                VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- At least one of job_id or workflow_step_id must be set
    CONSTRAINT chk_schedule_target CHECK (job_id IS NOT NULL OR workflow_step_id IS NOT NULL)
);

-- ============================================================
-- EXECUTION HISTORY: Audit log for all task executions
-- ============================================================
CREATE TABLE execution_history (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id        UUID REFERENCES jobs(id) ON DELETE SET NULL,
    workflow_id   UUID REFERENCES workflows(id) ON DELETE SET NULL,
    step_id       VARCHAR(100),
    executor_type VARCHAR(50) NOT NULL,
    status        VARCHAR(50) NOT NULL,
    attempt       INT NOT NULL DEFAULT 1,
    duration_ms   BIGINT,
    error         TEXT,
    metadata      JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- INDEXES: For query performance
-- ============================================================

-- Workflow step lookups
CREATE INDEX idx_workflow_steps_workflow_id ON workflow_steps(workflow_id);
CREATE INDEX idx_workflow_steps_status ON workflow_steps(workflow_id, status);

-- Schedule polling (the hot path — scheduler queries this every tick)
CREATE INDEX idx_task_schedule_poll ON task_schedule(execution_time_minute, segment, status);

-- Execution history queries
CREATE INDEX idx_exec_history_job ON execution_history(job_id);
CREATE INDEX idx_exec_history_workflow ON execution_history(workflow_id);

-- Job status lookups
CREATE INDEX idx_jobs_status ON jobs(status);

-- Workflow status lookups
CREATE INDEX idx_workflows_status ON workflows(status);
