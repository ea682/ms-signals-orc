-- Lifecycle promotion/adoption workers may run on multiple application replicas.
-- ShedLock uses this database-time lease to ensure that only one replica executes
-- a given lifecycle scan while crash recovery remains bounded by lock_until.
CREATE TABLE IF NOT EXISTS futuros_operaciones.shedlock (
    name varchar(64) NOT NULL PRIMARY KEY,
    lock_until timestamp(3) NOT NULL,
    locked_at timestamp(3) NOT NULL,
    locked_by varchar(255) NOT NULL
);
