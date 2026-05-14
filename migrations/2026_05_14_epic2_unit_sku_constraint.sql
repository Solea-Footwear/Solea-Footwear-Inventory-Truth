-- EPIC 2 Ticket 2.1 — prevent empty-string unit_code on units table
-- Safe to run multiple times (idempotent).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'check_unit_code_nonempty'
          AND conrelid = 'units'::regclass
    ) THEN
        ALTER TABLE units
            ADD CONSTRAINT check_unit_code_nonempty
            CHECK (unit_code != '');
    END IF;
END;
$$;
