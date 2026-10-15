-- Keep bootstrap model records on rollback: users may have edited them or
-- referenced them from agents/knowledge bases. This migration only seeds data.
SELECT 1;
