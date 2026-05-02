-- Sprint 3 verification script
-- Usage example:
-- PowerShell:
-- Get-Content scripts/check_raw_tables.sql | docker compose exec -T postgres psql -U postgres -d postgres
-- Bash:
-- docker compose exec -T postgres psql -U postgres -d postgres < scripts/check_raw_tables.sql

\timing on

SELECT 'members' AS table_name, COUNT(*) AS row_count FROM members;
SELECT 'transactions' AS table_name, COUNT(*) AS row_count FROM transactions;
SELECT 'user_logs' AS table_name, COUNT(*) AS row_count FROM user_logs;
SELECT 'train_label' AS table_name, COUNT(*) AS row_count FROM train_label;

SELECT t.msno, t.is_churn, m.city
FROM train_label t
LEFT JOIN members m ON t.msno = m.msno
LIMIT 10;
