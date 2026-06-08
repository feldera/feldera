-- rule: cast_interval_literal
-- spark: CAST('3' AS INTERVAL DAY) / CAST('interval 3 day' AS interval day) — string to interval
-- feldera: INTERVAL '3' DAY — drop CAST, use interval literal directly
CREATE OR REPLACE TEMP VIEW milestone_windows AS SELECT project_milestones.milestone_id, project_milestones.milestone_name, project_milestones.start_date, (project_milestones.start_date + CAST('10' AS INTERVAL DAY)) AS expected_end, (project_milestones.start_date - CAST('2' AS INTERVAL DAY)) AS prep_date FROM project_milestones WHERE project_milestones.duration_days > 5;
