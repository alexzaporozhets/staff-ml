CREATE TABLE timedoctor.timeuse_daily
(
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    date DATE,
    process VARCHAR(255),
    domian VARCHAR(255),
    time MEDIUMINT
);
CREATE INDEX timeuse_daily_date_index ON timedoctor.timeuse_daily (date);




SELECT c.user_id, GROUP_CONCAT(DATEDIFF(f.max_active_date, c.`date`), ":", c.`time` ORDER BY c.`date` DESC SEPARATOR ','), f.total_days, f.hours_per_day, f.`reason`
FROM cache_day_work_time c, (
                              SELECT
                                t.user_id,
                                COUNT(*)                                  AS `total_days`,
                                sum(t.time)                               AS `total_time`,
                                (sum(t.time) / 3600) / COUNT(*)           AS `hours_per_day`,
                                DATE(l.leave_date),
                                l.reason,
                                MAX(t.date) as `max_active_date`,
                                DATEDIFF(DATE(l.leave_date), MAX(t.date)) AS `inactive_days`
                              FROM timedoctor.cache_day_work_time t
                                JOIN timedoctor.users_leave_log l
                                  ON (t.user_id = l.user_id AND l.reason IN ('They Quit', 'They were fired'))
                              WHERE t.date < l.leave_date
                              GROUP BY t.user_id
                              HAVING total_days > 20 AND inactive_days < 30 AND hours_per_day > 4) AS f
WHERE c.user_id = f.user_id
GROUP BY c.user_id
