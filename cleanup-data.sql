-- remove duplicated leave records
DELETE users_leave_log
FROM users_leave_log
  INNER JOIN (SELECT
                user_id,
                MAX(id) AS `max_id`
              FROM users_leave_log
              WHERE reason IN ('They Quit', 'They were fired')
              GROUP BY user_id
              HAVING COUNT(id) > 1) dup ON (users_leave_log.user_id = dup.user_id AND users_leave_log.id != max_id)
WHERE reason IN ('They Quit', 'They were fired')

-- empty records from the cache
DELETE FROM cache_day_work_time WHERE time = 0;
-- empty old records from the cache
DELETE FROM `cache_day_work_time` WHERE `date` < DATE('2016-01-01');
-- empty buggly records from the cache
DELETE FROM `cache_day_work_time` WHERE `date` > NOW()

-- remove empty records
DELETE FROM timeuse_daily WHERE coalesce(app, website) = '';
--[2017-03-14 09:44:43] 957250 rows affected in 2m 48s 531ms

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

USE `timedoctor`;
SET GLOBAL group_concat_max_len = 100000000000;
SET GLOBAL max_allowed_packet = 1677721600;

SHOW PROCESSLIST;

SELECT
  l.user_id,
  l.reason,
  l.leave_date,
  SUM(c.time) / 3600 AS `total_hours_worklog`,
  COUNT(*)           AS total_days_worklog,
  (SELECT COUNT(DISTINCT `date`) FROM timeuse_daily t WHERE l.user_id = t.user_id) as total_days_timeuse,
  GROUP_CONCAT(DATEDIFF(l.leave_date, c.`date`), ':', ROUND(c.time / 3600, 2) ORDER BY c.`date` DESC separator ',') as working_time_per_day,
  (
    SELECT GROUP_CONCAT(date, ':', '{', activity, '}' ORDER BY `date` DESC SEPARATOR ',')
    FROM (
           SELECT user_id, date, GROUP_CONCAT(COALESCE(t.app, t.website), ':', t.time SEPARATOR ',') as activity
           FROM timeuse_daily t
           WHERE user_id = 376006
           GROUP BY date) temp GROUP BY user_id
    ) as activity_per_day

FROM users_leave_log l
  JOIN cache_day_work_time c ON (l.user_id = c.user_id)
WHERE reason IN ('They Quit', 'They were fired')
GROUP BY l.user_id
HAVING total_days_worklog > 330
ORDER BY `total_days_worklog` DESC;




SHOW VARIABLES LIKE 'max_allowed%';



# SELECT c.user_id, SUM(c.time) / 3600 as `total_hours`, COUNT(*) as total_days
# FROM cache_day_work_time c
# WHERE 1
# GROUP BY c.user_id
# HAVING total_days > 60
# ORDER BY `total_days` DESC