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

