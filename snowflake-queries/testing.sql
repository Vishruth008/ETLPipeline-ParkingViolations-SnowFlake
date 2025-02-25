
SELECT * FROM PARKING_VIOLATIONS_2014
UNION
SELECT * FROM PARKING_VIOLATIONS_2015;


SELECT TOP 10 VIOLATION_TIME  FROM PARKING_VIOLATIONS_2014;

SELECT VIOLATION_TIME, COUNT(*) FROM PARKING_VIOLATIONS_2014 GROUP BY VIOLATION_TIME ORDER BY COUNT(*) DESC;

SHOW COLUMNS IN PARKING_VIOLATIONS_2016;


SELECT ISSUE_DATE FROM PARKING_VIOLATIONS_2014 ORDER BY ISSUE_DATE DESC;

