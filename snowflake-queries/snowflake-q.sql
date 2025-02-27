DESC INTEGRATION PARKING_INTEGRATION;


CREATE STAGE IF NOT EXISTS PARKING_STAGE
STORAGE_INTEGRATION = PARKING_INTEGRATION
URL = 's3://parking-violations-bucket/';

DESC STAGE PARKING_STAGE;


SELECT TOP 9999 * FROM PARKING_VIOLATIONS;

SELECT * FROM PARKING_VIOLATIONS_CODE;

SELECT * FROM GOLD_TICKET_METRICS;

SELECT * FROM GOLD_VEHICLES_METRICS;

SELECT VEHICLE_YEAR, COUNT(VEHICLE_YEAR) as my_count
FROM PARKING_VIOLATIONS_2014
GROUP BY VEHICLE_YEAR;

SELECT violation_precinct, COUNT(violation_precinct) as my_count
FROM PARKING_VIOLATIONS_2014
GROUP BY violation_precinct 
ORDER BY my_count DESC;


SELECT DISTINCT(VIOLATION_COUNTY)
FROM GOLD_YEARLY_VIOLATIONS_BY_COUNTY;



