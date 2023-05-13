-- STUDENT number. UPDATE ONLY, DO NOT DELETE. 
select "1005288";
-- Replace the above with your student number

-- DATA LOADING Seperator. DO NOT DELETE
select "DATA LOADING";
-- Put your table creation and data loading SQL statements here

create table if not exists flights
(
    fid integer primary key, year_of_flight integer, 
    month_id integer, day_of_month integer, day_of_week_id integer, 
    carrier_id varchar(100), flight_num integer, 
    origin_city varchar(100), origin_state varchar(100), dest_city varchar(100), 
    dest_state varchar(100), departure_delay integer, taxi_out integer, arrival_delay integer, 
    cancelled integer, actual_time integer, distance integer
);

load data infile "/Users/limboonhanmelvin/Downloads/Database/Assignment 1/flights/flights-small.csv"
into table flights
fields terminated by ',' Enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

create table if not exists carriers
(
    cid varchar(100) primary key,
    carrier_name varchar(100)
);

load data infile "/Users/limboonhanmelvin/Downloads/Database/Assignment 1/flights/carriers.csv" 
into table carriers
fields terminated by ',' Enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

create table if not exists months
(
    mid integer primary key,
    m_name varchar(100)
);

load data infile "/Users/limboonhanmelvin/Downloads/Database/Assignment 1/flights/months.csv" into table months
fields terminated by ',' Enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

create table if not exists weekdays
(
    did integer,
    d_name varchar(100)
);

load data infile "/Users/limboonhanmelvin/Downloads/Database/Assignment 1/flights/weekdays.csv" 
into table weekdays
fields terminated by ',' Enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


-- QUESTION 1 Seperator. DO NOT DELETE
select "QUESTION 1";
-- Put your Q1 SQL statements here
SELECT flight_num as "Flight Number",origin_city,dest_city,carrier_id, day_of_month 
FROM flights

WHERE origin_city = 'Seattle WA' AND dest_city = 'Boston WA' AND carrier_id = 'AS' AND day_of_month = 1;

-- QUESTION 2 Seperator. DO NOT DELETE
select "QUESTION 2";
-- Put your Q2 SQL statements here
SELECT avg(f.arrival_delay) as "Average Delay", f.day_of_week_id, d.did, d.d_name as "Day"
FROM flights f, weekdays d
WHERE f.day_of_week_id = d.did
order by avg(f.arrival_delay) DESC LIMIT 1 OFFSET 2

-- QUESTION 3 Seperator. DO NOT DELETE
select "QUESTION 3"; 
-- Put your Q3 SQL statements here
-- SELECT count(distinct ) as "Total Flights", 

-- QUESTION 4 Seperator. DO NOT DELETE
select "QUESTION 4";
-- Put your Q4 SQL statements here
-- do self join, find carrier same, sum together
SELECT sum(p.departure_delay) as "Total Delay",p.carrier_id,f.carrier_id,
c.carrier_id, c.carrier_name as "Airline"
FROM flights p, flights f, carriers c

WHERE p.carrier_id = f.carrier_id AND p.carrier_id = c.carrier_id;

-- QUESTION 5 Seperator. DO NOT DELETE
select "QUESTION 5";
-- Put your Q5 SQL statements here
-- find total number of flights from same carrier
SELECT p.origin_city, p.cancelled, count(distinct p.carrier_id), p.carrier_id,
c.carrier_id, c.carrier_name as "Airline", percentage_of_flights as "Percentage"
FROM flights p, carriers c

WHERE 
p.origin_city = "New York" AND
percentage_of_flights = (p.cancelled / count(distinct p.carrier_id)) AND
p.carrier_id = c.carrier_id
order by percentage_of_flights ASC;