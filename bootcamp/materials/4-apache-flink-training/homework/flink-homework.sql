CREATE TABLE sessionized_events (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    num_hits BIGINT
);


-- Q1: Calculate the average number of web events per session for users on Tech Creator
-- The below query takes avg of num_hits per host and gives average events per host per session
select host, round(avg(num_hits),0) as avg_events_per_session
from sessionized_events
where host like '%techcreator.io'
group by host

-- Q2: Compare the average number of web events per session across specific hosts
-- The query focuses on sessions from three specific hosts and calculates the average number of events per session.
-- Results are ordered in descending order of average events per session)
SELECT
    host,
    ROUND(AVG(num_hits),1) AS avg_events_per_session
FROM sessionized_events
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;