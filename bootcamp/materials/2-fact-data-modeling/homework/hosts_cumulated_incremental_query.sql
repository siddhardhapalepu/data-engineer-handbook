insert into hosts_cumulated 
-- bringing yesterday's data
with yesterday as (
select host,
	host_activity_datelist,
	date
from hosts_cumulated hc 
where date = date('2023-01-02')),

-- bringing today's data
today as (
select
	host,
	date(event_time :: timestamp) as date_active
from events e 
where date(event_time :: timestamp) = date('2023-01-03')
group by 1,2)


select 
	coalesce (t.host, y.host) as host,
	case when y.date is null then ARRAY[t.date_active]
	when t.date_active is null then y.host_activity_datelist
	else ARRAY[t.date_active] || y.host_activity_datelist
	end as host_activity_datelist,
	coalesce (t.date_active, y.date + interval '1 day') as date
from today t full outer join yesterday y
on t.host = y.host


