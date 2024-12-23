-- This is a cumulative query to insert into user_devices_cumulated table
insert into user_devices_cumulated 
-- yesterday's data
with yesterday as (
select * from user_devices_cumulated e 
where date = date('2023-01-09')),

-- today's data
today as (
select 
	user_id,
	browser_type,
	date(event_time :: timestamp) as date_active
from events e join devices d
on e.device_id = d.device_id
where date(event_time :: timestamp) = date('2023-01-10')
and user_id is not null
group by 1,2,3)

-- removing duplicates after cumulation
select distinct
	coalesce (t.user_id, y.user_id) as user_id,
	coalesce (t.browser_type, y.browser_type) as browser_type,
	case when y.device_activity_datelist is null then ARRAY[t.date_active]
	when t.date_active is null then y.device_activity_datelist
	else ARRAY[t.date_active] || y.device_activity_datelist
	end as device_activity_datelist,
	coalesce (t.date_active, y.date + interval '1 day') as date
from today t full outer join yesterday y 
on t.user_id = y.user_id