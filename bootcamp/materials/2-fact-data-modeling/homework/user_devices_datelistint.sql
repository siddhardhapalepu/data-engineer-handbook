-- generating a series for the month and identifying which user is active on which day
-- and also calculating last active day
with starter as (
select udc.device_activity_datelist @> array [DATE(d.valid_date)] as is_active,
extract(day from date('2023-01-31') - d.valid_date) as days_since,
udc.user_id,
udc.browser_type
from user_devices_cumulated udc
	cross join
(select generate_series('2023-01-01', '2023-01-31', interval '1 day') as valid_date) as d
where date = date('2023-01-31')),

-- converting the date array into 32 bit binary in order to see the pattern when the user was active
bits as (
select user_id,
	browser_type,
	sum(case when is_active then pow(2, 32 - days_since)
	else 0 end)::bigint::bit(32) as datelist_int,
	date('2023-01-31') as date
	from starter
	group by user_id, browser_type
)

insert into user_devices_datelist_int
select * from bits
