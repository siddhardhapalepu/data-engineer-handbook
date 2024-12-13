insert into host_activity_reduced 
-- Aggregating daily metrics
with daily_aggregate as (
select host,
	date(event_time::timestamp) as current_date,
	count(host) as daily_hits,
	count(distinct user_id) as daily_unique_visitors
from events e 
where date(event_time::timestamp) = date('2023-01-03')
group by 1,2),

-- collecting yesterdays results
yesterday_array as (
select *
from host_activity_reduced har 
where month = date('2023-01-01'))


select
	coalesce (ya.month, date_trunc('month', da.current_date)) as month,
	coalesce (da.host, ya.host) as host,
	case 
		when ya.hit_array is not null then
			ya.hit_array || array[coalesce(da.daily_hits, 0)]
		when ya.hit_array is null then
			array_fill(0,array[coalesce(da.current_date - date(date_trunc('month', da.current_date)),0)])
			|| array[coalesce(da.daily_hits,0)]
			end as hit_array,
	case 
		when ya.unique_visitors_array is not null then
			ya.unique_visitors_array || array[coalesce(da.daily_unique_visitors, 0)]
		when ya.hit_array is null then
			array_fill(0,array[coalesce(da.current_date - date(date_trunc('month', da.current_date)),0)])
			|| array[coalesce(da.daily_unique_visitors,0)]
			end as unique_visitors_array
from daily_aggregate da full outer join yesterday_array ya
on da.host = ya.host
-- mentioning what to do when primary key conflict happens
on conflict (host,month)
do
	update set hit_array = excluded.hit_array,
	 unique_visitors_array = excluded.unique_visitors_array;