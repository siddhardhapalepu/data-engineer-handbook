insert into player_state_tracker
with yesterday as (
select * from player_state_tracker
where current_season = 2002),

today as (
select p.player_name,
		p.is_active,
		p.current_season
	from players p 
	where p.current_season = 2003)

select coalesce(y.player_name, t.player_name) as player_name,
	coalesce(y.first_active_season, t.current_season) as first_active_season,
	case when t.is_active is true then t.current_season
	else y.last_active_season end as last_active_season,
	case when coalesce(y.first_active_season, t.current_season) = t.current_season then 'New'
	when y.player_state in ('New', 'Continued Playing', 'Returned from Retirement') and t.is_active = True then 'Continued Playing'
	when y.player_state in ('New', 'Continued Playing', 'Returned from Retirement') and t.is_active = False then 'Retired'
	when y.player_state in ('Retired','Stayed Retired') and t.is_active = True then 'Returned from Retirement'
	when y.player_state in ('Retired','Stayed Retired') and t.is_active = False then 'Stayed Retired'
	end as player_state,
	coalesce(y.seasons_active, ARRAY[]::INTEGER[])|| case when t.is_active = true then ARRAY[t.current_season]::INTEGER[] else ARRAY[]::INTEGER[]
	end as seasons_active,
	t.current_season as current_season
	from today t full outer join yesterday y on t.player_name = y.player_name