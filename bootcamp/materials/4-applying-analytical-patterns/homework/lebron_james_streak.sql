-- First deduping game_details
WITH game_details_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, team_id, player_id
            ORDER BY game_id  -- Or some other column for stable ordering
        ) AS rnum
    FROM game_details
),

-- then creating a pattern where we identify streak change and also join with games to bring in date for ordering
streak_pattern AS (
    SELECT
		g.game_date_est,
        gd.game_id,
        gd.player_id,
        gd.player_name,
        COALESCE(gd.pts, 0) AS pts,
        CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END AS streak_changed,
		row_number() over (partition by player_id order by g.game_date_est) as rnum
    FROM game_details_deduped gd join games g
	on gd.game_id = g.game_id
    WHERE gd.rnum = 1
      AND gd.player_name = 'LeBron James'
),

-- here we do a staging - windowing to identify continuous pattern for streak
streak_staging as (
select *,
sum(streak_changed) over (partition by player_id order by game_date_est) as streak_identifier
from streak_pattern),

-- here we are creating a pattern to identify continuous streaks and only looking for values when streak is changed
grouped_streak as (
select *,
rnum - streak_identifier as diff
from streak_staging
where streak_changed = 1)


select player_name, diff, count(*) as game_streak
from grouped_streak
group by 1,2
order by 3 desc
limit 1