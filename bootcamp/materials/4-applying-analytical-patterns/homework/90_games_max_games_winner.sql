-- Flattening games to bring home and visitors in a single row
with games_flattened as (
select game_id, home_team_id as team_id, game_date_est,
case when home_team_wins = 1 then 1 else 0 end as win
from games
union all
select game_id, visitor_team_id as team_id, game_date_est,
case when home_team_wins = 1 then 0 else 1 end as win
from games),

-- calculating sum of 90 days window
game_streak as (
SELECT 
        team_id,
        game_date_est,
        SUM(win) OVER (
            PARTITION BY team_id 
            ORDER BY game_date_est 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_90_days,
		row_number() over (partition by team_id order by game_date_est) as rnum
    FROM games_flattened),

-- within the windows, selecting max of each window
max_wins as (
select team_id, max(wins_90_days) as total_wins
from game_streak
group by team_id
having max(rnum) >= 90)

-- outputting final row
select team_id, total_wins
from max_wins
order by 
total_wins desc
limit 1