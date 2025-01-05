-- DDL to create a game_stats using grouping sets
create table game_stats as 
-- deduplicating game_details data
with game_details_deduped_staging as (
select *,
row_number() over (partition by game_id, team_id, player_id) as rnum
from game_details),

-- flattening games table to have single team_id column
games_flattened as (
select game_id, home_team_id as team_id, season,
case when home_team_wins = 1 then 1 else 0 end as win
from games
union all
select game_id, visitor_team_id as team_id, season,
case when home_team_wins = 1 then 0 else 1 end as win
from games
),

-- calculating team_wins separately since the grain of final_game_details is players 
-- so, when we calculate team wins it doesnt take distinct. Basically pre-aggregating
team_wins AS (
    SELECT team_id, 
           COUNT(DISTINCT CASE WHEN win = 1 THEN game_id END) AS team_total_wins
    FROM games_flattened
    GROUP BY team_id),

-- Finally bringing games and game_details tables together
final_game_details as (
select gd.game_id,
	g.season,
	gd.player_id,
	gd.player_name,
	g.team_id,
	gd.team_abbreviation,
	g.win,
	coalesce(gd.pts, 0) as pts,
	tw.team_total_wins
	from game_details_deduped_staging gd join games_flattened g
	on gd.game_id = g.game_id
	and gd.team_id = g.team_id 
	join team_wins tw on gd.team_id = tw.team_id
	where rnum = 1)
	
-- calculating metrics based on different grouping sets
select
	case when grouping(game_id) = 0
		and grouping(season) = 0
		and grouping(player_id) = 0
		and grouping(player_name) = 0
		and grouping(team_id) = 0
		and grouping(team_abbreviation) = 0
		then 'all_grouping'
	when grouping(player_id) = 0
		and grouping(player_name) = 0
		and grouping(team_id) = 0
		and grouping(team_abbreviation) = 0
		then 'player_team'
	when grouping(player_id) = 0
		and grouping(player_name) = 0
		and grouping(season) = 0
		then 'player_season'
	when grouping(team_id) = 0
		and grouping(team_abbreviation) = 0
		then 'team'
	end as aggregation_level,
	coalesce(cast(game_id as varchar), '(overall)') as game_id,
	coalesce(cast(season as varchar), '(overall)') as season,
	coalesce(cast(player_id as varchar), '(overall)') as player_id,
	coalesce(cast(player_name as varchar), '(overall)') as player_name,
	coalesce(cast(team_id as varchar), '(overall)') as team_id,
	coalesce(cast(team_abbreviation as varchar), '(overall)') as team_abbreviation,
	sum(pts) as pts,
	max(team_total_wins) AS wins
from final_game_details
group by grouping sets(
	(game_id, season, player_id, player_name, team_id, team_abbreviation),
	(player_id, player_name, team_id, team_abbreviation),
	(player_id, player_name, season),
	(team_id, team_abbreviation)
)

-- who scored the most points playing for one team?
-- Giannis Antetokounmpo
select player_id, player_name,team_id, team_abbreviation, pts
from game_stats
where aggregation_level = 'player_team'
order by pts desc
limit 1

-- who scored the most points in one season
-- James Harden
select player_id, player_name, season, pts
from game_stats
where aggregation_level = 'player_season'
order by pts desc
limit 1

-- which team has won the most games?
-- GSW
select team_id, team_abbreviation, wins
from game_stats
where aggregation_level = 'team'
order by wins desc
limit 1



