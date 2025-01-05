CREATE TABLE player_state_tracker (
	player_name text,
	first_active_season integer,
	last_active_season integer,
	player_state text,
	seasons_active INTEGER[],
	current_season integer,
	PRIMARY KEY(player_name, current_season)
)