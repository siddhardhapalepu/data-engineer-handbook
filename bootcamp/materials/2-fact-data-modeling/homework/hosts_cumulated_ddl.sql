create table hosts_cumulated (
	host text,
	host_activity_datelist date[],
	date date,
	primary key (host, date)
)