create table user_devices_cumulated (
	user_id numeric,
	browser_type text,
	device_activity_datelist date[],
	date DATE,
	primary key (user_id, browser_type, date)
	);