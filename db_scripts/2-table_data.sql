CREATE TABLE IF NOT EXISTS tvshows.data(
    id serial4 NOT NULL,
    show_name varchar(300),
    show_url varchar(300),
    show_series_name varchar(300),
    season integer,
    airdate date
);