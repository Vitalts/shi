create schema if not exists measurements;

create table if not exists measurements.device(
id serial primary key,
name text not null,
unique (name),
unique (name, id)
);

create table if not exists measurements.measurement(
id serial primary key,
  device_id integer references measurements.device(id),
  tstamp timestamp with time zone default now(),
unique (device_id, id, tstamp)
);

create table if not exists measurements.data(
id integer references measurements.measurement(id),
measure text not null,
value numeric,
unique (measure, id, value)
);
