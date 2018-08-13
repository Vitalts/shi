create temporary table values(id integer, name varchar, measure_id integer, measure varchar, value numeric);

insert into values(name, measure, value)
%DATA%
;

insert into measurements.device(name)
  select distinct values.name
  from values
    left join measurements.device d on d.name = values.name
  where d.id is null;

update values
set id = (select d.id from measurements.device d where d.name = values.name);

insert into measurements.measurement(device_id)
  select distinct id from values;

update values
set measure_id = (select m.id from measurements.measurement m where m.device_id = values.id);

insert into measurements.data(id, measure, value)
  select measure_id, measure, value
  from values;
