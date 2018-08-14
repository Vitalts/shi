select name, tstamp %MEASUREMENTS_SELECT%
from measurements.measurement m
join measurements.device on device.id = m.device_id
%MEASUREMENTS_JOIN%
where 1=1
%DEVICES%
%DATE_FROM%
%DATE_TO%
