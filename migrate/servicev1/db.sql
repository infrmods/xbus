alter table services add column service varchar(256) not null after status;
alter table services add column zone varchar(16) not null default "default" after service;
alter table services drop index nv_dup;
update services set service=concat(name, ':', version);
alter table services add unique index service_dup (service, zone);
alter table services drop column name;
alter table services drop column version;