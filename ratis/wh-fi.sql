create or replace function wh_fi(w_id int) returns integer as $$
select ((w_id)%8)
$$ language sql immutable;

drop index wh_lookup;
create index wh_lookup on  wh  (  wh_fi(w_id));
