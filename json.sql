JSON
********
create table orders(
	id serial not null,
	info jsonb not null
);

insert into orders (info) 
values 
	('{ "customer": "John Doe", "items": {"product": "Beer","qty": 6}}'),
	('{ "customer": "Josh William", "items": {"product": "Toy Car","qty": 1}}'),
	('{ "customer": "Mary Clark", "items": {"product": "Toy Train","qty": 2}}');
	
select info->'customer' as Customers from orders;
select info->'items'->>'product' as Items from orders;
select 
	info->>'customer' as Customer,
	info->'items'->>'product' as Product,
	info->'items'->>'qty' as Quantity from orders
where cast
	(info->'items'->>'qty' as Integer ) > 1;

//Table
select *
from (
VALUES(1,2,3),(4,5,6),(7,8,9)
) as x(id, namt, rollno)
