create function get_sum(val1 integer, val2 integer)
returns integer as $sum$
begin
	return val1 + val2;
end;
$sum$ language plpgsql;

select get_sum(5,5);

create or replace function get_customers(cid integer)
returns text as $customer$
declare
cname text;
begin
	select first_name ||' ' ||last_name into cname from customers where id=cid;
	return cname;
end;
$customer$ language plpgsql;

select get_customers(2);

*********************
create or replace function get_top_customers_json()
returns json as $cid$
declare 
	customer_name text;
begin
	select first_name from (
		select count(c.id) as purchases,c.id, first_name from customers c
		inner join purchases p on
		c.id = p.customer_id
		group by c.id
		order by purchases desc 
		limit 1
	) top_customer into customer_name;
	return json_build_object(
		'status','success',
		'result',concat(customer_name)
	);
end
$cid$ language plpgsql 

select get_top_customers();
select get_top_customers_json();

**************************

create or replace function add_customers(f_name text, l_name text)
returns json
language plpgsql
as $body$
declare	
	fname_len integer;
	lname_len integer;
begin
	select char_length(f_name) into fname_len;
	select char_length(l_name) into lname_len;

--	validation 
	if (fname_len <=0 or f_name is null) then
		return json_build_object(
			'status','Fail',
			'details', 'First name is null or empty'
			);
	end if;

	if (lname_len <= 0 or l_name is null) then
		return json_build_object(
			'status','Fail',
			'details','Last name is null or empty'
		);
	end if;

--insert to customers #id column should be serial
	insert into customers(first_name, last_name, id)
	values(f_name, l_name, 88);
	return json_build_object(
		'status','Success',
		'data', json_build_object(
			'first_name',f_name,
			'last_name', l_name
			)
	);
end;
$body$;


select add_customers('Bishal', 'Udash');





