--creating view
--View acts as a camera point view to the database
**************
create view get_revenue_per_customer as
select c.id, c.first_name, c.last_name, sum(i.price) as "Money Spent" from customers c
inner join purchases p on c.id = p.customer_id
inner join items i on p.item_id = i.id
group by c.id
order by id;

select * from get_revenue_per_customer;

drop view get_revenue_per_customer;
