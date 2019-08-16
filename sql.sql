--select c.id, c.first_name as "First name", c.last_name from customers c;

--select c.id, c.first_name as "First name", c.last_name from customers c
--where first_name = 'Rolf';

select * from  customers where last_name like '%t%';

update items set price = 3.50 where id=3;

delete from items where id=4;

--customers and purchases join
select * from customers c
inner join purchases p
on c.id = p.customer_id

--items and purchases join
select * from items i
inner join purchases p on i.id = p.item_id
inner join customers c on p.customer_id = c.id

--Group by
select c.id, c.first_name, c.last_name, count(p.id) from customers c inner join purchases p
on c.id = p.customer_id 
group by c.id;

select c.id, c.first_name, c.last_name, count(p.id) as "Items Bought",  sum(i.price) as "Total"  from items i  
inner join purchases p on i.id = p.item_id
inner join customers c on p.customer_id = c.id
group by c.id;

--Assignment
select * from purchases p
inner join items i on p.item_id = i.id; 

select i.id, i.name, i.price, count(p.id), sum(i.price) from purchases p
inner join items i on p.item_id = i.id
group by i.id
order by sum desc;   

drop table student;

create table public.student(
	id serial not null,
	name varchar(191) not null,
	address varchar(191) not null,
	grade integer null,
	
	constraint student_pkey primary key(id) 
);


create table public.book(
	id serial not null,
	book_name varchar(191) not null,
	
	constraint book_pkey primary key (id)
);

create table public.student_book(
	id serial not null,
	student_id integer not null,
	book_id integer not null,
	
	constraint student_book_pkey primary key (id),
	constraint fk_student_id foreign key (student_id) references student(id),
	constraint fk_book_id foreign key (book_id) references book(id)
);

--DROP TABLE IF EXISTS student,book,student_book;

insert into student(name, address, grade) values 
('Sugat', 'Patan', 7), 
('Asim', 'Jawalakhel', 10), 
('Ram', 'Kupondol', 7),
('Sandeep', 'Patan', 7),
('Bishal', 'Ekantakuna', 6);

insert into public.book(book_name) values
('Adam and Eve'),
('Harry Potter'),
('IT'),
('5 point someone')

insert into public.student_book(student_id, book_id) values
(6,4),(5,2),(1,3),(2,2),(3,1),(3,2),(4,1);

select s."name", s.grade, s.address, count(sb.book_id) as "books_issued" from student s 
inner join student_book sb on s.id = sb.student_id
inner join book b on sb.book_id = b.id
group by s.id;
