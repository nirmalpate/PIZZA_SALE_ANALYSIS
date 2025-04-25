# PIZZA_SALE_ANALYSIS

I HAVE ANALYSED THE PIZZA SALES. THERE ARE FOUR TABLES AND QUERIES ARE BASED ON BASIC INTERMEDIATE AND ADVANCED.
i HAVE ADDRESSED VARIOUS BUSINESS QUESTIONS.

FIRST I HAVE EXTRACTED THE DATA FROM DIFFERENT SOURCES. THEN I HAVE UPLOADED THE DATA IN PYTHON. THEN USING SQLALCHEMY, I HAE UPLOADED TO MYSQL. 
THEN I HAVE ADDRESSED THE BUSINESS PROBLEM.
PYTHON CODE:

import pandas as pd
import numpy as np

df = pd.read_csv("C:/Users/ADM/Desktop/pizza_sales/order_details.csv")

df.info()
df1 = pd.read_csv("C:/Users/ADM/Desktop/pizza_sales/orders.csv", encoding='latin1')
df2 = pd.read_csv("C:/Users/ADM/Desktop/pizza_sales/pizza_types.csv", encoding='latin1')
df3 = pd.read_csv("C:/Users/ADM/Desktop/pizza_sales/pizzas.csv", encoding='latin1')

df

df3

from sqlalchemy import create_engine
db_user = 'root'
db_password = '0000'
db_host = 'localhost'
db_port = '3306'
db_name = 'pop'

connection_string = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

engine = create_engine(connection_string)

engine

df.to_sql('order_details',engine, if_exists = 'replace', index = False)

df1.to_sql('orders',engine, if_exists = 'replace', index = False)

df2.to_sql('pizza_types',engine, if_exists = 'replace', index = False)

df3.to_sql('pizzas',engine, if_exists = 'replace', index = False)

# SQL

#Retrieve the total number of orders placed.
# SQL ANALYSIS
select  distinct count(*) as Total_Orders
from pop.orders ;

#Calculate the total revenue generated from pizza sales.

SELECT * 
FROM pop.pizzas;

SELECT * FROM pop.order_details;


With Revenue as(
select  o1.order_id, o1.pizza_id,o1.quantity, o2.price as Sin_Price, o1.quantity * o2.price as Price
from order_details o1
inner join pizzas o2
on o1.pizza_id = o2.pizza_id)
select  round(sum(Price),2) as Total_Revenue
from Revenue;

#Identify the highest-priced pizza.

select  pizza_id, max(price) as Highest_Priced_Pizza
from pizzas
group by pizza_id
order by max(price) desc
limit 1;

#Identify the most common pizza size ordered.

select  size,count(size) as Total_size
from pizzas
group by size
limit 1;

#List the top 5 most ordered pizza types along with their quantities.

With cte as(
select  o1.quantity, o2.pizza_type_id
from order_details o1
join pizzas o2
on o1.pizza_id = o2.pizza_id)
select  pizza_type_id, sum(quantity) as Total_Quantity
from cte
group by pizza_type_id
order by sum(quantity)  desc
limit 1;

# Intermediate:

# Join the necessary tables to find the total quantity of each pizza category ordered.

With cte as(select  p1.pizza_type_id,p1.pizza_id, p2.category
from pizzas p1
inner join pizza_types p2
on p1.pizza_type_id = p2.pizza_type_id
)
select   c1.category, sum(c2.quantity) as Total_Quantity
from cte c1
inner join pop.order_details c2
on c1.pizza_id = c2.pizza_id
group by c1.category;

#Determine the distribution of orders by hour of the day.

SELECT  hour(time) as hrs,count(order_id) as Orders
FROM pop.orders
group by  hrs 
order by hrs asc;
 
# Group the orders by date and calculate the average number of pizzas ordered per day.

With cte as(
SELECT  order_id,day(Date) as Day_
FROM pop.orders)
select  Day_, count(order_id) as Total_Orders
from cte
group by Day_;

#Determine the top 3 most ordered pizza types based on revenue.

With cte as(
SELECT  p1.pizza_id,p1.quantity,p2.price,p2.pizza_type_id
FROM pop.order_details p1
join pop.pizzas p2
on p1.pizza_id = p2.pizza_id), Revenue as(
select  c1.quantity, c2.pizza_type_id, c2.price
from cte c1
inner join pop.pizzas c2
on c1.pizza_id = c2.pizza_id), Net_Revenue as(
select  pizza_type_id, price * quantity as Net_Price
from Revenue)
select  pizza_type_id, round(sum(Net_Price),0) as Total_Revenue
from Net_Revenue
group by pizza_type_id
order by round(sum(Net_Price),0) desc
limit 3;

# Advanced:
# Calculate the percentage contribution of each pizza type to total revenue.

With cte as(
SELECT  p1.pizza_id,p1.quantity,p2.price,p2.pizza_type_id
FROM pop.order_details p1
join pop.pizzas p2
on p1.pizza_id = p2.pizza_id), Revenue as(
select  c1.quantity, c2.pizza_type_id, c2.price
from cte c1
inner join pop.pizzas c2
on c1.pizza_id = c2.pizza_id), Net_Revenue as(
select  pizza_type_id, price * quantity as Net_Price
from Revenue), Growth as(
select  pizza_type_id, round(sum(Net_Price),0) as Total_Revenue
from Net_Revenue
group by pizza_type_id
order by round(sum(Net_Price),0) desc), gross as(
select *, sum(Total_Revenue) over() as Rev
from Growth)
select  pizza_type_id, Total_Revenue, round((Total_Revenue*100)/Rev,2) as Percnt
from Gross;


#Analyze the cumulative revenue generated over time.

with cte as(
SELECT  p1.order_id, p1.pizza_id,p1.quantity * p2.price as Revenue
FROM pop.order_details p1
inner join pop.pizzas p2
on p1.pizza_id = p2.pizza_id), final as(
select  order_id, round(sum(revenue) ,2)as Total
from cte
group by order_id), DC as(
select  f1.order_id, f1.Total, f2.date as Date_
from final f1
inner join pop.orders f2
on f1.order_id = f2.order_id)
select  order_id, Total ,month(Date_) as month_, sum(Total) over(partition by month(Date_) order by month(Date_)) as rnk
from DC;

#Determine the top 3 most ordered pizza types based on revenue for each pizza category.

with ct as(
SELECT  d1.order_id, d1.quantity * d2.price as Rev, d1.pizza_id
FROM pop.order_details d1
inner join pop.pizzas d2
on d1.pizza_id = d2.pizza_id), Final as(
select  t1.order_id, t1.Rev, t2.pizza_type_id, t3.category
from ct t1
inner join pop.pizzas t2
on t1.pizza_id  = t2.pizza_id
inner join pop.pizza_types t3
on t2.pizza_type_id = t3.pizza_type_id), Paul as(
select   pizza_type_id, category, round(sum(rev),2) as Total
from Final 
group by pizza_type_id, category), rnk as(
select *, sum(Total) over(partition by category) as sum
from paul), net as(
select *, row_number() over(partition by category order by total desc) as sn
from rnk)
select  pizza_type_id, category
from net
where sn = 1;
