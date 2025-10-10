-- Task: Create a list of all the different (distinct) replacement costs of the films.
SELECT DISTINCT replacement_cost 
FROM film
ORDER BY 1
-- Task: Write a query that gives an overview of how many films have replacements costs in the following cost ranges
SELECT 
title,
name,
length
FROM film f
LEFT JOIN film_category fc
ON f.film_id=fc.film_id
LEFT JOIN category c
ON c.category_id=fc.category_id
WHERE name = 'Sports' OR name = 'Drama'
ORDER BY length DESC

-- Task: Create a list of the film titles including their title, length, and category name ordered descendingly by length. Filter the results to only the movies in the category 'Drama' or 'Sports'.
SELECT
name,
COUNT(title)
FROM film f
INNER JOIN film_category fc
ON f.film_id=fc.film_id
INNER JOIN category c
ON c.category_id=fc.category_id
GROUP BY name
ORDER BY 2 DESC

-- Task: Create an overview of how many movies (titles) there are in each category (name).
SELECT
name,
COUNT(title)
FROM film f
INNER JOIN film_category fc
ON f.film_id=fc.film_id
INNER JOIN category c
ON c.category_id=fc.category_id
GROUP BY name
ORDER BY 2 DESC

-- Task: Create an overview of the actors' first and last names and in how many movies they appear in.
SELECT
a.actor_id,
first_name,
last_name,
COUNT(*)
FROM actor a
LEFT JOIN film_actor fa
ON fa.actor_id=a.actor_id
LEFT JOIN film f
ON fa.film_id=f.film_id
GROUP BY a.actor_id,first_name, last_name
ORDER BY COUNT(*) DESC

-- Task: Create an overview of the addresses that are not associated to any customer.
SELECT * FROM address a
LEFT JOIN customer c
ON c.address_id = a.address_id
WHERE c.first_name is null

-- Task: Create the overview of the sales  to determine the from which city (we are interested in the city in which the customer lives, not where the store is) most sales occur.
SELECT 
city,
SUM(amount)
FROM payment p
LEFT JOIN customer c
ON p.customer_id=c.customer_id
LEFT JOIN address a
ON a.address_id=c.address_id
LEFT JOIN city ci
ON ci.city_id=a.city_id
GROUP BY city
ORDER BY city DESC

-- Task: Create an overview of the revenue (sum of amount) grouped by a column in the format "country, city".
SELECT 
country ||', ' ||city,
SUM(amount)
FROM payment p
LEFT JOIN customer c
ON p.customer_id=c.customer_id
LEFT JOIN address a
ON a.address_id=c.address_id
LEFT JOIN city ci
ON ci.city_id=a.city_id
LEFT JOIN country co
ON co.country_id=ci.country_id
GROUP BY country ||', ' ||city
ORDER BY 2 ASC

-- Task: Create a list with the average of the sales amount each staff_id has per customer.
SELECT 
staff_id,
ROUND(AVG(total),2) as avg_amount 
FROM (
SELECT SUM(amount) as total,customer_id,staff_id
FROM payment
GROUP BY customer_id, staff_id) a
GROUP BY staff_id

-- Task: Create a query that shows average daily revenue of all Sundays.
SELECT 
AVG(total)
FROM 
	(SELECT
	 SUM(amount) as total,
	 DATE(payment_date),
	 EXTRACT(dow from payment_date) as weekday
	 FROM payment
	 WHERE EXTRACT(dow from payment_date)=0
	 GROUP BY DATE(payment_date),weekday) daily


-- Task: Create a list of movies - with their length and their replacement cost - that are longer than the average length in each replacement cost group.
SELECT 
title,
length
FROM film f1
WHERE length > (SELECT AVG(length) FROM film f2
			   WHERE f1.replacement_cost=f2.replacement_cost)
ORDER BY length ASC

-- Task: Create a list that shows the "average customer lifetime value" grouped by the different districts.
SELECT
district,
ROUND(AVG(total),2) avg_customer_spent
FROM
(SELECT 
c.customer_id,
district,
SUM(amount) total
FROM payment p
INNER JOIN customer c
ON c.customer_id=p.customer_id
INNER JOIN address a
ON c.address_id=a.address_id
GROUP BY district, c.customer_id) sub
GROUP BY district
ORDER BY 2 DESC

-- Task: Create a list that shows all payments including the payment_id, amount, and the film category (name) plus the total amount that was 
-- made in this category. Order the results ascendingly by the category (name) and as second order criterion by the payment_id ascendingly.
SELECT
title,
amount,
name,
payment_id,
(SELECT SUM(amount) FROM payment p
LEFT JOIN rental r
ON r.rental_id=p.rental_id
LEFT JOIN inventory i
ON i.inventory_id=r.inventory_id
LEFT JOIN film f
ON f.film_id=i.film_id
LEFT JOIN film_category fc
ON fc.film_id=f.film_id
LEFT JOIN category c1
ON c1.category_id=fc.category_id
WHERE c1.name=c.name)
FROM payment p
LEFT JOIN rental r
ON r.rental_id=p.rental_id
LEFT JOIN inventory i
ON i.inventory_id=r.inventory_id
LEFT JOIN film f
ON f.film_id=i.film_id
LEFT JOIN film_category fc
ON fc.film_id=f.film_id
LEFT JOIN category c
ON c.category_id=fc.category_id
ORDER BY name
