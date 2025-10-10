# Databricks notebook source
# MAGIC %md
# MAGIC #### Task: Create a list of all the different (distinct) replacement costs of the films.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT replacement_cost 
# MAGIC FROM film
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Task: Create a list of the film titles including their title, length, and category name ordered descendingly by length. Filter the results to only the movies in the category 'Drama' or 'Sports'.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC title,
# MAGIC name,
# MAGIC length
# MAGIC FROM film f
# MAGIC LEFT JOIN film_category fc
# MAGIC ON f.film_id=fc.film_id
# MAGIC LEFT JOIN category c
# MAGIC ON c.category_id=fc.category_id
# MAGIC WHERE name = 'Sports' OR name = 'Drama'
# MAGIC ORDER BY length DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task: Create an overview of how many movies (titles) there are in each category (name).

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC name,
# MAGIC COUNT(title)
# MAGIC FROM film f
# MAGIC INNER JOIN film_category fc
# MAGIC ON f.film_id=fc.film_id
# MAGIC INNER JOIN category c
# MAGIC ON c.category_id=fc.category_id
# MAGIC GROUP BY name
# MAGIC ORDER BY 2 DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task: Create an overview of the actors' first and last names and in how many movies they appear in.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC a.actor_id,
# MAGIC first_name,
# MAGIC last_name,
# MAGIC COUNT(*)
# MAGIC FROM actor a
# MAGIC LEFT JOIN film_actor fa
# MAGIC ON fa.actor_id=a.actor_id
# MAGIC LEFT JOIN film f
# MAGIC ON fa.film_id=f.film_id
# MAGIC GROUP BY a.actor_id,first_name, last_name
# MAGIC ORDER BY COUNT(*) DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task: Create an overview of the addresses that are not associated to any customer.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM address a
# MAGIC LEFT JOIN customer c
# MAGIC ON c.address_id = a.address_id
# MAGIC WHERE c.first_name is null

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task: Create the overview of the sales  to determine the from which city (we are interested in the city in which the customer lives, not where the store is) most sales occur.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC city,
# MAGIC SUM(amount)
# MAGIC FROM payment p
# MAGIC LEFT JOIN customer c
# MAGIC ON p.customer_id=c.customer_id
# MAGIC LEFT JOIN address a
# MAGIC ON a.address_id=c.address_id
# MAGIC LEFT JOIN city ci
# MAGIC ON ci.city_id=a.city_id
# MAGIC GROUP BY city
# MAGIC ORDER BY city DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Task: Create an overview of the revenue (sum of amount) grouped by a column in the format "country, city".

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC country ||', ' ||city,
# MAGIC SUM(amount)
# MAGIC FROM payment p
# MAGIC LEFT JOIN customer c
# MAGIC ON p.customer_id=c.customer_id
# MAGIC LEFT JOIN address a
# MAGIC ON a.address_id=c.address_id
# MAGIC LEFT JOIN city ci
# MAGIC ON ci.city_id=a.city_id
# MAGIC LEFT JOIN country co
# MAGIC ON co.country_id=ci.country_id
# MAGIC GROUP BY country ||', ' ||city
# MAGIC ORDER BY 2 ASC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task: Create a list with the average of the sales amount each staff_id has per customer.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC staff_id,
# MAGIC ROUND(AVG(total),2) as avg_amount 
# MAGIC FROM (
# MAGIC SELECT SUM(amount) as total,customer_id,staff_id
# MAGIC FROM payment
# MAGIC GROUP BY customer_id, staff_id) a
# MAGIC GROUP BY staff_id

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Task: Create a query that shows average daily revenue of all Sundays.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC AVG(total)
# MAGIC FROM 
# MAGIC 	(SELECT
# MAGIC 	 SUM(amount) as total,
# MAGIC 	 DATE(payment_date),
# MAGIC 	 EXTRACT(dow from payment_date) as weekday
# MAGIC 	 FROM payment
# MAGIC 	 WHERE EXTRACT(dow from payment_date)=0
# MAGIC 	 GROUP BY DATE(payment_date),weekday) daily

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Task: Create a list of movies - with their length and their replacement cost - that are longer than the average length in each replacement cost group.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC title,
# MAGIC length
# MAGIC FROM film f1
# MAGIC WHERE length > (SELECT AVG(length) FROM film f2
# MAGIC 			   WHERE f1.replacement_cost=f2.replacement_cost)
# MAGIC ORDER BY length ASC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task: Create a list that shows the "average customer lifetime value" grouped by the different districts.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC district,
# MAGIC ROUND(AVG(total),2) avg_customer_spent
# MAGIC FROM
# MAGIC (SELECT 
# MAGIC c.customer_id,
# MAGIC district,
# MAGIC SUM(amount) total
# MAGIC FROM payment p
# MAGIC INNER JOIN customer c
# MAGIC ON c.customer_id=p.customer_id
# MAGIC INNER JOIN address a
# MAGIC ON c.address_id=a.address_id
# MAGIC GROUP BY district, c.customer_id) sub
# MAGIC GROUP BY district
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task: Create a list that shows all payments including the payment_id, amount, and the film category (name) plus the total amount that was 
# MAGIC #### made in this category. Order the results ascendingly by the category (name) and as second order criterion by the payment_id ascendingly.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC title,
# MAGIC amount,
# MAGIC name,
# MAGIC payment_id,
# MAGIC (SELECT SUM(amount) FROM payment p
# MAGIC LEFT JOIN rental r
# MAGIC ON r.rental_id=p.rental_id
# MAGIC LEFT JOIN inventory i
# MAGIC ON i.inventory_id=r.inventory_id
# MAGIC LEFT JOIN film f
# MAGIC ON f.film_id=i.film_id
# MAGIC LEFT JOIN film_category fc
# MAGIC ON fc.film_id=f.film_id
# MAGIC LEFT JOIN category c1
# MAGIC ON c1.category_id=fc.category_id
# MAGIC WHERE c1.name=c.name)
# MAGIC FROM payment p
# MAGIC LEFT JOIN rental r
# MAGIC ON r.rental_id=p.rental_id
# MAGIC LEFT JOIN inventory i
# MAGIC ON i.inventory_id=r.inventory_id
# MAGIC LEFT JOIN film f
# MAGIC ON f.film_id=i.film_id
# MAGIC LEFT JOIN film_category fc
# MAGIC ON fc.film_id=f.film_id
# MAGIC LEFT JOIN category c
# MAGIC ON c.category_id=fc.category_id
# MAGIC ORDER BY name