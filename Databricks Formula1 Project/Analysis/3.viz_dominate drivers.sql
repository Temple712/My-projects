-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 drivers of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_drivers
as
select drivers_name, 
sum(calculated_points) as total_points , 
count(*) as total_races, 
avg(calculated_points) as avg_points,
rank() over (order by avg(calculated_points) desc) driver_rank
from f1_presentation.calculated_race_results
group by drivers_name
having total_races > 50
order by avg_points desc

-- COMMAND ----------

select * from v_dominant_drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Top 10 drivers of all times and their performance over the years

-- COMMAND ----------

select race_year,
drivers_name, 
sum(calculated_points) as total_points , 
count(*) as total_races, 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where drivers_name in (select drivers_name from v_dominant_drivers where driver_rank <=10)
group by drivers_name, race_year
order by race_year, avg_points desc

-- COMMAND ----------

select race_year,
drivers_name, 
sum(calculated_points) as total_points , 
count(*) as total_races, 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where drivers_name in (select drivers_name from v_dominant_drivers where driver_rank <=10)
group by drivers_name, race_year
order by race_year, avg_points desc

-- COMMAND ----------

select race_year,
drivers_name, 
sum(calculated_points) as total_points , 
count(*) as total_races, 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where drivers_name in (select drivers_name from v_dominant_drivers where driver_rank <=10)
group by drivers_name, race_year
order by race_year, avg_points desc

-- COMMAND ----------

