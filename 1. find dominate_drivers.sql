-- Databricks notebook source
select drivers_name, sum(calculated_points) as total_points , count(*) as total_races, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by drivers_name
having total_races > 50
order by avg_points desc

-- COMMAND ----------

select drivers_name, sum(calculated_points) as total_points , count(*) as total_races, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year >= 2011
group by drivers_name
having total_races > 50
order by avg_points desc

-- COMMAND ----------

