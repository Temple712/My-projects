-- Databricks notebook source
use f1_processed

-- COMMAND ----------

show tables

-- COMMAND ----------

create table f1_presentation.calculated_race_results
using parquet
as
select races.race_year, constructors.name as team_name, drivers.name as drivers_name, results.position, results.points,
11 - results.position as calculated_points
from results
inner join races
on results.race_id = races.race_id
inner join drivers
on results.driver_id = drivers.driver_id
inner join constructors
on results.constructor_id = constructors.constructor_id
where results.position in (1,2,3,4,5,6,7,8,9,10)


-- COMMAND ----------

select drivers_name, sum(calculated_points) 
from f1_presentation.calculated_race_results
where race_year >= 2010
group by drivers_name
order by sum(calculated_points) desc

-- COMMAND ----------

select drivers_name, race_year, position
from f1_presentation.calculated_race_results
where drivers_name = 'Lewis Hamilton'
order by race_year
limit 10

-- COMMAND ----------

