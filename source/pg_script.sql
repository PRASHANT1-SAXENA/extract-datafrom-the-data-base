SELECT *, ROUND(CAST(float8 (num/denom)*100 AS numeric), 1) AS cs_val FROM (
SELECT {0} cs.t_name, cs.is_tc, indicator_name, indicatr, sub_catg, SUM(cs.maids_for_segment_in_grid) AS num, SUM(cs.maids_in_hexgrid08) AS denom 
	FROM public.hexgrid08_cs cs  LEFT JOIN public.locality_geo_cod loc ON cs.hexid08=loc.hex_id WHERE loc.t_name IN ({1})
	GROUP BY {0} cs.t_name, cs.is_tc, indicator_name, indicatr, sub_catg) AS a
--
SELECT {0} hx.t_name, hx.is_tc,
	AVG(CASE WHEN dt.indicatr = 'average_monthly_residential_rental_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_monthly_residential_rental_price,
	AVG(CASE WHEN dt.indicatr = 'building_density' then CAST(dt.val AS DECIMAL) else NULL end) AS building_density,
	AVG(CASE WHEN dt.indicatr = 'green_space_density' then CAST(dt.val AS DECIMAL) else NULL end) AS green_space_density,
	SUM(CASE WHEN dt.indicatr = 'road_length' then CAST(dt.val AS DECIMAL) else NULL end) AS road_length,
	SUM(CASE WHEN dt.indicatr = 'transportation_hubs' then CAST(dt.val AS DECIMAL) else NULL end) AS transportation_hubs,
	AVG(CASE WHEN dt.indicatr = 'daily_avg_footfall' then CAST(dt.val AS DECIMAL) else NULL end) AS daily_avg_footfall,
	AVG(CASE WHEN dt.indicatr = 'weekly_avg_footfall' then CAST(dt.val AS DECIMAL) else NULL end) AS weekly_avg_footfall,
	SUM(CASE WHEN dt.indicatr = 'consumer_digital_expenditure' then CAST(dt.val AS DECIMAL) else NULL end) AS consumer_digital_expenditure,
	SUM(CASE WHEN dt.indicatr = 'total_digital_consumers' then CAST(dt.val AS DECIMAL) else NULL end) AS total_digital_consumers,
	SUM(CASE WHEN dt.indicatr = 'elderly_population' then CAST(dt.val AS DECIMAL) else NULL end) AS elderly_population,
	SUM(CASE WHEN dt.indicatr = 'female_population' then CAST(dt.val AS DECIMAL) else NULL end) AS female_population,
	SUM(CASE WHEN dt.indicatr = 'female_population_15_to_49years' then CAST(dt.val AS DECIMAL) else NULL end) AS female_population_15_to_49years,
	SUM(CASE WHEN dt.indicatr = 'kids_population' then CAST(dt.val AS DECIMAL) else NULL end) AS kids_population,
	SUM(CASE WHEN dt.indicatr = 'male_population' then CAST(dt.val AS DECIMAL) else NULL end) AS male_population,
	SUM(CASE WHEN dt.indicatr = 'population' then CAST(dt.val AS DECIMAL) else NULL end) AS population,
	SUM(CASE WHEN dt.indicatr = 'youth_population' then CAST(dt.val AS DECIMAL) else NULL end) AS youth_population,
	AVG(CASE WHEN dt.indicatr = 'population_density' then CAST(dt.val AS DECIMAL) else NULL end) AS population_density,
	AVG(CASE WHEN dt.indicatr = 'average_residential_sale_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_residential_sale_price,
	AVG(CASE WHEN dt.indicatr = 'public_infrastructure_index' then CAST(dt.val AS DECIMAL) else NULL end) AS public_infrastructure_index,
	AVG(CASE WHEN dt.indicatr = 'digital_adoption_rate' then CAST(dt.val AS DECIMAL) else NULL end) AS digital_adoption_rate,
	AVG(CASE WHEN dt.indicatr = 'average_monthly_commercial_rental_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_monthly_commercial_rental_price
FROM hexgrid08 hx LEFT JOIN public.locality_geo_cod loc ON hx.hexid08=loc.hex_id 
	INNER JOIN hexgrid08_data dt ON hx.hexid08=dt.hexid08 WHERE loc.t_name IN ({1}) GROUP BY {0} hx.t_name, hx.is_tc;
--
SELECT {0} hx.t_name, hx.is_tc,
	SUM(CASE WHEN dt.indicatr = 'amusement_park' then CAST(dt.val AS DECIMAL) else NULL end) AS amusement_park,
	SUM(CASE WHEN dt.indicatr = 'clothing_store' then CAST(dt.val AS DECIMAL) else NULL end) AS clothing_store,
	SUM(CASE WHEN dt.indicatr = 'sports_centre' then CAST(dt.val AS DECIMAL) else NULL end) AS sports_centre,
	SUM(CASE WHEN dt.indicatr = 'museum' then CAST(dt.val AS DECIMAL) else NULL end) AS museum,
	SUM(CASE WHEN dt.indicatr = 'golf_practice_range' then CAST(dt.val AS DECIMAL) else NULL end) AS golf_practice_range,
	SUM(CASE WHEN dt.indicatr = 'ice_skating_rink' then CAST(dt.val AS DECIMAL) else NULL end) AS ice_skating_rink,
	SUM(CASE WHEN dt.indicatr = 'industrial_zone' then CAST(dt.val AS DECIMAL) else NULL end) AS industrial_zone,
	SUM(CASE WHEN dt.indicatr = 'bowling_centre' then CAST(dt.val AS DECIMAL) else NULL end) AS bowling_centre,
	SUM(CASE WHEN dt.indicatr = 'grocery_store' then CAST(dt.val AS DECIMAL) else NULL end) AS grocery_store,
	SUM(CASE WHEN dt.indicatr = 'medical_service' then CAST(dt.val AS DECIMAL) else NULL end) AS medical_service,
	SUM(CASE WHEN dt.indicatr = 'transportation_service' then CAST(dt.val AS DECIMAL) else NULL end) AS transportation_service,
	SUM(CASE WHEN dt.indicatr = 'school' then CAST(dt.val AS DECIMAL) else NULL end) AS school,
	SUM(CASE WHEN dt.indicatr = 'fast_food_restaurants' then CAST(dt.val AS DECIMAL) else NULL end) AS fast_food_restaurants,
	SUM(CASE WHEN dt.indicatr = 'golf_course' then CAST(dt.val AS DECIMAL) else NULL end) AS golf_course,
	SUM(CASE WHEN dt.indicatr = 'library' then CAST(dt.val AS DECIMAL) else NULL end) AS library,
	SUM(CASE WHEN dt.indicatr = 'consumer_electronics_store' then CAST(dt.val AS DECIMAL) else NULL end) AS consumer_electronics_store,
	SUM(CASE WHEN dt.indicatr = 'jewellery_store' then CAST(dt.val AS DECIMAL) else NULL end) AS jewellery_store,
	SUM(CASE WHEN dt.indicatr = 'restaurant' then CAST(dt.val AS DECIMAL) else NULL end) AS restaurant,
	SUM(CASE WHEN dt.indicatr = 'premium_hotels' then CAST(dt.val AS DECIMAL) else NULL end) AS premium_hotels,
	SUM(CASE WHEN dt.indicatr = 'train_station' then CAST(dt.val AS DECIMAL) else NULL end) AS train_station,
	SUM(CASE WHEN dt.indicatr = 'airport' then CAST(dt.val AS DECIMAL) else NULL end) AS airport,
	SUM(CASE WHEN dt.indicatr = 'motorcycle_dealership' then CAST(dt.val AS DECIMAL) else NULL end) AS motorcycle_dealership,
	SUM(CASE WHEN dt.indicatr = 'book_store' then CAST(dt.val AS DECIMAL) else NULL end) AS book_store,
	SUM(CASE WHEN dt.indicatr = 'home_improvement_hardware_store' then CAST(dt.val AS DECIMAL) else NULL end) AS home_improvement_hardware_store,
	SUM(CASE WHEN dt.indicatr = 'hospital' then CAST(dt.val AS DECIMAL) else NULL end) AS hospital,
	SUM(CASE WHEN dt.indicatr = 'atm' then CAST(dt.val AS DECIMAL) else NULL end) AS atm,
	SUM(CASE WHEN dt.indicatr = 'retail_branded' then CAST(dt.val AS DECIMAL) else NULL end) AS retail_branded,
	SUM(CASE WHEN dt.indicatr = 'retail_non_branded' then CAST(dt.val AS DECIMAL) else NULL end) AS retail_non_branded,
	SUM(CASE WHEN dt.indicatr = 'training_centre_institute' then CAST(dt.val AS DECIMAL) else NULL end) AS training_centre_institute,
	SUM(CASE WHEN dt.indicatr = 'casino' then CAST(dt.val AS DECIMAL) else NULL end) AS casino,
	SUM(CASE WHEN dt.indicatr = 'home_specialty_store' then CAST(dt.val AS DECIMAL) else NULL end) AS home_specialty_store,
	SUM(CASE WHEN dt.indicatr = 'government_office' then CAST(dt.val AS DECIMAL) else NULL end) AS government_office,
	SUM(CASE WHEN dt.indicatr = 'truck_dealership' then CAST(dt.val AS DECIMAL) else NULL end) AS truck_dealership,
	SUM(CASE WHEN dt.indicatr = 'fine_dining_restaurants' then CAST(dt.val AS DECIMAL) else NULL end) AS fine_dining_restaurants,
	SUM(CASE WHEN dt.indicatr = 'police_station' then CAST(dt.val AS DECIMAL) else NULL end) AS police_station,
	SUM(CASE WHEN dt.indicatr = 'department_store' then CAST(dt.val AS DECIMAL) else NULL end) AS department_store,
	SUM(CASE WHEN dt.indicatr = 'beauty_and_salons' then CAST(dt.val AS DECIMAL) else NULL end) AS beauty_and_salons,
	SUM(CASE WHEN dt.indicatr = 'performing_arts' then CAST(dt.val AS DECIMAL) else NULL end) AS performing_arts,
	SUM(CASE WHEN dt.indicatr = 'park_recreation_area' then CAST(dt.val AS DECIMAL) else NULL end) AS park_recreation_area,
	SUM(CASE WHEN dt.indicatr = 'shopping' then CAST(dt.val AS DECIMAL) else NULL end) AS shopping,
	SUM(CASE WHEN dt.indicatr = 'higher_education' then CAST(dt.val AS DECIMAL) else NULL end) AS higher_education,
	SUM(CASE WHEN dt.indicatr = 'coffee_shop' then CAST(dt.val AS DECIMAL) else NULL end) AS coffee_shop,
	SUM(CASE WHEN dt.indicatr = 'convenience_store' then CAST(dt.val AS DECIMAL) else NULL end) AS convenience_store,
	SUM(CASE WHEN dt.indicatr = 'bus_station' then CAST(dt.val AS DECIMAL) else NULL end) AS bus_station,
	SUM(CASE WHEN dt.indicatr = 'office_supply_services_store' then CAST(dt.val AS DECIMAL) else NULL end) AS office_supply_services_store,
	SUM(CASE WHEN dt.indicatr = 'auto_service_maintenance' then CAST(dt.val AS DECIMAL) else NULL end) AS auto_service_maintenance,
	SUM(CASE WHEN dt.indicatr = 'business_facility' then CAST(dt.val AS DECIMAL) else NULL end) AS business_facility,
	SUM(CASE WHEN dt.indicatr = 'cinema' then CAST(dt.val AS DECIMAL) else NULL end) AS cinema,
	SUM(CASE WHEN dt.indicatr = 'pharmacy' then CAST(dt.val AS DECIMAL) else NULL end) AS pharmacy,
	SUM(CASE WHEN dt.indicatr = 'auto_dealership' then CAST(dt.val AS DECIMAL) else NULL end) AS auto_dealership,
	SUM(CASE WHEN dt.indicatr = 'nightlife' then CAST(dt.val AS DECIMAL) else NULL end) AS nightlife,
	SUM(CASE WHEN dt.indicatr = 'hotel' then CAST(dt.val AS DECIMAL) else NULL end) AS hotel,
	SUM(CASE WHEN dt.indicatr = 'bar_or_pub' then CAST(dt.val AS DECIMAL) else NULL end) AS bar_or_pub,
	SUM(CASE WHEN dt.indicatr = 'petrol_gasoline_station' then CAST(dt.val AS DECIMAL) else NULL end) AS petrol_gasoline_station,
	SUM(CASE WHEN dt.indicatr = 'bank' then CAST(dt.val AS DECIMAL) else NULL end) AS bank,
	SUM(CASE WHEN dt.indicatr = 'sporting_goods_store' then CAST(dt.val AS DECIMAL) else NULL end) AS sporting_goods_store,
	SUM(CASE WHEN dt.indicatr = 'post_office' then CAST(dt.val AS DECIMAL) else NULL end) AS post_office
FROM public.hexgrid08 hx LEFT JOIN public.locality_geo_cod loc ON hx.hexid08=loc.hex_id 
	INNER JOIN hexgrid08_data dt ON hx.hexid08=dt.hexid08 WHERE loc.t_name IN ({1}) GROUP BY {0} hx.t_name, hx.is_tc;
--
SELECT hexid08, is_tc, ft.date_actual, ft.t_name, {3} SUM(footfall) AS footfall FROM public.hexgrid08_footfall ft WHERE date_actual 
	BETWEEN DATE({0}) AND DATE({1}) AND t_name IN ({2}) GROUP BY hexid08, is_tc, t_name, {3} date_actual;
--
SELECT hexid08, is_tc, t_name, SUM(footfall) AS footfall FROM public.hexgrid08_footfall WHERE t_name IN ({0}) GROUP BY hexid08, is_tc, t_name;
--
SELECT locality, is_tc, ft.date_actual, {3} ft.t_name, SUM(footfall) AS footfall FROM hexgrid08_footfall ft LEFT JOIN public.locality_geo_cod loc 
ON ft.hexid08=loc.hex_id WHERE date_actual BETWEEN DATE({0}) AND DATE({1}) AND ft.t_name IN ({2}) GROUP BY locality, is_tc, ft.t_name, {3} ft.date_actual;
--
SELECT locality, is_tc, ft.t_name, SUM(footfall) AS footfall FROM public.hexgrid08_footfall ft LEFT JOIN public.locality_geo_cod loc 
ON ft.hexid08=loc.hex_id WHERE loc.t_name IN ({0}) GROUP BY locality, is_tc, ft.t_name;
--
DROP TABLE IF EXISTS temp.p50mtr_{0}_buff;
CREATE TABLE temp.p50mtr_{0}_buff AS
SELECT {2} {3} t_name, is_tc, '50Mtr' AS buffer, ST_Buffer_Meters(geom, 50) AS geom FROM {1};
--
SELECT *, COUNT(sub_catg) AS poi_cnt FROM (SELECT buff.{1} buff.{2} sub_catg, brand_name, brand_code FROM public.poi pi 
INNER JOIN temp.p50mtr_{0}_buff AS buff ON ST_Intersects(pi.geom,buff.geom)) po 
GROUP BY {1} {2} sub_catg, brand_name, brand_code
--
DROP TABLE IF EXISTS temp.p50mtr_{0}_buff;
--
DROP TABLE IF EXISTS temp.point_{0};
CREATE TABLE temp.point_{0} (buff_cd varchar(300), latitude float, longitude float, buffer varchar(10));
ALTER TABLE temp.point_{0} ADD COLUMN geom geometry(POINT,4326);
--
INSERT INTO temp.point_{0}
VALUES('{1}', {2}, '{3}');
--
UPDATE temp.point_{0} SET geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326);
--
DROP TABLE IF EXISTS temp.point_{0}_buff;
CREATE TABLE temp.point_{0}_buff AS SELECT buff_cd, latitude, longitude, buffer, ST_Buffer_Meters(geom, {1})AS geom FROM temp.point_{0};
DROP TABLE IF EXISTS temp.point_{0};
DROP TABLE IF EXISTS temp.hexgrid{0}_buff;
CREATE TABLE temp.hexgrid{0}_buff AS
SELECT buff.buff_cd, hx.* FROM public.hexgrid08 AS hx INNER JOIN temp.point_{0}_buff AS buff ON ST_Intersects (hx.geom, buff.geom);
--
SELECT {1} buff_cd, 
	AVG(CASE WHEN dt.indicatr = 'average_monthly_residential_rental_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_monthly_residential_rental_price,
	AVG(CASE WHEN dt.indicatr = 'building_density' then CAST(dt.val AS DECIMAL) else NULL end) AS building_density,
	AVG(CASE WHEN dt.indicatr = 'green_space_density' then CAST(dt.val AS DECIMAL) else NULL end) AS green_space_density,
	SUM(CASE WHEN dt.indicatr = 'road_length' then CAST(dt.val AS DECIMAL) else NULL end) AS road_length,
	SUM(CASE WHEN dt.indicatr = 'transportation_hubs' then CAST(dt.val AS DECIMAL) else NULL end) AS transportation_hubs,
	AVG(CASE WHEN dt.indicatr = 'daily_avg_footfall' then CAST(dt.val AS DECIMAL) else NULL end) AS daily_avg_footfall,
	AVG(CASE WHEN dt.indicatr = 'weekly_avg_footfall' then CAST(dt.val AS DECIMAL) else NULL end) AS weekly_avg_footfall,
	SUM(CASE WHEN dt.indicatr = 'consumer_digital_expenditure' then CAST(dt.val AS DECIMAL) else NULL end) AS consumer_digital_expenditure,
	SUM(CASE WHEN dt.indicatr = 'total_digital_consumers' then CAST(dt.val AS DECIMAL) else NULL end) AS total_digital_consumers,
	SUM(CASE WHEN dt.indicatr = 'elderly_population' then CAST(dt.val AS DECIMAL) else NULL end) AS elderly_population,
	SUM(CASE WHEN dt.indicatr = 'female_population' then CAST(dt.val AS DECIMAL) else NULL end) AS female_population,
	SUM(CASE WHEN dt.indicatr = 'female_population_15_to_49years' then CAST(dt.val AS DECIMAL) else NULL end) AS female_population_15_to_49years,
	SUM(CASE WHEN dt.indicatr = 'kids_population' then CAST(dt.val AS DECIMAL) else NULL end) AS kids_population,
	SUM(CASE WHEN dt.indicatr = 'male_population' then CAST(dt.val AS DECIMAL) else NULL end) AS male_population,
	SUM(CASE WHEN dt.indicatr = 'population' then CAST(dt.val AS DECIMAL) else NULL end) AS population,
	SUM(CASE WHEN dt.indicatr = 'youth_population' then CAST(dt.val AS DECIMAL) else NULL end) AS youth_population,
	AVG(CASE WHEN dt.indicatr = 'population_density' then CAST(dt.val AS DECIMAL) else NULL end) AS population_density,
	AVG(CASE WHEN dt.indicatr = 'average_residential_sale_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_residential_sale_price,
	AVG(CASE WHEN dt.indicatr = 'public_infrastructure_index' then CAST(dt.val AS DECIMAL) else NULL end) AS public_infrastructure_index,
	AVG(CASE WHEN dt.indicatr = 'digital_adoption_rate' then CAST(dt.val AS DECIMAL) else NULL end) AS digital_adoption_rate,
	AVG(CASE WHEN dt.indicatr = 'average_monthly_commercial_rental_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_monthly_commercial_rental_price
FROM temp.hexgrid{0}_buff hx LEFT JOIN locality_geo_cod loc ON hx.hexid08=loc.hex_id INNER JOIN hexgrid08_data dt ON hx.hexid08=dt.hexid08 GROUP BY {1} buff_cd;
--
SELECT  buff_cd, 
	AVG(CASE WHEN dt.indicatr = 'average_monthly_residential_rental_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_monthly_residential_rental_price,
	AVG(CASE WHEN dt.indicatr = 'building_density' then CAST(dt.val AS DECIMAL) else NULL end) AS building_density,
	AVG(CASE WHEN dt.indicatr = 'green_space_density' then CAST(dt.val AS DECIMAL) else NULL end) AS green_space_density,
	SUM(CASE WHEN dt.indicatr = 'road_length' then CAST(dt.val AS DECIMAL) else NULL end) AS road_length,
	SUM(CASE WHEN dt.indicatr = 'transportation_hubs' then CAST(dt.val AS DECIMAL) else NULL end) AS transportation_hubs,
	AVG(CASE WHEN dt.indicatr = 'daily_avg_footfall' then CAST(dt.val AS DECIMAL) else NULL end) AS daily_avg_footfall,
	AVG(CASE WHEN dt.indicatr = 'weekly_avg_footfall' then CAST(dt.val AS DECIMAL) else NULL end) AS weekly_avg_footfall,
	SUM(CASE WHEN dt.indicatr = 'consumer_digital_expenditure' then CAST(dt.val AS DECIMAL) else NULL end) AS consumer_digital_expenditure,
	SUM(CASE WHEN dt.indicatr = 'total_digital_consumers' then CAST(dt.val AS DECIMAL) else NULL end) AS total_digital_consumers,
	SUM(CASE WHEN dt.indicatr = 'elderly_population' then CAST(dt.val AS DECIMAL) else NULL end) AS elderly_population,
	SUM(CASE WHEN dt.indicatr = 'female_population' then CAST(dt.val AS DECIMAL) else NULL end) AS female_population,
	SUM(CASE WHEN dt.indicatr = 'female_population_15_to_49years' then CAST(dt.val AS DECIMAL) else NULL end) AS female_population_15_to_49years,
	SUM(CASE WHEN dt.indicatr = 'kids_population' then CAST(dt.val AS DECIMAL) else NULL end) AS kids_population,
	SUM(CASE WHEN dt.indicatr = 'male_population' then CAST(dt.val AS DECIMAL) else NULL end) AS male_population,
	SUM(CASE WHEN dt.indicatr = 'population' then CAST(dt.val AS DECIMAL) else NULL end) AS population,
	SUM(CASE WHEN dt.indicatr = 'youth_population' then CAST(dt.val AS DECIMAL) else NULL end) AS youth_population,
	AVG(CASE WHEN dt.indicatr = 'population_density' then CAST(dt.val AS DECIMAL) else NULL end) AS population_density,
	AVG(CASE WHEN dt.indicatr = 'average_residential_sale_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_residential_sale_price,
	AVG(CASE WHEN dt.indicatr = 'public_infrastructure_index' then CAST(dt.val AS DECIMAL) else NULL end) AS public_infrastructure_index,
	AVG(CASE WHEN dt.indicatr = 'digital_adoption_rate' then CAST(dt.val AS DECIMAL) else NULL end) AS digital_adoption_rate,
	AVG(CASE WHEN dt.indicatr = 'average_monthly_commercial_rental_price' then CAST(dt.val AS DECIMAL) else NULL end) AS average_monthly_commercial_rental_price
FROM temp.hexgrid{0}_buff hx LEFT JOIN locality_geo_cod loc ON hx.hexid08=loc.hex_id INNER JOIN hexgrid08_data dt ON hx.hexid08=dt.hexid08 GROUP BY  buff_cd;

SELECT {1} buff_cd,
	SUM(CASE WHEN dt.indicatr = 'amusement_park' then CAST(dt.val AS DECIMAL) else NULL end) AS amusement_park,
	SUM(CASE WHEN dt.indicatr = 'clothing_store' then CAST(dt.val AS DECIMAL) else NULL end) AS clothing_store,
	SUM(CASE WHEN dt.indicatr = 'sports_centre' then CAST(dt.val AS DECIMAL) else NULL end) AS sports_centre,
	SUM(CASE WHEN dt.indicatr = 'museum' then CAST(dt.val AS DECIMAL) else NULL end) AS museum,
	SUM(CASE WHEN dt.indicatr = 'golf_practice_range' then CAST(dt.val AS DECIMAL) else NULL end) AS golf_practice_range,
	SUM(CASE WHEN dt.indicatr = 'ice_skating_rink' then CAST(dt.val AS DECIMAL) else NULL end) AS ice_skating_rink,
	SUM(CASE WHEN dt.indicatr = 'industrial_zone' then CAST(dt.val AS DECIMAL) else NULL end) AS industrial_zone,
	SUM(CASE WHEN dt.indicatr = 'bowling_centre' then CAST(dt.val AS DECIMAL) else NULL end) AS bowling_centre,
	SUM(CASE WHEN dt.indicatr = 'grocery_store' then CAST(dt.val AS DECIMAL) else NULL end) AS grocery_store,
	SUM(CASE WHEN dt.indicatr = 'medical_service' then CAST(dt.val AS DECIMAL) else NULL end) AS medical_service,
	SUM(CASE WHEN dt.indicatr = 'transportation_service' then CAST(dt.val AS DECIMAL) else NULL end) AS transportation_service,
	SUM(CASE WHEN dt.indicatr = 'school' then CAST(dt.val AS DECIMAL) else NULL end) AS school,
	SUM(CASE WHEN dt.indicatr = 'fast_food_restaurants' then CAST(dt.val AS DECIMAL) else NULL end) AS fast_food_restaurants,
	SUM(CASE WHEN dt.indicatr = 'golf_course' then CAST(dt.val AS DECIMAL) else NULL end) AS golf_course,
	SUM(CASE WHEN dt.indicatr = 'library' then CAST(dt.val AS DECIMAL) else NULL end) AS library,
	SUM(CASE WHEN dt.indicatr = 'consumer_electronics_store' then CAST(dt.val AS DECIMAL) else NULL end) AS consumer_electronics_store,
	SUM(CASE WHEN dt.indicatr = 'jewellery_store' then CAST(dt.val AS DECIMAL) else NULL end) AS jewellery_store,
	SUM(CASE WHEN dt.indicatr = 'restaurant' then CAST(dt.val AS DECIMAL) else NULL end) AS restaurant,
	SUM(CASE WHEN dt.indicatr = 'premium_hotels' then CAST(dt.val AS DECIMAL) else NULL end) AS premium_hotels,
	SUM(CASE WHEN dt.indicatr = 'train_station' then CAST(dt.val AS DECIMAL) else NULL end) AS train_station,
	SUM(CASE WHEN dt.indicatr = 'airport' then CAST(dt.val AS DECIMAL) else NULL end) AS airport,
	SUM(CASE WHEN dt.indicatr = 'motorcycle_dealership' then CAST(dt.val AS DECIMAL) else NULL end) AS motorcycle_dealership,
	SUM(CASE WHEN dt.indicatr = 'book_store' then CAST(dt.val AS DECIMAL) else NULL end) AS book_store,
	SUM(CASE WHEN dt.indicatr = 'home_improvement_hardware_store' then CAST(dt.val AS DECIMAL) else NULL end) AS home_improvement_hardware_store,
	SUM(CASE WHEN dt.indicatr = 'hospital' then CAST(dt.val AS DECIMAL) else NULL end) AS hospital,
	SUM(CASE WHEN dt.indicatr = 'atm' then CAST(dt.val AS DECIMAL) else NULL end) AS atm,
	SUM(CASE WHEN dt.indicatr = 'retail_branded' then CAST(dt.val AS DECIMAL) else NULL end) AS retail_branded,
	SUM(CASE WHEN dt.indicatr = 'retail_non_branded' then CAST(dt.val AS DECIMAL) else NULL end) AS retail_non_branded,
	SUM(CASE WHEN dt.indicatr = 'training_centre_institute' then CAST(dt.val AS DECIMAL) else NULL end) AS training_centre_institute,
	SUM(CASE WHEN dt.indicatr = 'casino' then CAST(dt.val AS DECIMAL) else NULL end) AS casino,
	SUM(CASE WHEN dt.indicatr = 'home_specialty_store' then CAST(dt.val AS DECIMAL) else NULL end) AS home_specialty_store,
	SUM(CASE WHEN dt.indicatr = 'government_office' then CAST(dt.val AS DECIMAL) else NULL end) AS government_office,
	SUM(CASE WHEN dt.indicatr = 'truck_dealership' then CAST(dt.val AS DECIMAL) else NULL end) AS truck_dealership,
	SUM(CASE WHEN dt.indicatr = 'fine_dining_restaurants' then CAST(dt.val AS DECIMAL) else NULL end) AS fine_dining_restaurants,
	SUM(CASE WHEN dt.indicatr = 'police_station' then CAST(dt.val AS DECIMAL) else NULL end) AS police_station,
	SUM(CASE WHEN dt.indicatr = 'department_store' then CAST(dt.val AS DECIMAL) else NULL end) AS department_store,
	SUM(CASE WHEN dt.indicatr = 'beauty_and_salons' then CAST(dt.val AS DECIMAL) else NULL end) AS beauty_and_salons,
	SUM(CASE WHEN dt.indicatr = 'performing_arts' then CAST(dt.val AS DECIMAL) else NULL end) AS performing_arts,
	SUM(CASE WHEN dt.indicatr = 'park_recreation_area' then CAST(dt.val AS DECIMAL) else NULL end) AS park_recreation_area,
	SUM(CASE WHEN dt.indicatr = 'shopping' then CAST(dt.val AS DECIMAL) else NULL end) AS shopping,
	SUM(CASE WHEN dt.indicatr = 'higher_education' then CAST(dt.val AS DECIMAL) else NULL end) AS higher_education,
	SUM(CASE WHEN dt.indicatr = 'coffee_shop' then CAST(dt.val AS DECIMAL) else NULL end) AS coffee_shop,
	SUM(CASE WHEN dt.indicatr = 'convenience_store' then CAST(dt.val AS DECIMAL) else NULL end) AS convenience_store,
	SUM(CASE WHEN dt.indicatr = 'bus_station' then CAST(dt.val AS DECIMAL) else NULL end) AS bus_station,
	SUM(CASE WHEN dt.indicatr = 'office_supply_services_store' then CAST(dt.val AS DECIMAL) else NULL end) AS office_supply_services_store,
	SUM(CASE WHEN dt.indicatr = 'auto_service_maintenance' then CAST(dt.val AS DECIMAL) else NULL end) AS auto_service_maintenance,
	SUM(CASE WHEN dt.indicatr = 'business_facility' then CAST(dt.val AS DECIMAL) else NULL end) AS business_facility,
	SUM(CASE WHEN dt.indicatr = 'cinema' then CAST(dt.val AS DECIMAL) else NULL end) AS cinema,
	SUM(CASE WHEN dt.indicatr = 'pharmacy' then CAST(dt.val AS DECIMAL) else NULL end) AS pharmacy,
	SUM(CASE WHEN dt.indicatr = 'auto_dealership' then CAST(dt.val AS DECIMAL) else NULL end) AS auto_dealership,
	SUM(CASE WHEN dt.indicatr = 'nightlife' then CAST(dt.val AS DECIMAL) else NULL end) AS nightlife,
	SUM(CASE WHEN dt.indicatr = 'hotel' then CAST(dt.val AS DECIMAL) else NULL end) AS hotel,
	SUM(CASE WHEN dt.indicatr = 'bar_or_pub' then CAST(dt.val AS DECIMAL) else NULL end) AS bar_or_pub,
	SUM(CASE WHEN dt.indicatr = 'petrol_gasoline_station' then CAST(dt.val AS DECIMAL) else NULL end) AS petrol_gasoline_station,
	SUM(CASE WHEN dt.indicatr = 'bank' then CAST(dt.val AS DECIMAL) else NULL end) AS bank,
	SUM(CASE WHEN dt.indicatr = 'sporting_goods_store' then CAST(dt.val AS DECIMAL) else NULL end) AS sporting_goods_store,
	SUM(CASE WHEN dt.indicatr = 'post_office' then CAST(dt.val AS DECIMAL) else NULL end) AS post_office
FROM (SELECT buff.buff_cd, hx.* FROM hexgrid08_data hx INNER JOIN temp.hexgrid{0}_buff buff ON hx.hexid08 = buff.hexid08) dt 
LEFT JOIN locality_geo_cod loc ON dt.hexid08=loc.hex_id GROUP BY {1} buff_cd;
--
SELECT *, ROUND(CAST(float8 (num/denom)*100 AS numeric), 1) AS cs_val FROM (
SELECT {1} buff_cd, cs.t_name, cs.is_tc, indicator_name, indicatr, sub_catg, SUM(cs.maids_for_segment_in_grid) AS num, SUM(cs.maids_in_hexgrid08) AS denom
	FROM (SELECT buff.buff_cd, cs.* FROM public.hexgrid08_cs cs INNER JOIN temp.hexgrid{0}_buff buff ON cs.hexid08 = buff.hexid08) cs
LEFT JOIN public.locality_geo_cod loc ON cs.hexid08=loc.hex_id
	GROUP BY {1} buff_cd, cs.t_name, cs.is_tc, indicator_name, indicatr, sub_catg) AS a
--
SELECT {1} buff_cd, date_actual, day_part, SUM(footfall) AS footfall
	FROM (SELECT buff.buff_cd, ft.* FROM public.hexgrid08_footfall ft INNER JOIN temp.hexgrid{0}_buff buff ON ft.hexid08 = buff.hexid08
	WHERE ft.date_actual BETWEEN DATE({2}) AND DATE({3})) ft
LEFT JOIN public.locality_geo_cod loc ON ft.hexid08=loc.hex_id GROUP BY {1} buff_cd, date_actual, day_part
--
SELECT {1} po.buff_cd, sub_catg, brand_name, brand_code, COUNT(poi_id) AS poi_cnt
	FROM (SELECT buff.buff_cd, pi.* FROM public.poi pi INNER JOIN temp.hexgrid{0}_buff buff ON pi.hexid08 = buff.hexid08) po
LEFT JOIN public.locality_geo_cod loc ON po.hexid08=loc.hex_id GROUP BY po.buff_cd, sub_catg, brand_name, {1} brand_code
--
DROP TABLE IF EXISTS temp.hexgrid{0}_buff;
DROP TABLE IF EXISTS temp.point_{0}_buff;
--
SELECT hexid08, p.t_name, p.is_tc, sub_catg, brand_name, brand_code, COUNT(sub_catg) AS poi_cnt  FROM poi p INNER JOIN locality_geo_cod loc 
ON p.hexid08=loc.hex_id WHERE p.t_name IN ({0}) GROUP BY hexid08, p.t_name, p.is_tc, sub_catg, brand_name, brand_code
--
SELECT loc.locality, p.t_name, p.is_tc, sub_catg, brand_name, brand_code, COUNT(sub_catg) AS poi_cnt  FROM poi p INNER JOIN locality_geo_cod loc 
ON p.hexid08=loc.hex_id WHERE p.t_name IN ({0}) GROUP BY loc.locality, p.t_name, p.is_tc, sub_catg, brand_name, brand_code