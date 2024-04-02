import pandas as pd
import geopandas as gpd
import psycopg2
import os
import random
from source.db_query_split import *

# os.chdir(r'D:\Sociometrik\poc_data_extraction')
os.getcwd()

loc = 'loc.locality, '
day_prt = 'ft.day_part, '
out_loc = r'output_csv'

# ========================================================================================================================================
town = "'Faridabad'"  # Use "'Faridabad', 'Gurugram'" for multiple towns
strt_dat = "'2023-06-09'" # Start Date Using for Footfall
end_dat = "'2023-06-10'" # End Date Using for Footfall

#-------------------EXTRACT All Data, PoiCount and CS DATA AT TOWN LEVEL -----------------------------------------------
all_dt_town = pd.read_sql(query_splt[1].format('', town), engine)
all_dt_town.to_csv(os.path.join(out_loc,'Alldata_Town_Level.csv'), index=False)
poi_cnt_town = pd.read_sql(query_splt[2].format('', town), engine)
poi_cnt_town.to_csv(os.path.join(out_loc,'POICount_Town_Level.csv'), index=False)
cs_dt_town = pd.read_sql(query_splt[0].format('', town), engine)
cs_dt_town.to_csv(os.path.join(out_loc,'CS_Data_Town_Level.csv'), index=False)
brnd_catg_poi_town = pd.read_sql(query_splt[20].format(town), engine)
brnd_catg_poi_town.to_csv(os.path.join(out_loc,'Brand_POI_CNT_Catgory_Town_Level.csv'), index=False)

#-------------------EXTRACT All Data, PoiCount and CS DATA AT LOCALITY LEVEL -------------------------------------------
all_dt_locality = pd.read_sql(query_splt[1].format(loc, town), engine)
all_dt_locality.to_csv(os.path.join(out_loc,'Alldata_Locality_Level.csv'), index=False)
poi_cnt_locality = pd.read_sql(query_splt[2].format(loc, town), engine)
poi_cnt_locality.to_csv(os.path.join(out_loc,'POICount_Locality_Level.csv'), index=False)
cs_dt_locality = pd.read_sql(query_splt[0].format(loc, town), engine)
cs_dt_locality.to_csv(os.path.join(out_loc,'CS_Data_Locality_Level.csv'), index=False)
brnd_catg_poi_loc = pd.read_sql(query_splt[21].format(town), engine)
brnd_catg_poi_loc.to_csv(os.path.join(out_loc,'Brand_POI_CNT_Catgory_Locality_Level.csv'), index=False)

#-------------------EXTRACT FOOTFALL FROM HEX9 TO HEX8 FOR SELECTED TOWN -----------------------------------------------
#--Aggregate from HEX09 to HEX08 by day part and date wise from start date to end date
footfall_hex08_day = pd.read_sql(query_splt[3].format(strt_dat, end_dat, town, day_prt), engine)
footfall_hex08_day.to_csv(os.path.join(out_loc,'FOOTFALL_HEX08_daypart_startDate_endDat.csv'), index=False)
#--Aggregate from HEX09 to HEX08 by date wise from start date to end date
footfall_hex08_dt = pd.read_sql(query_splt[3].format(strt_dat, end_dat, town, ''), engine)
footfall_hex08_dt.to_csv(os.path.join(out_loc,'FOOTFALL_HEX08_startDate_endDat.csv'), index=False)
#--Aggregate from HEX09 to HEX08 all available data
footfall_hex08 = pd.read_sql(query_splt[4].format(town), engine)
footfall_hex08.to_csv(os.path.join(out_loc,'FOOTFALL_HEX08.csv'), index=False)

#-------------------EXTRACT FOOTFALL FROM HEX9 TO LOCALITY FOR SELECTED TOWN -------------------------------------------
#--Aggregate from HEX09 to Locality by day part and date wise from start date to end date
footfall_loc_day = pd.read_sql(query_splt[5].format(strt_dat, end_dat, town, day_prt), engine)
footfall_loc_day.to_csv(os.path.join(out_loc,'FOOTFALL_LOC_daypart_startDate_endDat.csv'), index=False)
#--Aggregate from HEX09 to Locality by date wise from start date to end date
footfall_loc_dt = pd.read_sql(query_splt[5].format(strt_dat, end_dat, town, ''), engine)
footfall_loc_dt.to_csv(os.path.join(out_loc,'FOOTFALL_LOC_startDate_endDat.csv'), index=False)
#--Aggregate from HEX09 to Locality all available data
footfall_loc = pd.read_sql(query_splt[6].format(town), engine)
footfall_loc.to_csv(os.path.join(out_loc,'FOOTFALL_LOC.csv'), index=False)


#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx PART II xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
#-------------------MAKE 50 METER BUFFER FROM POLYGON AND EXTRACT POI COUNT BY BRAND AND NON BRAND ---------------------
# Insert Shapefile into database to make 50 meter buffer and get poi.
# Make sure your shapefile have columns "property_cd, name, t_name, town_cd, state"
# gdf = gpd.read_file(r'C:\GIS\top104grwthcentre5km_buff.shp')
# #Makesure your table name is not available in DB otherwise it will give you duplicate table name error
# gdf.to_postgis(con = engine_past, name=table_nm, schema='public', if_exists='fail', index=False)

table_nm = 'property'
column_id = 'property_cd, '
column_nm = 'name, '

nmb = random.randint(0,100)
tmp_tbl = str(nmb)+table_nm
conn = psycopg2.connect(pgconnection)
cur = conn.cursor()
cur.execute(query_splt[7].format(table_nm, table_nm, column_id, column_nm))
conn.commit()
buff_brnd_poi_cnt = pd.read_sql(query_splt[8].format(table_nm, column_id, column_nm), engine)
buff_brnd_poi_cnt.to_csv(os.path.join(out_loc,'50Meter_buff_poicnt_brand.csv'), index=False)
cur.execute(query_splt[9].format(table_nm))
conn.commit()
conn.close()

#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx PART III xXxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
#-------------------EXTRACT DATA FROM LOCATION BY GIVEN LAT LONG USING BUFFER ------------------------------------------
lat_long = '28.38636, 77.30458', '28.55811, 77.21271'
lat_long_nm = 'buff0001', 'buff0002'
buffer = 2000 #Meter

nmb = random.randint(0,1000)
conn = psycopg2.connect(pgconnection)
cur = conn.cursor()
cur.execute(query_splt[10].format(nmb))
conn.commit()
for i in range(len(lat_long)):
    buff_cd = lat_long_nm[i]
    cur.execute(query_splt[11].format(nmb, buff_cd, lat_long[i], str(buffer)+' Mtr'))
    conn.commit()
cur.execute(query_splt[12].format(nmb))
conn.commit()
cur.execute(query_splt[13].format(nmb, buffer))
conn.commit()
#-------------------EXTRACT All Data, PoiCount and CS DATA AT BUFFER LEVEL ---------------------------------------------
all_dt_buff = pd.read_sql(query_splt[14].format(nmb, ''), engine)
all_dt_buff.to_csv(os.path.join(out_loc,'AllData_Buff.csv'), index=False)
poi_cnt_buff = pd.read_sql(query_splt[15].format(nmb, ''), engine)
poi_cnt_buff.to_csv(os.path.join(out_loc,'POICount_Buff.csv'), index=False)
cs_dt_buff = pd.read_sql(query_splt[16].format(nmb, ''), engine)
cs_dt_buff.to_csv(os.path.join(out_loc,'CS_Buff.csv'), index=False)
ft_dt_buff = pd.read_sql(query_splt[17].format(nmb, '', strt_dat, end_dat), engine)
ft_dt_buff.to_csv(os.path.join(out_loc,'FootFall_Buff.csv'), index=False)
poi_brnd_buff = pd.read_sql(query_splt[18].format(nmb, ''), engine)
poi_brnd_buff.to_csv(os.path.join(out_loc,'Brand_POICnt_Buff.csv'), index=False)
#-------------------EXTRACT All Data, PoiCount and CS DATA AT BUFFER+LOCALITY LEVEL ------------------------------------
all_dt_buff_loc = pd.read_sql(query_splt[14].format(nmb, loc), engine)
all_dt_buff_loc.to_csv(os.path.join(out_loc,'AllData_Buff_locality.csv'), index=False)
poi_cnt_buff_loc = pd.read_sql(query_splt[15].format(nmb, loc), engine)
poi_cnt_buff_loc.to_csv(os.path.join(out_loc,'POICount_Buff_locality.csv'), index=False)
cs_dt_buff_loc = pd.read_sql(query_splt[16].format(nmb, loc), engine)
cs_dt_buff_loc.to_csv(os.path.join(out_loc,'CS_Buff_locality.csv'), index=False)
ft_dt_buff = pd.read_sql(query_splt[17].format(nmb, loc, strt_dat, end_dat), engine)
ft_dt_buff_loc.to_csv(os.path.join(out_loc,'FootFall_Buff_locality.csv'), index=False)
poi_brnd_buff_loc = pd.read_sql(query_splt[18].format(nmb, loc), engine)
poi_brnd_buff_loc.to_csv(os.path.join(out_loc,'Brand_POICnt_Buff_Locality.csv'), index=False)
#------------ REMOVE EXTRA TABLE FROM DB --------------------------
cur.execute(query_splt[19].format(nmb))
conn.commit()
cur.close()