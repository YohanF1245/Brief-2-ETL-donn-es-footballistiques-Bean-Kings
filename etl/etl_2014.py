import duckdb
import etl.etl_inserter_2014 as inserter
import pandas as pd
from unidecode import unidecode
import geonamescache
import logging

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

gc = geonamescache.GeonamesCache()
df = pd.read_csv('./data/WorldCupMatches2014.csv', sep=";", encoding='iso-8859-1') 
logger = logging.getLogger("ETL")

cities =  gc.get_cities()
countries = gc.get_countries()