from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import text
import os

def create_sql_connection():
    try:
        db_url = URL.create(
            drivername='postgresql+psycopg2',
            database=os.getenv('DB_NAME'),
            username=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )

        engine  = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"The error '{e}' occurred")
        return None

def execute_query(query, params):
    engine = create_sql_connection()
    with engine.connect() as connection:
        connection.execute(text(query), params)


def bulk_insert_to_rds(messages):
    try:
        insert_values = []
        for msg in messages:
            insert_values.append({
                'message_id': msg['messageId'],
                'location_id': msg['locationId'],
                'location': msg['location'],
                'parameter': msg['parameter'],
                'value': msg['value'],
                'date_time_utc': msg['date']['utc'],
                'date_time_local': msg['date']['local'],
                'unit': msg['unit'],
                'latitude': msg['coordinates']['latitude'],
                'longitude': msg['coordinates']['longitude'],
                'country': msg['country'],
                'city': msg['city'],
                'is_mobile': msg['isMobile'],
                'is_analysis': msg['isAnalysis'],
                'entity': msg['entity'],
                'sensor_type': msg['sensorType']
            })
        
        query = """
            INSERT INTO measurements (
                message_id, location_id, location, parameter, value, date_time_utc, date_time_local,
                unit, latitude, longitude, country, city, is_mobile, is_analysis, entity, sensor_type
            ) VALUES (
                :message_id, :location_id, :location, :parameter, :value, :date_time_utc, :date_time_local,
                :unit, :latitude, :longitude, :country, :city, :is_mobile, :is_analysis, :entity, :sensor_type
            )
        """
        execute_query(query, insert_values)


    except Exception as e:
        print(f"Error: {str(e)}")