# Import the dependencies.
from flask import Flask, jsonify
import json
import numpy as np
import pandas as pd
import datetime as dt
from datetime import timedelta, datetime

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect, distinct, Table, MetaData, select, join

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Declare a Base using `automap_base()`
Base = automap_base()

#################################################
# Database Setup
#################################################


# reflect an existing database into a new model
Base.prepare(autoload_with=engine)
Measurement = Base.classes.measurement
Station = Base.classes.station

# reflect the tables
inspector = inspect(engine)
inspector.get_table_names()

Base.classes.keys()

# Save references to each table
tables = {}

for table_name,table_class in Base.classes.items():
    tables[table_name]=table_class

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
# 3. Define static routes
@app.route("/api/v1.0/climate-app")
def climate_app():
    """Welcome to Hawaii climate app"""


@app.route("/")
def welcome():
    return (
        f"Welcome to Hawaii climate app!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/temperature/2016-08-23<br/>"
        f"/api/v1.0/temperature/2016-08-23/2017-08-23"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    recent_date_dt = datetime.strptime(recent_date.date, '%Y-%m-%d')
    one_year_past_date = recent_date_dt - timedelta(days=365)
    one_year_past_date_str = one_year_past_date.strftime('%Y-%m-%d')
    recent_date_str = recent_date_dt.strftime('%Y-%m-%d')

    # Perform a query to retrieve the data and precipitation scores
    prpc_date = session.query(Measurement.date, Measurement.prcp).\
            filter((Measurement.date <= recent_date_str) & (Measurement.date >= one_year_past_date_str)).all()
    
    prpc_date_dict = {}
    
    for date, prcp in prpc_date:
        prpc_date_dict[date] = prcp
    return jsonify(prpc_date_dict)

@app.route("/api/v1.0/stations")
def stations():
    stations = session.query(Station.station, Station.name).all()
    
    station_dict ={}
    for station, name in stations:
        station_dict[station] = name
    return jsonify(station_dict)

@app.route("/api/v1.0/tobs")
def tobs():
    # Perform a join between table1 and table2 on a common column
    join_condition = Measurement.station == Station.station
 
    # Execute the query
    connection = engine.connect()    
    
    active_station = session.query(Measurement.station, func.count(Measurement.station)) \
                         .group_by(Measurement.station) \
                         .order_by(func.count(Measurement.station).desc()) \
                         .limit(1).all()
    
    most_active_station = active_station[0][0]

    recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    recent_date_dt = datetime.strptime(recent_date.date, '%Y-%m-%d')
    one_year_past_date = recent_date_dt - timedelta(days=365)
    one_year_past_date_str = one_year_past_date.strftime('%Y-%m-%d')
    recent_date_str = recent_date_dt.strftime('%Y-%m-%d')

    joined_query = session.query(Measurement, Station).filter(join_condition & \
                    (Measurement.date <= recent_date_str) & \
                    (Measurement.date >= one_year_past_date_str) & \
                    (Measurement.station == most_active_station)).all()

   
    prpc_tobs_dict = {}
    prpc_date_list =[]
    
   
    for measurement, station in joined_query:
        prpc_tobs_dict = {
            'station': measurement.station,
            'name': station.name,
            'date': measurement.date,
            'tobs': measurement.tobs
        }
        prpc_date_list.append(prpc_tobs_dict)

    return jsonify(prpc_date_list)


@app.route("/api/v1.0/temperature/<start>")
def temperature_data(start):
    # Perform a query to retrieve the data and precipitation scores
    prpc_date = session.query(Measurement.date, Measurement.tobs).\
            filter(Measurement.date >= start).all()
    
    prpc_date_dict = {}
    
    for date, prcp in prpc_date:
        prpc_date_dict[date] = prcp
    return jsonify(prpc_date_dict)


@app.route("/api/v1.0/temperature/<start>/<end>")
def temp_start_end(start,end):
    prpc_date = session.query(Measurement.date, Measurement.tobs).\
            filter((Measurement.date >= start) & (Measurement.date <= end)).all()    
    prpc_date_dict = {}
    
    for date, prcp in prpc_date:
        prpc_date_dict[date] = prcp
    
    return jsonify(prpc_date_dict)



# 4. Define main behavior
if __name__ == "__main__":
    app.run(debug=True)