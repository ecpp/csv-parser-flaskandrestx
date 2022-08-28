from flask import Flask, jsonify, request
from flask_restx import Api, Resource, fields, reqparse
from flask_sqlalchemy import SQLAlchemy
import os
import pandas as pd
import werkzeug
from flask_celery import make_celery


basedir = os.path.dirname(os.path.realpath(__file__))

app = Flask(__name__)

conn = "mysql://username:password@localhost/database"
app.config['SQLALCHEMY_DATABASE_URI'] = conn
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ECHO'] = True
app.config['CELERY_BROKER_URL'] = 'amqp://localhost//'
app.config['CELERY_BACKEND'] = 'db+mysql://username:password@localhost/database'
app.config['RESTX_MASK_SWAGGER'] = False
celery = make_celery(app)


api = Api(app, doc='/', title="SIMPLE REST API",
          description="Simple rest api that uploads csv file to desired MySQL database. List, updates, deletes datas accordingly...")


db = SQLAlchemy(app)

class Data(db.Model):
    ident = db.Column(db.String(128), primary_key=True)
    type = db.Column(db.String(128), nullable=True)
    name = db.Column(db.String(128), nullable=False)
    elevation_ft = db.Column(db.Integer(), nullable=True)
    continent = db.Column(db.String(128), nullable=True)
    iso_country = db.Column(db.String(128), nullable=True)
    iso_region = db.Column(db.String(128), nullable=True)
    municipality = db.Column(db.String(128), nullable=True)
    gps_code = db.Column(db.String(128), nullable=True)
    iata_code = db.Column(db.String(128), nullable=True)
    local_code = db.Column(db.String(128), nullable=True)
    coordinates = db.Column(db.String(128), nullable=True)

    def __repr__(self):
        return self.ident


data_model = api.model(
    'Data',
    {
        'ident': fields.String(),
        'type': fields.String(),
        'name': fields.String(),
        'elevation_ft': fields.Integer(),
        'continent': fields.String(),
        'iso_country': fields.String(),
        'iso_region': fields.String(),
        'municipality': fields.String(),
        'gps_code': fields.String(),
        'iata_code': fields.String(),
        'local_code': fields.String(),
        'coordinates': fields.String(),
    }
)

file_upload = reqparse.RequestParser()
file_upload.add_argument('csvfile',
                         type=werkzeug.datastructures.FileStorage,
                         location='files',
                         required=True,
                         help='CSV file')


@api.route('/upload', methods=['POST'])
class Upload(Resource):
    @api.expect(file_upload)
    @api.doc(description="Upload a csv file. It will automatically parsed and uploaded to mysql server.")
    def post(self):
        ''' Upload a csv file to mysql database '''
        filebuf = request.files['csvfile']
        df = pd.read_csv(filebuf.stream)
        df.to_sql("data", conn, if_exists='replace', index=False)
        return 'File uploaded!', 200


@api.route('/post', methods=['POST'])
class Post(Resource):
    @api.marshal_with(data_model, code=201, envelope="data")
    @api.expect(data_model)
    @api.doc(description="Create a new entry. Use json format.")
    def post(self):
        ''' Create a new data '''
        data = request.get_json()
        ident_store = data.get('ident')
        type_store = data.get('type')
        name_store = data.get('name')
        elevation_store = data.get('elevation_ft')
        continent_store = data.get('continent')
        iso_country_store = data.get('iso_country')
        iso_region_store = data.get('iso_region')
        municipality_store = data.get('municipality')
        gps_code_store = data.get('gps_code')
        iata_code_store = data.get('iata_code')
        local_code_store = data.get('local_code')
        coordinates_store = data.get('coordinates')
        query = Data(ident=ident_store, type=type_store,
                     name=name_store, elevation_ft=elevation_store,
                     continent=continent_store, iso_country=iso_country_store,
                     iso_region=iso_region_store, municipality=municipality_store,
                     gps_code=gps_code_store, iata_code=iata_code_store,
                     local_code=local_code_store, coordinates=coordinates_store)
        insert.delay(ident_store, type_store, name_store, elevation_store, continent_store, iso_country_store,
                     iso_region_store, municipality_store, gps_code_store, iata_code_store, local_code_store, coordinates_store)
        return query


@celery.task(name='celery_async.insert')
def insert(ident_store, type_store, name_store, elevation_store, continent_store, iso_country_store, iso_region_store, municipality_store, gps_code_store, iata_code_store, local_code_store, coordinates_store):
    query = Data(ident=ident_store, type=type_store,
                 name=name_store, elevation_ft=elevation_store,
                 continent=continent_store, iso_country=iso_country_store,
                 iso_region=iso_region_store, municipality=municipality_store,
                 gps_code=gps_code_store, iata_code=iata_code_store,
                 local_code=local_code_store, coordinates=coordinates_store)
    db.session.add(query)
    db.session.commit()
    print('Celery got async insert request!')


@api.route('/datas')
class Datas(Resource):
    @api.doc(description="Get all datas from database.")
    @api.marshal_list_with(data_model, code=200, envelope="datas")
    def get(self):
        ''' Get all datas from database '''
        datas = Data.query.all()
        return datas


@api.route('/data/<string:ident>')
class DataResource(Resource):
    @api.doc(description="Get a data by it's ident.",
             params={"ident": "ID of desired airport. Ex: 00A"})
    @api.marshal_with(data_model, code=200, envelope="data")
    def get(self, ident):
        ''' Get a data by ident '''
        data = Data.query.get_or_404(ident)

        return data, 200

    @api.doc(description="Update a data with it's ident.",
             params={"ident": "ID of desired airport. Ex: 00A"})
    @api.expect(data_model)
    def put(self, ident):
        ''' Update a data with it's 'ident' '''

        ident_toupdate = request.json['ident']
        type_toupdate = request.json['type']
        name_toupdate = request.json['name']
        elevation_ft_toupdate = request.json['elevation_ft']
        continent_toupdate = request.json['continent']
        iso_country_toupdate = request.json['iso_country']
        iso_region_toupdate = request.json['iso_region']
        municipality_toupdate = request.json['municipality']
        gps_code_toupdate = request.json['gps_code']
        iata_code_toupdate = request.json['iata_code']
        local_code_toupdate = request.json['local_code']
        coordinates_toupdate = request.json['coordinates']

        update.delay(ident, ident_toupdate, type_toupdate, name_toupdate, elevation_ft_toupdate, continent_toupdate, iso_country_toupdate,
                     iso_region_toupdate, municipality_toupdate, gps_code_toupdate, iata_code_toupdate, local_code_toupdate, coordinates_toupdate)

        return "Update request received.", 200

    @api.doc(description="Delete a data with it's 'ident'",
             params={"ident": "ID of desired airport. Ex: 00A"})
    def delete(self, ident):
        ''' Delete a data with it's 'ident' '''
        delete_data.delay(ident)
        return "Delete request received.", 200


@celery.task(name='celery_async.update')
def update(ident, ident_toupdate, type_toupdate, name_toupdate, elevation_ft_toupdate, continent_toupdate, iso_country_toupdate, iso_region_toupdate, municipality_toupdate, gps_code_toupdate, iata_code_toupdate, local_code_toupdate, coordinates_toupdate):
    data_to_update = Data.query.get_or_404(ident)
    data_to_update.ident = ident_toupdate
    data_to_update.type = type_toupdate
    data_to_update.name = name_toupdate
    data_to_update.elevation_ft = elevation_ft_toupdate
    data_to_update.continent = continent_toupdate
    data_to_update.iso_country = iso_country_toupdate
    data_to_update.iso_region = iso_region_toupdate
    data_to_update.municipality = municipality_toupdate
    data_to_update.gps_code = gps_code_toupdate
    data_to_update.iata_code = iata_code_toupdate
    data_to_update.local_code = local_code_toupdate
    data_to_update.coordinates = coordinates_toupdate
    db.session.commit()


@celery.task(name='celery_async.delete')
def delete_data(ident):
    data_to_delete = Data.query.get_or_404(ident)
    db.session.delete(data_to_delete)
    db.session.commit()


@app.shell_context_processor
def make_shell_context():
    return {
        'db': db,
        'Data': Data
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
