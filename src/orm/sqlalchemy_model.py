#!/usr/envs/eyecode/bin/python
"""
.. module:: orm/sql_alchemy_model.py
	:platform: Unix, Windows
	:synopsis: Define the sql model and session
	.. moduleauthor:: Quim Castella <quim@nomorecode.com>
"""
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.dialects.postgresql import JSON, INET, UUID
import json
from common.server_info import Postgres 

engine = create_engine('postgresql://%s' % Postgres.PATH, echo=False)

Base = declarative_base(engine)

class Variable(Base):
    """ORM map of the SQL variables table #{{{ """
    __tablename__ = 'variables'

    id = Column(Integer,primary_key=True)

    id_device = Column(Integer, ForeignKey('devices.id')) #parent
    id_installer = Column(Integer, ForeignKey('installers.id')) #parent
    id_client = Column(Integer, ForeignKey('clients.id')) #parent
    id_location = Column(Integer, ForeignKey('locations.id')) #parent

    name = Column(String(48))
    actuator = Column(Boolean)
    id_cassandra = Column(UUID)
    timeseries_cassandra = Column(String(32))
    private_name = Column(String)
    propagation_group = Column(Integer)
    state_cassandra = Column(String(32))
    schedule = Column(Boolean)

    variable_type = Column(Integer)
    formula = Column(String)
    level = Column(Integer)

    info = Column(JSON)
    description = Column(String)

    deletion_date = Column(DateTime)

    last_value = Column(String)
    last_user = Column(Integer)
    last_update = Column(Integer)

    installer = relationship('Installer', backref='variables') #Variable.installer, Installer.variables
    client = relationship('Client', backref='variables') 
    location = relationship('Location', backref='variables') 

    def __repr__(self):
        return json.dumps(self.serialize)

    @property
    def serialize(self):
       output = {
                'id': self.id,
                'id_device':self.id_device,
                'id_installer':self.id_installer,
                'id_client':self.id_client,
                'id_location':self.id_location,
                'name':self.name,
                # 'propagation_group':self.propagation_group,
                'info':self.info,
                'actuator':self.actuator,
                'schedule':self.schedule,
                'last_value':self.last_value,
                'last_user':self.last_user,
                'last_update':self.last_update,
                'variable_type': self.variable_type,
                'formula': self.formula,
                'level': self.level
               # 'private_name':self.private_name,
               # 'id_cassandra':self.id_cassandra,
               # 'timeseries_cassandra':self.timeseries_cassandra,
               # 'state_cassandra':self.state_cassandra
       }
       if hasattr(self,'data'):
           output['data'] = self.data
       return output
#}}}


def load_session():
    """Session Maker: load metadata and return session #{{{
    """
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
#}}}


