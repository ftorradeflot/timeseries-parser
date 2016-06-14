#!/usr/envs/eyecode/bin/python

# PRODUCTION DATABASE SERVER
SERVER_IP = '172.31.34.147' # Postgres, Cassandra
SECOND_IP = '192.168.192.15' # Redis

# POSTGRES
class Postgres():
    USER = 'postgres'
    PASSWORD = 'unicorn'
    IP = 'localhost' 
    PORT = '5432'
    IP_PORT = IP+':'+PORT
#    FOLDER = 'stage_eyecode'
    FOLDER = 'eyecode'
    PATH = USER+':'+PASSWORD+'@'+IP+':'+PORT+'/'+FOLDER


class Postgres_arsys():
    USER = 'postgres'
    PASSWORD = 'unicorn'
    IP = 'localhost' 
    PORT = '5432'
    IP_PORT = IP+':'+PORT
#    FOLDER = 'stage_eyecode'
    FOLDER = 'eyecode_arsys'
    PATH = USER+':'+PASSWORD+'@'+IP+':'+PORT+'/'+FOLDER

# CASSANDRA
class Cassandra():
    IP = 'localhost'
    PORT = '9160'
    IP_PORT = IP+':'+PORT
    KEYSPACE = 'eyecode'

# REDIS
class Redis():
    IP = SECOND_IP
    PORT = '6379'
    IP_PORT = IP+':'+PORT

# User and Group
class UNIX():
    USER = "git"
    GROUP = "www-data"
    EYECODE_ROOT = "/usr/eyecode/"

#OPCS
class OPC():
    PORTS = {'7766'}

#ARAG
class ARAG():
    HUB_IP = '10.8.0.10'
    ###HUB_NAME = 'NETx.VOYAGER.SERVER.2.0'
    HUB_NAME = 'Kepware.KEPServerEX.V5'
