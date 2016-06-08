#!/usr/bin/python
'''
Realiza dump do MongoDB
'''
__author__ = 'Ernani de Britto Murtinho'
__version__ = '1.0.0'
__version_author__ = 'Ernani de Britto Murtinho'
__date__ = '19 may 2016'

import boto.utils
from boto import ec2
from boto.ec2 import connect_to_region
from pymongo import collection as MongoCollection
from pymongo import MongoClient
from datetime import datetime
import logging
import os
import socket
import subprocess
import ConfigParser
import argparse
import logging
import datetime
#import dbparse
import subprocess

def main():
  configurar_logs()
  logging.info('Iniciando Script de Dump MongoDB')

  ##Configurando
  date_now=datetime.now()
  config = ConfigParser.RawConfigParser()
  config.read('/zap/scripts/snapshot_mongo.cfg')
  aws_key=config.get('aws', 'key')
  aws_secret=config.get('aws', 'secret')
  aws_region=config.get('aws', 'region')
  mongo_uri=config.get('mongo', 'uri')
  mongo_user=config.get('mongoDump', 'user')
  mongo_pwd=config.get('mongoDump', 'pwd')
  mongo_authDB=config.get('mongoDump', 'authDB')

  ##Conexoes
  client=conn_mongo(mongo_uri)

  #Checa se servidor eh primario
  print "checando primario \n"
  check_primary(client)

def conn_mongo(mongo_uri):
  uri = mongo_uri
  client = MongoClient(uri)
  return client

def configurar_logs():
  logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/var/log/dump_mongo.log',
                    filemode='a')

  # Handler para console.
  ch = logging.StreamHandler()
  ch.setLevel(logging.INFO)
  formatter = logging.Formatter('%(asctime)s %(message)s')
  ch.setFormatter(formatter)
  logging.getLogger('').addHandler(ch)

def check_primary(client):
        if client.is_primary == True:
                logging.info('Servidor eh primary - saindo do programa')
                quit()
        logging.info('Servidor nao eh primary - seguindo programa')


parser = argparse.ArgumentParser(description='Backup MongoDB DBs')
parser.add_argument('-d', '--db',
        type=str,
        required=False,
        default=None,
        help='MongoDB DB for Backups')
parser.add_argument('-o', '--output_dir',
        type=str,
        required=False,
        default='./',
        help='Output directory for the backup.')

def backup(args):
    today = datetime.datetime.now()

    db = args.db
    output_dir = args.output_dir
    config = ConfigParser.RawConfigParser()
    config.read('/zap/scripts/snapshot_mongo.cfg')
    mongo_user=config.get('mongoDump', 'user')
    mongo_pwd=config.get('mongoDump', 'pwd')
    mongo_authDB=config.get('mongoDump', 'authDB')

    if db is None:
        logging.info('DB nao informado')

    username = mongo_user
    password = mongo_pwd
    authenticationDatabase = mongo_authDB
#	hostname = os.getenv('HOSTNAME').split('.')[0]
    hostname = "nomeMaquina"
#    db = db.path[1:]
		
    output_dir = os.path.abspath(os.path.join(
            os.path.curdir,
            args.output_dir))

    assert os.path.isdir(output_dir), 'Directory %s can\'t be found.' % output_dir

    output_dir = os.path.abspath(os.path.join(output_dir,
            '%s_%s_%s'% ( db, hostname,today.strftime('%Y_%m_%d'))
            ))

    logging.info('Backing up %s from %s to %s' % (db, hostname, output_dir))

    backup_output = subprocess.check_output(
            [
                'mongodump',
                '-u', '%s' % username,
                '-p', '%s' % password,
                '-d', '%s' % db,
				'--authenticationDatabase', '%s' % mongo_authDB,
                '-o', '%s' % output_dir,
                '--gzip'
            ])

    logging.info(backup_output)


## if __name__ == "__main__":
##   main()
  
if __name__ == '__main__':
    args = parser.parse_args()
   
    try:
        config = ConfigParser.RawConfigParser()
        config.read('/zap/scripts/snapshot_mongo.cfg')
        mongo_uri=config.get('mongo', 'uri')
        client=conn_mongo(mongo_uri)
        check_primary(client)
        backup(args)
    except AssertionError, msg:
        logging.error(msg)
