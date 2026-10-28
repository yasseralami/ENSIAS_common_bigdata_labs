#!/usr/bin/env python3
"""Lab 6 — collection des alertes dans MongoDB Atlas.

Usage (terminal cluster-master, variable MONGO_URI définie) :
  python3 init_alertes.py            # crée lab6.alertes avec le validateur $jsonSchema du Lab 5 (si absente)
  python3 init_alertes.py --compter  # nombre d'alertes par type et trois dernières alertes
"""
import os
import sys

from pymongo import MongoClient, DESCENDING

VALIDATEUR = {'$jsonSchema': {
    'bsonType': 'object',
    'required': ['entite', 'type', 'ts', 'statut'],
    'properties': {
        'entite': {'bsonType': 'string'},
        'type': {'bsonType': 'string'},
        'ts': {'bsonType': 'date'},
        'score': {'bsonType': ['double', 'int'], 'minimum': 0, 'maximum': 1},
        'statut': {'enum': ['nouvelle', 'en cours', 'traitée']}}}}


def main():
    if 'MONGO_URI' not in os.environ:
        sys.exit('Définissez d\'abord MONGO_URI (section 0).')
    db = MongoClient(os.environ['MONGO_URI'])['lab6']
    if '--compter' in sys.argv:
        for r in db.alertes.aggregate([{'$group': {'_id': '$type', 'n': {'$sum': 1}}}]):
            print('%-16s %d' % (r['_id'], r['n']))
        for d in db.alertes.find().sort('ts', DESCENDING).limit(3):
            print(d)
        return
    if 'alertes' in db.list_collection_names():
        print('La collection lab6.alertes existe déjà : elle est conservée.')
    else:
        db.create_collection('alertes', validator=VALIDATEUR)
        print('Collection lab6.alertes créée avec son validateur $jsonSchema.')


if __name__ == '__main__':
    main()
