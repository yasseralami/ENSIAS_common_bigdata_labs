# -*- coding: utf-8 -*-
"""Lab 5 : charge le jeu de données de votre filière dans MongoDB Atlas (base lab5).

Usage, depuis la racine du dépôt, dans le terminal du Codespace :
    export MONGO_URI='mongodb+srv://<utilisateur>:<mot_de_passe>@<cluster>/'
    python3 assets/lab5/import_lab5.py IDF      # ou 2SCL, ou BIA

Le script est idempotent : il vide la collection avant de la recharger.
Le mot de passe reste dans la variable MONGO_URI : ne l'écrivez jamais dans un fichier du dépôt.
"""
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient

ASSETS = Path(__file__).resolve().parent.parent


def charger(coll, docs):
    coll.drop()
    if docs:
        coll.insert_many(docs)
    print('%-12s %6d documents' % (coll.name, coll.count_documents({})))


def idf(db):
    docs = json.load(open(ASSETS / 'mongodb' / 'transactions.json', encoding='utf8'))
    for d in docs:
        d['Timestamp'] = datetime.strptime(d['Timestamp'], '%Y-%m-%d %H:%M:%S')
        d.pop('PIN Code', None)  # donnée sensible : jamais chargée (minimisation)
    charger(db.transactions, docs)


def scl(db):
    scans = [json.loads(l) for l in open(ASSETS / 'lab3' / 'scl_scans_clean.json', encoding='utf8') if l.strip()]
    for s in scans:
        s['ts'] = datetime.fromisoformat(s['ts'])
    charger(db.scans, scans)
    ref = list(csv.DictReader(open(ASSETS / 'lab3' / 'scl_colis_ref_clean.csv', encoding='utf8')))
    for r in ref:
        r['_id'] = r.pop('colis_id')
        r['promesse_h'] = int(r['promesse_h'])
    charger(db.colis_ref, ref)


def bia(db):
    ventes = []
    for ligne in open(ASSETS / 'lab5' / 'bia_ventes_lab5.txt', encoding='utf8'):
        date, heure, magasin, categorie, montant, paiement = ligne.rstrip('\n').split('\t')
        ventes.append({'ts': datetime.strptime(date + ' ' + heure, '%Y-%m-%d %H:%M'),
                       'magasin': magasin, 'categorie': categorie,
                       'montant': float(montant), 'paiement': paiement})
    charger(db.ventes, ventes)


if __name__ == '__main__':
    filiere = (sys.argv[1] if len(sys.argv) > 1 else '').upper().replace('&', '')
    if filiere not in ('IDF', '2SCL', 'BIA') or 'MONGO_URI' not in os.environ:
        sys.exit("Usage : export MONGO_URI='...' puis python3 assets/lab5/import_lab5.py IDF|2SCL|BIA")
    client = MongoClient(os.environ['MONGO_URI'])
    client.admin.command('ping')
    db = client['lab5']
    {'IDF': idf, '2SCL': scl, 'BIA': bia}[filiere](db)
    print('Import terminé dans la base lab5.')
