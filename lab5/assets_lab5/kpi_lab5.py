# -*- coding: utf-8 -*-
"""Lab 5 (bonus) : le second pipeline d'agrégation de votre filière, exécuté depuis Python avec PyMongo.

Remplace l'ancien map_reduce(), déprécié depuis MongoDB 5.0 : un pipeline est une liste de dictionnaires.
Usage, depuis la racine du dépôt (après l'import) :
    export MONGO_URI='mongodb+srv://<utilisateur>:<mot_de_passe>@<cluster>/'
    python3 assets/lab5/kpi_lab5.py IDF      # ou 2SCL, ou BIA
"""
import os
import sys

from pymongo import MongoClient

# BI&A : recopiez ici votre TODO 3 de la section 4 (l'heure de la vente), en syntaxe Python :
# un dictionnaire, avec le nom de l'opérateur entre guillemets.
HEURE = None

PIPELINES = {
    # Comptes émetteurs au plus gros montant de transactions frauduleuses réussies
    'IDF': ('transactions', [
        {'$match': {'Fraud Flag': 'True', 'Transaction Status': 'Success'}},
        {'$group': {'_id': '$Sender Account ID',
                    'montant': {'$sum': '$Transaction Amount'}, 'nb': {'$sum': 1}}},
        {'$sort': {'montant': -1}},
        {'$limit': 5},
    ]),
    # Colis non livrés, par transporteur et dernier site connu (collection colis de la section 3)
    '2SCL': ('colis', [
        {'$match': {'evenements.etape': {'$ne': 'LIVRE'}}},
        {'$project': {'transporteur': 1, 'derniere': {'$last': '$evenements'}}},
        {'$group': {'_id': {'transporteur': '$transporteur', 'site': '$derniere.site'},
                    'nb_en_attente': {'$sum': 1}}},
        {'$sort': {'nb_en_attente': -1, '_id': 1}},
        {'$limit': 5},
    ]),
    # Chiffre d'affaires selon l'heure de la vente
    'BIA': ('ventes', [
        {'$group': {'_id': HEURE, 'ca': {'$sum': '$montant'}, 'nb': {'$sum': 1}}},
        {'$sort': {'_id': 1}},
    ]),
}

if __name__ == '__main__':
    filiere = (sys.argv[1] if len(sys.argv) > 1 else '').upper().replace('&', '')
    if filiere not in PIPELINES or 'MONGO_URI' not in os.environ:
        sys.exit("Usage : export MONGO_URI='...' puis python3 assets/lab5/kpi_lab5.py IDF|2SCL|BIA")
    db = MongoClient(os.environ['MONGO_URI'])['lab5']
    nom, pipeline = PIPELINES[filiere]
    for doc in db[nom].aggregate(pipeline):
        print(doc)
