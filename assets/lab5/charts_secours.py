# -*- coding: utf-8 -*-
"""Lab 5 : solution de secours si Atlas Charts n'est pas disponible.

Construit trois graphiques de votre filière avec des pipelines d'agrégation (PyMongo) et matplotlib.

Usage, depuis la racine du dépôt, dans le terminal du Codespace :
    pip install pymongo matplotlib
    export MONGO_URI='mongodb+srv://<utilisateur>:<mot_de_passe>@<cluster>/'
    python3 assets/lab5/charts_secours.py IDF      # ou 2SCL, ou BIA
Sortie : tableau_de_bord_<filière>.png, à ouvrir dans l'explorateur de fichiers du Codespace.
2SCL et BI&A : la collection kpi_lab5 doit exister (section 6, pipeline terminé par $out).
"""
import os
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pymongo import MongoClient


def barres(ax, lignes, x, y, titre, horizontal=False):
    noms = [str(d[x]) for d in lignes]
    vals = [d[y] for d in lignes]
    if horizontal:
        ax.barh(noms[::-1], vals[::-1])
    else:
        ax.bar(noms, vals)
        ax.tick_params(axis='x', rotation=30)
    ax.set_title(titre)


def idf(db, axes):
    t = db.transactions
    barres(axes[0], list(t.aggregate([
        {'$match': {'Fraud Flag': 'True'}},
        {'$group': {'_id': '$Device Used', 'nb': {'$sum': 1}}}, {'$sort': {'_id': 1}}])),
        '_id', 'nb', 'Fraudes par appareil')
    barres(axes[1], list(t.aggregate([
        {'$match': {'Fraud Flag': 'True'}},
        {'$group': {'_id': '$Transaction Type', 'montant': {'$sum': '$Transaction Amount'}}},
        {'$sort': {'_id': 1}}])),
        '_id', 'montant', 'Montant frauduleux par type')
    par_min = list(t.aggregate([
        {'$group': {'_id': {'$dateTrunc': {'date': '$Timestamp', 'unit': 'minute'}}, 'nb': {'$sum': 1}}},
        {'$sort': {'_id': 1}}]))
    axes[2].plot([d['_id'] for d in par_min], [d['nb'] for d in par_min])
    axes[2].set_title('Transactions par minute')
    axes[2].tick_params(axis='x', rotation=30)


def scl(db, axes):
    barres(axes[0], sorted(db.kpi_lab5.find(), key=lambda d: d['_id']), '_id', 'taux_retard',
           'Taux de retard par transporteur (kpi_lab5)')
    barres(axes[1], list(db.scans.aggregate([
        {'$group': {'_id': '$site', 'nb': {'$sum': 1}}}, {'$sort': {'nb': -1}}])),
        '_id', 'nb', 'Scans par site')
    par_jour = list(db.scans.aggregate([
        {'$group': {'_id': {'$dateTrunc': {'date': '$ts', 'unit': 'day'}}, 'nb': {'$sum': 1}}},
        {'$sort': {'_id': 1}}]))
    axes[2].plot([d['_id'] for d in par_jour], [d['nb'] for d in par_jour])
    axes[2].set_title('Scans par jour')
    axes[2].tick_params(axis='x', rotation=30)


def bia(db, axes):
    barres(axes[0], sorted(db.kpi_lab5.find(), key=lambda d: d['_id']), '_id', 'ca',
           'CA des 5 premières catégories (kpi_lab5)', horizontal=True)
    barres(axes[1], list(db.ventes.aggregate([
        {'$group': {'_id': '$paiement', 'ca': {'$sum': '$montant'}}}, {'$sort': {'ca': -1}}])),
        '_id', 'ca', 'CA par moyen de paiement')
    par_jour = list(db.ventes.aggregate([
        {'$group': {'_id': {'$dateTrunc': {'date': '$ts', 'unit': 'day'}}, 'ca': {'$sum': '$montant'}}},
        {'$sort': {'_id': 1}}]))
    axes[2].plot([d['_id'] for d in par_jour], [d['ca'] for d in par_jour])
    axes[2].set_title('CA par jour')
    axes[2].tick_params(axis='x', rotation=30)


if __name__ == '__main__':
    filiere = (sys.argv[1] if len(sys.argv) > 1 else '').upper().replace('&', '')
    if filiere not in ('IDF', '2SCL', 'BIA') or 'MONGO_URI' not in os.environ:
        sys.exit("Usage : export MONGO_URI='...' puis python3 assets/lab5/charts_secours.py IDF|2SCL|BIA")
    db = MongoClient(os.environ['MONGO_URI'])['lab5']
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.8))
    {'IDF': idf, '2SCL': scl, 'BIA': bia}[filiere](db, axes)
    fig.tight_layout()
    sortie = 'tableau_de_bord_%s.png' % filiere
    fig.savefig(sortie, dpi=110)
    print('Graphiques enregistrés dans', sortie)
