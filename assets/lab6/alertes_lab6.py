#!/usr/bin/env python3
"""Lab 6 — Spark Structured Streaming : du topic Kafka aux alertes MongoDB.

Usage (terminal cluster-master, depuis /shared_volume/lab6) :
  spark-submit --master "local[2]" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
    alertes_lab6.py FILIERE ETAPE

FILIERE : IDF, 2SCL ou BIA.
ETAPE   : lecture  -> affiche les événements lus dans Kafka (section 4)
          alertes  -> affiche les alertes calculées par votre règle (section 5)
          mongo    -> écrit les alertes dans MongoDB, collection lab6.alertes (section 6)
Arrêt : Ctrl + C.
"""
import os
import sys

from pyspark.sql import SparkSession, functions as F, types as T

TOPIC = {'IDF': 'idf-transactions', '2SCL': 'scl-scans', 'BIA': 'bia-ventes'}

SCHEMA = {
    'IDF': T.StructType([
        T.StructField('transaction_id', T.StringType()), T.StructField('client_id', T.StringType()),
        T.StructField('country', T.StringType()), T.StructField('amount', T.DoubleType()),
        T.StructField('transaction_type', T.StringType()), T.StructField('risk_score', T.IntegerType()),
        T.StructField('ts', T.StringType())]),
    '2SCL': T.StructType([
        T.StructField('colis', T.StringType()), T.StructField('etape', T.StringType()),
        T.StructField('site', T.StringType()), T.StructField('ts', T.StringType())]),
    'BIA': T.StructType([
        T.StructField('store', T.StringType()), T.StructField('category', T.StringType()),
        T.StructField('amount', T.DoubleType()), T.StructField('payment', T.StringType()),
        T.StructField('ts', T.StringType())]),
}


# ======================================================================
#  À VOUS DE JOUER (section 5) : complétez les ___ de la règle de VOTRE filière
# ======================================================================

def regle_idf(ev):
    """Fraude probable : montant supérieur à 20 000 ET score de risque d'au moins 80 (sans état)."""
    alertes = ev.filter((F.col('amount') > ___) & (F.col('risk_score') >= ___))
    return alertes.select(
        F.col('client_id').alias('entite'), F.lit('fraude_probable').alias('type'), 'ts',
        (F.col('risk_score') / 100).alias('score'),
        F.col('transaction_id').alias('cle'),
        F.format_string('transaction %s : %.2f, risque %d', 'transaction_id', 'amount', 'risk_score').alias('details'))


def regle_scl(ev):
    """Retard : au moins 2 échecs de livraison d'un même colis dans une fenêtre d'une minute (avec état)."""
    echecs = ev.filter(F.col('etape') == ___)
    fen = (echecs.withWatermark('ts', '30 seconds')
           .groupBy(F.window('ts', '1 minute'), 'colis')
           .agg(F.count('*').alias('nb_echecs')))
    alertes = fen.filter(F.col('nb_echecs') >= ___)
    return alertes.select(
        F.col('colis').alias('entite'), F.lit('retard').alias('type'), F.col('window.end').alias('ts'),
        F.least(F.lit(1.0), F.col('nb_echecs') / 3).alias('score'),
        F.concat_ws('|', 'colis', F.col('window.start').cast('string')).alias('cle'),
        F.format_string('%d échecs de livraison', 'nb_echecs').alias('details'))


def regle_bia(ev):
    """Baisse du CA : chiffre d'affaires d'un magasin inférieur à 2 000 sur une fenêtre d'une minute (avec état)."""
    fen = (ev.withWatermark('ts', '30 seconds')
           .groupBy(F.window('ts', '1 minute'), 'store')
           .agg(F.sum(___).alias('ca'), F.count('*').alias('nb_ventes')))
    alertes = fen.filter(F.col('ca') < ___)
    return alertes.select(
        F.col('store').alias('entite'), F.lit('baisse_ca').alias('type'), F.col('window.end').alias('ts'),
        F.greatest(F.lit(0.0), 1 - F.col('ca') / 2000).alias('score'),
        F.concat_ws('|', 'store', F.col('window.start').cast('string')).alias('cle'),
        F.format_string('CA %.2f sur %d ventes', 'ca', 'nb_ventes').alias('details'))


REGLE = {'IDF': regle_idf, '2SCL': regle_scl, 'BIA': regle_bia}
# ======================================================================


def ecrire_mongo(lot, num_lot):
    """Écrit un micro-lot d'alertes dans MongoDB. Upsert sur _id : réécrire une alerte ne crée pas de doublon."""
    from pymongo import MongoClient
    lignes = lot.collect()
    if not lignes:
        return
    coll = MongoClient(os.environ['MONGO_URI'])['lab6']['alertes']
    for r in lignes:
        doc = {'_id': r['cle'], 'entite': r['entite'], 'type': r['type'], 'ts': r['ts'],
               'score': float(r['score']), 'statut': 'nouvelle', 'details': r['details']}
        coll.replace_one({'_id': doc['_id']}, doc, upsert=True)
    print('micro-lot %d : %d alerte(s) écrite(s) dans lab6.alertes' % (num_lot, len(lignes)))


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in TOPIC or sys.argv[2] not in ('lecture', 'alertes', 'mongo'):
        sys.exit(__doc__)
    filiere, etape = sys.argv[1], sys.argv[2]
    if etape == 'mongo' and 'MONGO_URI' not in os.environ:
        sys.exit('Définissez d\'abord MONGO_URI (section 0).')

    spark = SparkSession.builder.appName('lab6-%s-%s' % (filiere, etape)).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    brut = (spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers', 'localhost:9092')
            .option('subscribe', TOPIC[filiere])
            .option('startingOffsets', 'earliest')
            .load())
    ev = (brut.select('partition', 'offset',
                      F.from_json(F.col('value').cast('string'), SCHEMA[filiere]).alias('e'))
          .select('partition', 'offset', 'e.*')
          .withColumn('ts', F.to_timestamp('ts')))

    chk = 'file:///tmp/lab6-chk-%s-%s' % (filiere, etape)
    if etape == 'lecture':
        q = ev.writeStream.format('console').option('truncate', False).option('checkpointLocation', chk).start()
    elif etape == 'alertes':
        q = (REGLE[filiere](ev).writeStream.format('console').option('truncate', False)
             .option('checkpointLocation', chk).start())
    else:
        q = REGLE[filiere](ev).writeStream.foreachBatch(ecrire_mongo).option('checkpointLocation', chk).start()
    q.awaitTermination()


if __name__ == '__main__':
    main()
