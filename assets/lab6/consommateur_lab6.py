#!/usr/bin/env python3
"""Lab 6 — consommateur Kafka : effet du moment de la validation des offsets (commit).

Usage (terminal cluster-master, depuis /shared_volume/lab6) :
  python3 consommateur_lab6.py TOPIC --groupe G --commit apres   # valide après le traitement d'un lot
  python3 consommateur_lab6.py TOPIC --groupe G --commit avant   # valide avant le traitement d'un lot
  python3 consommateur_lab6.py TOPIC --groupe G --bilan           # doublons et trous du groupe G

Le consommateur lit des lots de 10 enregistrements et « traite » chacun en 0,3 s : il note la
partition et l'offset dans traites_<G>.txt. Ctrl + C simule une panne : aucun commit n'est fait.
"""
import argparse
import os
import time
import warnings
from collections import Counter

warnings.filterwarnings('ignore', category=DeprecationWarning)   # avertissements de kafka-python
from kafka import KafkaConsumer


def journal(groupe):
    return 'traites_%s.txt' % groupe


def consommer(topic, groupe, mode):
    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092', group_id=groupe,
                             auto_offset_reset='earliest', enable_auto_commit=False)
    print('Groupe %s, commit %s le traitement. Ctrl + C pour simuler une panne.' % (groupe, 'après' if mode == 'apres' else 'avant'))
    try:
        with open(journal(groupe), 'a') as f:
            while True:
                lots = consumer.poll(timeout_ms=1000, max_records=10)
                if not lots:
                    continue
                if mode == 'avant':
                    consumer.commit()
                for tp, messages in lots.items():
                    for m in messages:
                        time.sleep(0.3)                        # traitement
                        f.write('%d,%d\n' % (m.partition, m.offset))
                        f.flush()
                        print('traité : partition %d, offset %d' % (m.partition, m.offset))
                if mode == 'apres':
                    consumer.commit()
                    print('-- commit')
    except KeyboardInterrupt:
        print('\nPanne simulée : arrêt sans commit.')
        consumer.close(autocommit=False)


def bilan(groupe):
    if not os.path.exists(journal(groupe)):
        print('Aucun traitement enregistré pour le groupe %s.' % groupe)
        return
    vus = Counter(tuple(map(int, l.split(','))) for l in open(journal(groupe)) if l.strip())
    doublons = sum(n - 1 for n in vus.values() if n > 1)
    trous = 0
    for p in sorted({p for p, _ in vus}):
        offs = [o for q, o in vus if q == p]
        manquants = set(range(min(offs), max(offs) + 1)) - set(offs)
        trous += len(manquants)
        print('partition %d : offsets %d à %d, %d traitement(s) en double, %d offset(s) jamais traité(s)'
              % (p, min(offs), max(offs), sum(vus[(p, o)] - 1 for o in offs), len(manquants)))
    print('Total : %d traitements, %d doublon(s), %d trou(s).' % (sum(vus.values()), doublons, trous))


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('topic')
    p.add_argument('--groupe', required=True)
    p.add_argument('--commit', choices=['avant', 'apres'], default='apres')
    p.add_argument('--bilan', action='store_true')
    a = p.parse_args()
    if a.bilan:
        bilan(a.groupe)
    else:
        consommer(a.topic, a.groupe, a.commit)
