#!/usr/bin/env python3
"""Lab 6 — simulateur d'événements : publie un flux continu dans Kafka.

Usage (terminal cluster-master, depuis /shared_volume/lab6) :
  python3 simulateur_lab6.py IDF     # transactions bancaires  -> topic idf-transactions
  python3 simulateur_lab6.py 2SCL    # scans de colis          -> topic scl-scans
  python3 simulateur_lab6.py BIA     # ventes des magasins     -> topic bia-ventes
Options : --debit N (événements par seconde, 5 par défaut), --duree S (secondes, 600 par défaut).
Arrêt : Ctrl + C.

Les événements sont tirés avec une graine fixe : mêmes valeurs à chaque exécution, horodatées à
l'instant de l'envoi. Le flux démarre au début de la minute suivante (fenêtres d'une minute alignées).
"""
import argparse
import json
import random
import time
import warnings
from datetime import datetime

warnings.filterwarnings('ignore', category=DeprecationWarning)   # avertissements de kafka-python
from kafka import KafkaProducer

TOPIC = {'IDF': 'idf-transactions', '2SCL': 'scl-scans', 'BIA': 'bia-ventes'}
CLE = {'IDF': 'client_id', '2SCL': 'colis', 'BIA': 'store'}
rng = random.Random(6)


def maintenant():
    return datetime.now().isoformat(timespec='seconds')


# ---------------------------------------------------------------- IDF : transactions
PAYS = ['Morocco', 'France', 'Spain', 'UAE', 'UK', 'Germany', 'USA', 'Turkey', 'China']
TYPES = ['Card', 'Online', 'POS', 'Transfer', 'ATM']
num_tx = [0]
lancement = datetime.now().strftime('%d%H%M')   # identifiants uniques d'une exécution à l'autre


def evenement_idf():
    num_tx[0] += 1
    tirage = rng.random()
    if tirage < 0.03:                       # fraude : montant élevé ET score de risque élevé
        montant, risque = rng.uniform(20000, 50000), rng.randint(80, 99)
    elif tirage < 0.08:                     # gros montant, client sûr
        montant, risque = rng.uniform(20000, 40000), rng.randint(5, 50)
    elif tirage < 0.13:                     # petit montant, score élevé
        montant, risque = rng.uniform(50, 2000), rng.randint(80, 95)
    else:
        montant, risque = rng.uniform(50, 15000), rng.randint(0, 79)
    return {'transaction_id': 'T%s-%05d' % (lancement, num_tx[0]), 'client_id': 'C%03d' % rng.randint(100, 399),
            'country': rng.choice(PAYS), 'amount': round(montant, 2), 'transaction_type': rng.choice(TYPES),
            'risk_score': risque, 'ts': maintenant()}


# ---------------------------------------------------------------- 2SCL : scans de colis
SITES = ['Casablanca', 'Rabat', 'Tanger Med', 'Marrakech', 'Fès', 'Agadir', 'Oujda', 'Kénitra']
ETAPES = ['pris en charge', 'entrepôt', 'en livraison', 'livré']
colis_actifs = []                            # [identifiant, indice d'étape, échecs restants, site]
num_colis = [10000]


def evenement_scl():
    if len(colis_actifs) < 40 or rng.random() < 0.25:
        num_colis[0] += 1
        echecs = 3 if rng.random() < 0.08 else (1 if rng.random() < 0.10 else 0)
        colis_actifs.append(['CL-%05d' % num_colis[0], 0, echecs, rng.choice(SITES)])
        c = colis_actifs[-1]
        return {'colis': c[0], 'etape': ETAPES[0], 'site': c[3], 'ts': maintenant()}
    c = rng.choice(colis_actifs)
    if ETAPES[c[1]] == 'en livraison' and c[2] > 0:   # tentative de livraison échouée
        c[2] -= 1
        return {'colis': c[0], 'etape': 'échec livraison', 'site': c[3], 'ts': maintenant()}
    c[1] += 1
    ev = {'colis': c[0], 'etape': ETAPES[c[1]], 'site': c[3], 'ts': maintenant()}
    if c[1] == len(ETAPES) - 1:
        colis_actifs.remove(c)
    return ev


# ---------------------------------------------------------------- BI&A : ventes
MAGASINS = ['San Jose', 'Raleigh', 'San Antonio', 'Garland', 'Omaha', 'Boston', 'Fort Worth', 'Miami', 'Austin', 'Seattle']
CATEGORIES = ['Computers', 'Cameras', 'Music', 'Toys', 'Books', 'Sporting Goods', "Children's Clothing", 'Garden']
PAIEMENTS = ['Visa', 'MasterCard', 'Amex', 'Cash', 'Discover']
debut = [None]


def evenement_bia():
    magasin = rng.choice(MAGASINS)
    montant = rng.uniform(5, 500)
    # après 2 minutes, les ventes de San Jose s'effondrent (panne de caisse simulée)
    if magasin == 'San Jose' and time.time() - debut[0] > 120:
        if rng.random() < 0.8:
            magasin = rng.choice(MAGASINS[1:])
        else:
            montant = rng.uniform(5, 60)
    return {'store': magasin, 'category': rng.choice(CATEGORIES), 'amount': round(montant, 2),
            'payment': rng.choice(PAIEMENTS), 'ts': maintenant()}


GEN = {'IDF': evenement_idf, '2SCL': evenement_scl, 'BIA': evenement_bia}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('filiere', choices=['IDF', '2SCL', 'BIA'])
    p.add_argument('--debit', type=float, default=5)
    p.add_argument('--duree', type=int, default=600)
    a = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
    )
    topic, cle = TOPIC[a.filiere], CLE[a.filiere]
    attente = 60 - datetime.now().second
    print('Topic %s, clé = %s, %g événements/s. Démarrage dans %d s (début de la minute suivante).'
          % (topic, cle, a.debit, attente))
    time.sleep(attente)
    debut[0] = time.time()
    n = 0
    try:
        while time.time() - debut[0] < a.duree:
            ev = GEN[a.filiere]()
            meta = producer.send(topic, key=ev[cle], value=ev).get(timeout=10)
            n += 1
            if n % 20 == 0:
                print('%5d événements | dernier : clé %-12s -> partition %d, offset %d'
                      % (n, ev[cle], meta.partition, meta.offset))
            time.sleep(1 / a.debit)
    except KeyboardInterrupt:
        print('\nArrêt demandé.')
    finally:
        producer.flush()
        producer.close()
        print('%d événements publiés dans %s.' % (n, topic))


if __name__ == '__main__':
    main()
