#!/usr/bin/env python3
"""
Producer Kafka — Simulateur de transactions bancaires
CHEMIN: /shared_volume/kafka/producer.py
CLUSTER: cluster-master
TARGET: 500 tx/sec avec délai visible
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=['cluster-master:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
)

# Données de simulation
COUNTRIES = ['MA', 'FR', 'US', 'UK', 'IR', 'KP', 'CH', 'SY', 'SOM']
MERCHANTS = ['Amazon', 'Carrefour', 'Shell', 'SPA', 'Jumia', 'Cora', 'MARJANE', 'BNP Paribas']
CARD_TYPES = ['VISA', 'MASTERCARD', 'AMEX', 'DISCOVER']

def generate_transaction():
    """Génère une transaction bancaire simulée."""
    return {
        'tx_id': f"TXN_{int(time.time()*1000)}_{random.randint(1000, 9999)}",
        'amount': round(random.uniform(100, 50000), 2),
        'country': random.choice(COUNTRIES),
        'merchant': random.choice(MERCHANTS),
        'card_type': random.choice(CARD_TYPES),
        'timestamp': datetime.utcnow().isoformat(),
        'is_fraud': random.random() < 0.02,
        'cardholder_age': random.randint(18, 75),
    }

def send_transactions(rate_per_sec=500, duration_minutes=10):
    """Envoie des transactions à 500 tx/sec avec délai visible."""
    batch_size = 50
    delay_between_batches = batch_size / rate_per_sec
    
    print(f"\n{'='*80}")
    print(f"🚀 PRODUCER KAFKA DÉMARRÉ")
    print(f"{'='*80}")
    print(f"📁 Chemin: /shared_volume/kafka/producer.py")
    print(f"📊 Target: {rate_per_sec} tx/sec")
    print(f"📍 Bootstrap: cluster-master:9092")
    print(f"📋 Topic: transactions")
    print(f"⏳ Délai entre batches: {delay_between_batches*1000:.0f}ms")
    print(f"⏹️  Appuyez sur Ctrl+C pour arrêter")
    print(f"{'='*80}\n")
    
    try:
        tx_count = 0
        start_time = time.time()
        
        while True:
            batch_start = time.time()
            
            # Envoyer 50 transactions (un batch)
            for i in range(batch_size):
                tx = generate_transaction()
                future = producer.send('transactions', value=tx)
                
                def on_error(exc):
                    print(f"❌ Erreur: {exc}")
                future.add_errback(on_error)
                tx_count += 1
                
                # Afficher chaque 10 transactions
                if (tx_count % 10) == 0:
                    print(f"  📤 {tx_count} tx | {tx['tx_id'][:20]}... | {tx['amount']:>8.2f} MAD | {tx['country']}")
            
            # Attendre pour maintenir 500 tx/sec
            elapsed_since_batch = time.time() - batch_start
            target_delay = delay_between_batches
            
            if elapsed_since_batch < target_delay:
                sleep_time = target_delay - elapsed_since_batch
                # Afficher la pause (pour montrer le streaming)
                print(f"⏸️  Pause de {sleep_time*1000:.0f}ms avant le prochain batch...")
                time.sleep(sleep_time)
            
            # Affichage du status tous les 500 messages
            if tx_count % 500 == 0:
                elapsed_total = time.time() - start_time
                actual_rate = tx_count / elapsed_total if elapsed_total > 0 else 0
                print(f"\n{'─'*80}")
                print(f"✅ CHECKPOINT | {tx_count:>5} tx envoyées | {elapsed_total:>6.1f}s | Débit réel: {actual_rate:>6.0f} tx/sec")
                print(f"{'─'*80}\n")
            
            # STOP après 10 minutes de test
            if (time.time() - start_time) > (duration_minutes * 60):
                print(f"\n⏰ Durée limite atteinte ({duration_minutes} min)")
                break
                
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Arrêt manuel")
    finally:
        elapsed_total = time.time() - start_time
        print(f"\n{'='*80}")
        print(f"📊 RÉSUMÉ FINAL")
        print(f"{'='*80}")
        print(f"├── Total transactions: {tx_count}")
        print(f"├── Durée: {elapsed_total:.1f}s")
        print(f"├── Débit moyen: {tx_count / elapsed_total:.0f} tx/sec")
        print(f"└── Chemin: /shared_volume/kafka/producer.py")
        print(f"{'='*80}\n")
        
        producer.flush()
        producer.close()
        print("✅ Producer fermé proprement\n")

if __name__ == '__main__':
    send_transactions(rate_per_sec=500, duration_minutes=10)
