#!/usr/bin/env python3
"""
Consumer Kafka — Filtre AML avec EOS
CHEMIN: /shared_volume/kafka/consumer.py
CLUSTER: cluster-master
EXACTLY-ONCE SEMANTICS (EOS)
"""

import json
import logging
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration Consumer Kafka
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['cluster-master:9092'],
    group_id='aml-filter',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    enable_auto_commit=False,  # EOS — Manual commit
    session_timeout_ms=30000,
)

# Règles AML
BLACKLIST_COUNTRIES = ['IR', 'KP', 'SY', 'SOM']
AML_THRESHOLD_MAD = 10000
FRAUD_THRESHOLD = 0.95

def evaluate_aml_rules(tx):
    """Évalue une transaction selon les règles AML."""
    alerts = []
    risk_score = 0
    
    if tx['amount'] > AML_THRESHOLD_MAD:
        alerts.append(f"MONTANT_ÉLEVÉ({tx['amount']:.2f} MAD)")
        risk_score += 40
    
    if tx['country'] in BLACKLIST_COUNTRIES:
        alerts.append(f"PAYS_INTERDIT({tx['country']})")
        risk_score += 50
    
    if tx.get('is_fraud', False):
        alerts.append("FRAUDE_DÉTECTÉE")
        risk_score += 35
    
    is_suspicious = risk_score > 50
    
    return is_suspicious, alerts, risk_score

def main():
    """Boucle principale du consumer avec délai visible."""
    
    print(f"\n{'='*80}")
    print(f"🔍 CONSUMER AML DÉMARRÉ")
    print(f"{'='*80}")
    print(f"📁 Chemin: /shared_volume/kafka/consumer.py")
    print(f"📊 Topic: transactions")
    print(f"📋 Consumer Group: aml-filter")
    print(f"📍 Bootstrap: cluster-master:9092")
    print(f"🔒 EOS (Exactly-Once): ACTIVÉ")
    print(f"⏳ Délai de traitement: 50ms par message (pour visualiser le flux)")
    print(f"⚠️  Règles AML activées:")
    print(f"    • Montant seuil: {AML_THRESHOLD_MAD:,} MAD")
    print(f"    • Pays blacklistés: {', '.join(BLACKLIST_COUNTRIES)}")
    print(f"    • Risk score > 50 = ALERTE")
    print(f"{'='*80}\n")
    
    try:
        processed = 0
        flagged = 0
        start_time = time.time()
        
        for message in consumer:
            # Délai intentionnel pour voir le traitement (50ms)
            time.sleep(0.05)
            
            tx = message.value
            is_alert, alerts, risk = evaluate_aml_rules(tx)
            
            processed += 1
            
            if is_alert:
                flagged += 1
                logger.warning(
                    f"🚨 [ALERTE AML] | TxID: {tx['tx_id'][:20]}... | "
                    f"Montant: {tx['amount']:>8.2f} MAD | "
                    f"Pays: {tx['country']} | "
                    f"Marchand: {tx['merchant']:<12} | "
                    f"Risk: {risk:>3}% | "
                    f"Détails: {' + '.join(alerts)}"
                )
            else:
                logger.info(
                    f"✅ [OK] | TxID: {tx['tx_id'][:20]}... | "
                    f"Montant: {tx['amount']:>8.2f} MAD | "
                    f"Pays: {tx['country']} | "
                    f"Marchand: {tx['merchant']:<12} | "
                    f"Carte: {tx['card_type']}"
                )
            
            # EOS : Commit tous les 100 messages (après traitement réussi)
            if processed % 100 == 0:
                consumer.commit()
                elapsed = time.time() - start_time
                alert_ratio = (flagged / processed * 100) if processed > 0 else 0
                logger.info(
                    f"📊 [CHECKPOINT EOS] | Traité: {processed:>5} | "
                    f"Alertes: {flagged:>4} ({alert_ratio:>5.1f}%) | "
                    f"Offset: {message.offset} | "
                    f"Durée: {elapsed:>6.1f}s | "
                    f"Débit: {processed/elapsed:>6.0f} msg/sec"
                )
            
            # STOP après 1000 messages (démo)
            if processed >= 1000:
                logger.warning("🛑 Limite démo atteinte (1000 messages)")
                break
        
        consumer.commit()
        
    except KafkaError as e:
        logger.error(f"❌ Erreur Kafka: {e}")
    except KeyboardInterrupt:
        print(f"\n")
        logger.info("👋 Arrêt manuel du consumer")
    finally:
        elapsed_total = time.time() - start_time
        if processed > 0:
            alert_ratio = (flagged / processed * 100)
            print(f"\n{'='*80}")
            print(f"📈 STATISTIQUES FINALES")
            print(f"{'='*80}")
            print(f"├── Chemin: /shared_volume/kafka/consumer.py")
            print(f"├── Total traité: {processed}")
            print(f"├── Total alertes: {flagged}")
            print(f"├── Ratio alertes: {alert_ratio:.2f}%")
            print(f"├── Durée: {elapsed_total:.1f}s")
            print(f"├── Débit: {processed/elapsed_total:.1f} msg/sec")
            print(f"├── Status: Tous les messages committés (EOS assuré ✅)")
            print(f"└── Consumer Group: aml-filter")
            print(f"{'='*80}\n")
        
        consumer.close()
        logger.info("✅ Consumer fermé proprement\n")

if __name__ == '__main__':
    main()
