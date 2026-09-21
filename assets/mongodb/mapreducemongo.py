# -*- coding: utf-8 -*-
"""
MapReduce moderne avec MongoDB (agrégation)
Remplace l'ancienne méthode map_reduce() obsolète depuis MongoDB 5.0.
"""

# Installer pymongo si nécessaire (dans Colab ou terminal)
# !pip install pymongo

from pymongo import MongoClient
import getpass
import os

# 1. Connexion sécurisée
# Méthode 1 : demander le mot de passe en mode interactif
# password = getpass.getpass("Entrez votre mot de passe MongoDB Atlas : ")
# uri = f"mongodb+srv://yalami:{password}@cluster0.apxc2.mongodb.net/?retryWrites=true&w=majority"

# Méthode 2 : utiliser une variable d'environnement (recommandé en production)
# uri = os.environ.get("MONGO_URI")
# Pour Colab, vous pouvez directement coller votre URI (attention sécurité)
uri = "mongodb+srv://yalami:VOTRE_MOT_DE_PASSE@cluster0.apxc2.mongodb.net/?retryWrites=true&w=majority"

try:
    client = MongoClient(uri)
    db = client["bankdb"]           # Nom de votre base (BankDB)
    collection = db["transactions"] # Nom de votre collection
    # Tester la connexion
    client.admin.command('ping')
    print("✅ Connexion à MongoDB Atlas réussie !")
except Exception as e:
    print(f"❌ Erreur de connexion : {e}")
    exit()

# 2. Agrégation 1 : Montant total des transactions par type de transaction
pipeline1 = [
    {"$group": {"_id": "$Transaction Type", "totalAmount": {"$sum": "$Transaction Amount"}}}
]

print("\n📊 Montant total par type de transaction :")
for doc in collection.aggregate(pipeline1):
    print(f"   {doc['_id']} : {doc['totalAmount']:.2f} MAD")

# 3. Agrégation 2 : Montant moyen par type de transaction
pipeline2 = [
    {"$group": {"_id": "$Transaction Type", "avgAmount": {"$avg": "$Transaction Amount"}}}
]

print("\n📈 Montant moyen par type de transaction :")
for doc in collection.aggregate(pipeline2):
    print(f"   {doc['_id']} : {doc['avgAmount']:.2f} MAD")

# 4. Agrégation 3 : Top 5 des émetteurs (Sender Account ID) par montant total envoyé
pipeline3 = [
    {"$group": {"_id": "$Sender Account ID", "totalSent": {"$sum": "$Transaction Amount"}}},
    {"$sort": {"totalSent": -1}},
    {"$limit": 5}
]

print("\n🏆 Top 5 des comptes qui envoient le plus d'argent :")
for doc in collection.aggregate(pipeline3):
    print(f"   {doc['_id']} : {doc['totalSent']:.2f} MAD")

# 5. Agrégation 4 : Taux de fraude (proportion de transactions frauduleuses)
pipeline4 = [
    {"$group": {"_id": "$Fraud Flag", "count": {"$count": {}}}},
    {"$group": {"_id": None, "total": {"$sum": "$count"}, 
                "fraudCount": {"$sum": {"$cond": [{"$eq": ["$_id", "True"]}, "$count", 0]}}}},
    {"$project": {"fraudRate": {"$multiply": [{"$divide": ["$fraudCount", "$total"]}, 100]}}}
]

result = list(collection.aggregate(pipeline4))
if result:
    print(f"\n⚠️ Taux de fraude : {result[0]['fraudRate']:.2f}%")
else:
    print("\n⚠️ Aucune donnée trouvée pour le calcul du taux de fraude.")