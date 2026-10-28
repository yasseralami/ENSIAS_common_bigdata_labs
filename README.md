# Big Data & Applications — labs ENSIAS

Labs et projet du module **Big Data & Applications**, cycle ingénieur, 3e année (IDF, 2SCL, BI&A), ENSIAS, Université Mohammed V de Rabat, 2026-2027.

**Site du module : https://yasseralami.github.io/ENSIAS_common_bigdata_labs/**

## Les labs

| Lab | Sujet | Environnement |
|---|---|---|
| [Lab 0](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/lab0/lab0.html) | Diagnostiquer un besoin Big Data | Navigateur |
| [Lab 1](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/lab1/lab1.html) | Cluster Hadoop, HDFS et MapReduce | Codespaces |
| [Lab 2](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/lab2/lab2.html) | Cluster Spark, RDD et Spark UI | Codespaces |
| [Lab 3](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/lab3/lab3.html) | DataFrames et Spark SQL | Databricks Free Edition |
| [Lab 4](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/lab4/lab4.html) | Lakehouse Bronze, Silver, Gold | Databricks Free Edition |
| [Lab 5](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/lab5/lab5.html) | MongoDB : modélisation, agrégation, index, Charts | MongoDB Atlas |
| [Lab 6](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/lab6/lab6.html) | Kafka, Spark Structured Streaming, alertes MongoDB | Codespaces + Atlas |
| Lab 7 | Orchestration et qualité avec Airflow | Codespaces (à venir) |

Projet fil rouge : [sujets et consignes](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/projet/projet.html) · [suivi d'équipe](https://yasseralami.github.io/ENSIAS_common_bigdata_labs/projet/suivi.html).

## Démarrage rapide (Codespaces)

```bash
git pull
docker compose up -d
docker exec -it cluster-master bash
```

Erreur SSH `Connection timed out` entre les conteneurs : voir [CODESPACES-HADOOP-SSH.md](CODESPACES-HADOOP-SSH.md).

## Contenu du dépôt

| Chemin | Contenu |
|---|---|
| `index.html`, `labN/`, `projet/` | Pages du site (énoncés, projet) |
| `assets/` | Données et scripts des labs, montés dans `cluster-master` sous `/shared_volume` |
| `docker-compose.yml` | Cluster Hadoop, Spark et Kafka (conteneurs `cluster-master`, `cluster-slave1`, `cluster-slave2`, `kafka-ui`) |

## Licence

Énoncés et pages : CC BY-NC-SA 4.0. Scripts : MIT. Détails dans [LICENSE](LICENSE).
