# Projet : Analyse des jeux vidéo

groupe : Thomas Coutarel et Jacques Lin 


---

##  Structure du projet


```
src/
└── main/
    ├── resources/
    └── scala/
        ├── datamart/
        │   ├── GenrePerformance.scala
        │   ├── SalesByPlatform.scala
        │   └── silver.scala
        │
        ├── pipelines/           <-- Architecture ETL 
        │   ├── extract/         <-- Extraction des différentes sources de données
        │   ├── transform/       <-- Nettoyage des sources de données   
        │   └── load/            <-- Chargement dans la BDD
        │
        └── main.scala           <-- Orchestration du pipeline ET du datamart

```


---

##  Orchestration du Pipeline ETL

Le script principal d'orchestration (`main.scala`) exécute les scripts dans l'ordre suivant :


```scala
//  EXTRACT
ExtractFromCSV.run(spark)
ExtractFromPostgres.run(spark)

//  TRANSFORM
silver.silver_layer(spark)

//  DATAMART
SalesByPlatform.run(spark)
GenrePerformance.run(spark)

//  LOAD
load__SalesByPlatform.run(spark)
load__GenrePerformance.run(spark)
```

---

##  scripts & Responsabilités

1. **Extract**

   * `ExtractFromCSV` : lit le fichier `vgsales.csv` et charge les données brutes.
   * `ExtractFromPostgres` : extrait des données dans une table PostgreSQL.

2. **Transform (layer silver)**

   * `silver` normalise les colonnes (ex : conversions de casse, filtrages, suppression de doublons).

3. **Datamart**

   * `SalesByPlatform` : agrège les ventes globales par plateforme.
   * `GenrePerformance` : calcule les ventes totales et nombre de jeux par genre.

4. **Load**

   * `load__SalesByPlatform` & `load__GenrePerformance` : insèrent les tables agrégées dans PostgreSQL via JDBC.

---

##  Installation & Exécution

##  Prérequis

* **Scala 2.12+** et **Spark 3.x**
* **PostgreSQL** avec une table (connectivité via JDBC, driver `"org.postgresql.Driver"`)
* Fichier source **`vgsales.csv`**
* Fichier de configuration (`application.conf`) pour les connexions et répertoires

1. Cloner le dépôt :

```bash
git clone https://github.com/linjacques/Functionnal-programming
```

2. aller à au répertoir du projet cloné :

```bash
cd Functionnal-programming
```

3. aller à la racine de l'application Scala-spark :

```bash
cd projet_analyse_jeux_videos
```

4. Exécuter main.scala :
```bash
sbt run 
```

cette commande va automatiquement lancer le/les fichier(s) contenant l'instruction suivante : 
```scala
  def main(args: Array[String]): Unit = {
  ...
}
```
---

##  Résultats 

* Tables (datamarts) créées dans PostgreSQL :

  * `sales_by_platform`
  * `genre_performance`
* Fichiers .parquet générés dans les couches du lakehouse:

  * bronze
  * silver
  * gold

---
