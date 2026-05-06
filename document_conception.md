# Document de Conception — Architecture Data Lake
## Mexora RH Intelligence | Miniprojet 2 Data Engineering



---

## 1. Justification des Formats par Zone

| Zone | Format choisi | Pourquoi ce format ? | Pourquoi pas les autres ? |
|------|--------------|---------------------|--------------------------|
| **Bronze** | **JSON** | Les données source arrivent en JSON depuis le scraping web (Rekrute, MarocAnnonce, LinkedIn). Conserver le format natif garantit une fidélité absolue à la source sans risque de perte ou distorsion. JSON est lisible par tout outil sans décodage préalable, ce qui facilite l'audit. | **CSV** : perd les types (null vs "null") et les structures imbriquées (langue_requise = liste). **Parquet** : format colonnaire inadapté à une archive brute — transformation trop tôt. |
| **Silver** | **Parquet (Snappy)** | Format colonnaire compressé, idéal pour les analyses pandas et DuckDB. Les requêtes sur des colonnes spécifiques (salaire, profil) ne lisent que les colonnes nécessaires → performance x5 vs CSV. Snappy offre compression ~3:1 avec décompression rapide. | **CSV** : non typé, plus lent, volumineux (pas de compression native). **JSON** : format ligne par ligne, peu performant pour les agrégats colonnaires. |
| **Gold** | **Parquet (Snappy)** | Tables pré-agrégées conçues pour la lecture analytique intensive. DuckDB peut requêter directement les fichiers Parquet sans chargement en mémoire via `read_parquet()`. Les données Gold sont immuables entre deux exécutions du pipeline. | **Base de données SQL** : nécessite un serveur, plus lourd à déployer. **CSV** : aucun typage, performance insuffisante pour les dashboards. |

---

## 2. Questions de Conception

### 2.1 Pourquoi conserver les données brutes en zone Bronze sans les modifier ?

La zone Bronze est le **contrat de confiance** entre les données sources et le système d'analyse. Elle doit être conservée intacte pour plusieurs raisons fondamentales :

**Reproductibilité totale :** Si une transformation Silver introduit une erreur (ex : mauvaise conversion de devise), on peut toujours repartir des données Bronze originales sans avoir à relancer le scraping. C'est le filet de sécurité du pipeline.

**Traçabilité légale et métier :** En cas de litige ou d'audit ("pourquoi cette offre a-t-elle un salaire de 0 MAD ?"), le Bronze permet de vérifier exactement ce qui a été reçu depuis la source. La réponse est toujours dans Bronze.

**Évolution des règles métier :** Les règles de nettoyage Silver évoluent (nouveau taux EUR/MAD, nouvelle ville à mapper). Avec Bronze intact, on peut re-transformer sans repasser par le scraping.

**Risques si on ne le fait pas :** Perte irrémédiable de l'information originale, impossibilité de détecter si le problème vient de la source ou du pipeline, risque de data drift non détectable.

### 2.2 Schema-on-read vs Schema-on-write

| | **Schema-on-write (DWH classique — Miniprojet 1)** | **Schema-on-read (Data Lake — Miniprojet 2)** |
|---|---|---|
| **Définition** | Le schéma est défini avant l'écriture des données. Les données doivent se conformer au schéma à l'ingestion. | Le schéma est appliqué au moment de la lecture. Les données sont stockées telles quelles. |
| **Exemple** | Table SQL `offres(id INT, salaire DECIMAL(10,2))` : on ne peut pas insérer "Selon profil" dans `salaire`. | Fichier JSON Bronze : `"salaire_brut": "Selon profil"` est stocké tel quel. La conversion en DECIMAL se fait à la lecture Silver. |
| **Avantage** | Données toujours cohérentes, requêtes simples. | Ingestion rapide, flexibilité totale, supporte les schémas variables. |
| **Inconvénient** | Rigide : un nouveau champ exige une migration de schéma. | Risque de mauvaise qualité si Silver est mal conçue. |
| **Usage** | Données structurées stables (transactions bancaires). | Données semi-structurées variées (offres d'emploi scrappées). |

### 2.3 Choix de partitionnement par zone

**Bronze : partitionnement par `source / mois`**

Le partitionnement Bronze reflète la **provenance** des données. Chaque source (Rekrute, MarocAnnonce, LinkedIn) a ses propres caractéristiques de format et de qualité. En cas de problème sur une source spécifique, on peut isoler et réingérer uniquement cette partition.

Le sous-partitionnement par **mois** permet de réingérer des données sur une période précise (ex : re-scraping de LinkedIn pour janvier 2024) sans toucher aux autres mois.

```
bronze/rekrute/2024_01/offres_raw.json   ← 194 offres Rekrute de janvier 2024
bronze/linkedin/2024_01/offres_raw.json  ← offres LinkedIn du même mois
```

**Silver : partitionnement par `ville / mois`**

En Silver, les données sont nettoyées et le partitionnement devient **analytique**. Les requêtes les plus fréquentes filtrent par ville (Tanger vs Casablanca) et par période (tendances temporelles). Ce partitionnement permet à DuckDB de faire du **partition pruning** : pour une requête `WHERE ville = 'Tanger' AND mois = '2024-06'`, seuls les fichiers de cette partition sont lus.

### 2.4 Gouvernance : éviter le Data Swamp

Un Data Lake non gouverné devient un **Data Swamp** (marécage) : données non documentées, non traçables, inutilisables. Pour Mexora RH Intelligence, les règles suivantes sont appliquées :

**Règle 1 — Catalogue de données :** Chaque fichier Bronze inclut des métadonnées (`date_ingestion`, `source`, `nb_offres`, `partition`). Le `rapport_pipeline.md` documente toutes les transformations.

**Règle 2 — Immuabilité Bronze :** Aucun processus n'est autorisé à modifier les fichiers Bronze. Toute correction passe par Silver.

**Règle 3 — Nommage standardisé :** `offres_raw.json` en Bronze, `offres_clean.parquet` en Silver, noms explicites en Gold (`salaires_par_profil.parquet`).

**Règle 4 — Qualité mesurée :** Chaque étape Silver produit des métriques (% salaires connus, % dates valides, % titres classifiés) loguées dans `rapport_pipeline.md`. Un seuil d'alerte à 70% peut déclencher une revue manuelle.

**Règle 5 — Versioning Git :** Tous les scripts sont versionnés. Une modification de règle métier génère un commit documenté.

---

## 3. Schéma d'Architecture

*(Voir fichier image `schema_architecture.png` joint)*

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                           │
│   [Rekrute JSON]    [MarocAnnonce JSON]    [LinkedIn JSON]          │
└────────────────────────┬────────────────────────────────────────────┘
                         │ Python: bronze_ingestion.py
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ZONE BRONZE — Archive Immuable                                      │
│  Format : JSON partitionné (source/mois)                             │
│  69 partitions — 5 000 offres brutes                                 │
│  Accès : Data Engineers uniquement (audit, debug)                    │
└────────────────────────┬────────────────────────────────────────────┘
                         │ Python: silver_transform.py + silver_nlp.py
                         │ Nettoyage villes, salaires, dates, contrats
                         │ NLP: extraction compétences depuis texte libre
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ZONE SILVER — Données Nettoyées                                     │
│  Format : Parquet Snappy                                             │
│  offres_clean.parquet (5 000 lignes, 33 colonnes)                   │
│  competences.parquet (32 227 lignes — 1 par compétence×offre)       │
│  Accès : Data Engineers, Data Scientists                             │
└────────────────────────┬────────────────────────────────────────────┘
                         │ Python: gold_aggregation.py (DuckDB SQL)
                         │ Agrégats, KPIs, rankings
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ZONE GOLD — Tables Analytiques                                      │
│  Format : Parquet Snappy                                             │
│  top_competences.parquet                                             │
│  salaires_par_profil.parquet                                         │
│  offres_par_ville.parquet                                            │
│  entreprises_recruteurs.parquet                                      │
│  tendances_mensuelles.parquet                                        │
│  Accès : Data Analysts, DRH, Dashboard (Metabase/Power BI)          │
└─────────────────────────────────────────────────────────────────────┘
                         │ DuckDB + Python (analyse_marche.py)
                         ▼
              ┌──────────────────────┐
              │   RAPPORT ANALYTIQUE │
              │   Dashboard DRH      │
              │   Recommandations RH │
              └──────────────────────┘
```

**Outils utilisés par étape :**
- Bronze → Silver : `Python 3.11`, `pandas`, `re` (regex)
- Silver → Gold : `DuckDB`, `pandas`, `pyarrow`
- Gold → Analyse : `DuckDB`, `matplotlib`, `seaborn`, `plotly`
- Stockage : Système de fichiers local (simulant un objet store S3/GCS)
