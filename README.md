# Mexora RH Intelligence — Data Lake IT Maroc

Projet académique de Data Engineering — Miniprojet 2  
**Analyse du marché de l'emploi IT au Maroc via un Data Lake Bronze/Silver/Gold**


## 🗂️ Structure du projet

```
mexora_rh_lake/
├── data_sources/
│   ├── offres_emploi_it_maroc.json       # 5 000 offres IT marocaines
│   └── referentiel_competences_it.json   # Dictionnaire 143 compétences
│
├── pipeline/
│   ├── bronze_ingestion.py    # Chargement brut → zone Bronze
│   ├── silver_transform.py    # Nettoyage + standardisation → Silver
│   ├── silver_nlp.py          # Extraction compétences depuis texte
│   └── gold_aggregation.py    # Agrégats → Gold
│
├── analysis/
│   └── analyse_marche.py      # Requêtes DuckDB analytiques
│
├── data_lake/                 # Répertoire racine du Data Lake (généré)
│   ├── bronze/                # Données brutes partitionnées (JSON)
│   ├── silver/                # Données nettoyées (Parquet)
│   └── gold/                  # Tables analytiques (Parquet)
│
├── main.py                    # Orchestration du pipeline complet
├── requirements.txt           # Dépendances Python
├── rapport_pipeline.md        # Rapport des transformations (généré)
└── README.md                  # Ce fichier
```

---

## ⚙️ Installation

### Prérequis
- Python 3.11+
- Git

### Étapes

```bash
# 1. Cloner le dépôt
git clone https://github.com/[votre-username]/mexora-rh-intelligence.git
cd mexora-rh-intelligence

# 2. Créer un environnement virtuel (recommandé)
python -m venv venv
source venv/bin/activate        # Linux/Mac
# ou
venv\Scripts\activate           # Windows

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Lancer le pipeline complet
python main.py
```

---

## 🚀 Utilisation

### Lancer le pipeline complet (recommandé)
```bash
python main.py
```

### Lancer chaque étape séparément

```bash
# Étape Bronze uniquement
python pipeline/bronze_ingestion.py

# Étape Silver uniquement (après Bronze)
python pipeline/silver_transform.py

# Extraction compétences NLP (après Silver transform)
python pipeline/silver_nlp.py

# Construction Gold (Binôme — après Silver)
python pipeline/gold_aggregation.py
```

---

## 📊 Architecture Data Lake

```
offres_emploi_it_maroc.json
          │
          ▼
┌─────────────────────────────────┐
│  ZONE BRONZE — Données brutes   │
│  Format : JSON                  │
│  Partition : source / mois      │
│  Immuable — archive fidèle      │
└────────────┬────────────────────┘
             │ Nettoyage + Standardisation
             ▼
┌─────────────────────────────────┐
│  ZONE SILVER — Données propres  │
│  Format : Parquet (Snappy)      │
│  offres_clean.parquet           │
│  competences.parquet (format    │
│  long, 1 ligne = 1 compétence)  │
└────────────┬────────────────────┘
             │ Agrégation + KPIs
             ▼
┌─────────────────────────────────┐
│  ZONE GOLD — Tables analytiques │
│  Format : Parquet               │
│  top_competences.parquet        │
│  salaires_par_profil.parquet    │
│  offres_par_ville.parquet       │
│  entreprises_recruteurs.parquet │
│  tendances_mensuelles.parquet   │
└─────────────────────────────────┘
```

---

## 🔧 Transformations Silver appliquées

| Champ | Problème | Traitement |
|---|---|---|
| `ville` | `"casa"`, `"CASABLANCA"` | Regex + mapping → `"Casablanca"` |
| `type_contrat` | `"cdi"`, `"Contrat à durée indéterminée"` | Regex → `"CDI"` |
| `salaire_brut` | `"15K-20K"`, `"1500 EUR"`, `null` | Parser regex + conversion EUR→MAD |
| `experience_requise` | `"3 à 5 ans"`, `"min 3 ans"` | Parser regex → `exp_min`, `exp_max` |
| `titre_poste` | `"Dev Data"`, `"Data Eng."` | 20+ patterns → `"Data Engineer"` |
| `date_publication` | Formats mixtes + incohérences | Normalisation → `YYYY-MM-DD` |
| `description` | Texte libre | NLP regex → compétences structurées |

---

## 📁 Fichiers générés

Après exécution de `python main.py` :

| Fichier | Zone | Taille |
|---|---|---|
| `bronze/rekrute/YYYY_MM/offres_raw.json` | Bronze | ~xx Ko/partition |
| `silver/offres_clean/offres_clean.parquet` | Silver | ~420 Ko |
| `silver/competences_extraites/competences.parquet` | Silver | ~168 Ko |
| `rapport_pipeline.md` | — | Rapport transformations |

---

## 📦 Dépendances

| Package | Version | Usage |
|---|---|---|
| pandas | ≥2.0 | Manipulation DataFrames |
| pyarrow | ≥12.0 | Lecture/écriture Parquet |
| duckdb | ≥0.9 | Requêtes SQL sur Parquet |
| matplotlib | ≥3.7 | Visualisations |
| seaborn | ≥0.12 | Visualisations statistiques |
| plotly | ≥5.15 | Visualisations interactives |
| jupyter | ≥1.0 | Notebooks d'analyse |

---

## 📋 Rapport des transformations

Le fichier `rapport_pipeline.md` est généré automatiquement par `main.py`.
Il documente pour chaque transformation :
- La règle appliquée
- Les statistiques avant/après
- Les cas limites et leur traitement

---

## 🏫 Contexte académique

Miniprojet 2 — Data Lake & Analyse du Marché de l'Emploi IT au Maroc  
Module : Data Engineering  
Entreprise fictive : **Mexora RH Intelligence**  
Sources fictives : Rekrute, MarocAnnonce, LinkedIn Maroc
