# Mexora RH Intelligence - Data Lake IT Maroc

Projet academique de Data Engineering - Miniprojet 2.

Objectif : construire un Data Lake RH pour analyser le marche de l'emploi IT au Maroc et produire des indicateurs utiles pour la strategie de recrutement de Mexora.

Le projet suit une architecture Bronze / Silver / Gold.

---

## Etat du Projet

Le pipeline complet a ete execute avec succes.

| Element | Etat |
|---|---|
| Dataset principal | 5 000 offres IT marocaines |
| Referentiel competences | 455 competences normalisees, 788 alias, 14 familles |
| Referentiel entreprises | 41 entreprises, 6 colonnes |
| Bronze | 69 partitions JSON par source et mois |
| Silver offres | 5 000 lignes, 33 colonnes |
| Silver competences | 35 042 lignes, 70 competences detectees |
| Gold | 5 tables analytiques Parquet |
| Notebook analyse | Execute et mis a jour |
| Visualisations | PNG + carte HTML dans `analysis/` |

---

## Structure

```text
mexora_rh_lake/
|-- data_sources/
|   |-- offres_emploi_it_maroc.json
|   |-- referentiel_competences_it.json
|   `-- entreprises_it_maroc.csv
|
|-- pipeline/
|   |-- bronze_ingestion.py
|   |-- silver_transform.py
|   |-- silver_nlp.py
|   `-- gold_aggregation.py
|
|-- data_lake/
|   |-- bronze/
|   |-- silver/
|   |   |-- offres_clean/
|   |   `-- competences_extraites/
|   `-- gold/
|
|-- analysis/
|   |-- analyse_marche_it_maroc.ipynb
|   |-- carte_maroc_offres.html
|   |-- top15_competences.png
|   |-- boxplot_salaires.png
|   |-- evolution_mensuelle.png
|   |-- correlation_experience_salaire.png
|   |-- top15_recruteurs.png
|   `-- top5_par_profil_data.png
|
|-- document_conception.md
|-- rapport_pipeline.md
|-- main.py
|-- requirements.txt
`-- README.md
```

---

## Installation

Prerequis :

- Python 3.11+
- Git

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

---

## Execution

Lancer le pipeline complet :

```bash
python main.py
```

Lancer les etapes separement :

```bash
python pipeline/bronze_ingestion.py
python pipeline/silver_transform.py
python pipeline/silver_nlp.py
python pipeline/gold_aggregation.py
```

---

## Architecture Data Lake

### Bronze

But : conserver les donnees brutes sans modification.

- Format : JSON
- Partitionnement : `source / mois`
- Contenu : offres originales issues du dataset source

### Silver

But : nettoyer et standardiser les donnees.

- Format : Parquet
- `offres_clean.parquet` : offres nettoyees
- `competences.parquet` : format long, une ligne par competence detectee

Transformations principales :

- normalisation des villes
- standardisation des types de contrat
- parsing des salaires en MAD
- parsing de l'experience en annees
- normalisation des titres de poste
- controle des dates incoherentes
- extraction NLP des competences depuis `competences_brut` et `description`

### Gold

But : produire les tables analytiques consommees par DuckDB, le notebook et les visualisations.

Tables produites :

- `top_competences.parquet`
- `salaires_par_profil.parquet`
- `offres_par_ville.parquet`
- `entreprises_recruteurs.parquet`
- `tendances_mensuelles.parquet`

---

## Donnees Sources

| Fichier | Description |
|---|---|
| `data_sources/offres_emploi_it_maroc.json` | Dataset principal de 5 000 offres IT marocaines |
| `data_sources/referentiel_competences_it.json` | Dictionnaire de 455 competences IT avec alias |
| `data_sources/entreprises_it_maroc.csv` | Referentiel de 41 entreprises IT ou recruteurs |

---

## Analyse

Le notebook `analysis/analyse_marche_it_maroc.ipynb` repond aux 5 questions demandees :

1. Competences IT les plus demandees
2. Comparaison Tanger / Casablanca / Rabat
3. Analyse des salaires IT
4. Relation experience / salaire
5. Entreprises recruteurs et concurrents de Mexora

Visualisations disponibles :

- carte du Maroc des offres par ville
- top 15 des competences
- boxplot des salaires par profil
- evolution mensuelle des profils Data Engineer, Data Analyst et Data Scientist
- correlation experience / salaire
- top recruteurs
- top competences par profil data

---

## Dependances

| Package | Usage |
|---|---|
| pandas | Manipulation des donnees |
| pyarrow | Lecture/ecriture Parquet |
| duckdb | Requetes SQL sur Parquet |
| matplotlib | Visualisations |
| seaborn | Visualisations statistiques |
| plotly | Carte interactive |
| jupyter | Notebook d'analyse |

---

## Livrables Presents

| Livrable | Fichier |
|---|---|
| Pipeline Python | `pipeline/`, `main.py` |
| Rapport pipeline | `rapport_pipeline.md` |
| Document conception | `document_conception.md` |
| Notebook analyse | `analysis/analyse_marche_it_maroc.ipynb` |
| Dashboard / visualisations | `analysis/*.png`, `analysis/carte_maroc_offres.html` |
| Tables Gold | `data_lake/gold/*.parquet` |

---

## Verification Rapide

```bash
python -c "import pandas as pd; print(pd.read_parquet('data_lake/silver/offres_clean/offres_clean.parquet').shape); print(pd.read_parquet('data_lake/silver/competences_extraites/competences.parquet').shape)"
```

Resultats attendus :

```text
(5000, 33)
(35042, 10)
```
