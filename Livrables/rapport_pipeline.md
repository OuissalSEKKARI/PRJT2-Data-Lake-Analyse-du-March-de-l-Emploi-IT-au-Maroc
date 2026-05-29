# Rapport Pipeline — Mexora RH Intelligence Data Lake

Généré le : 2026-05-25 15:28:48

---

## 1. Ingestion Bronze

| Métrique | Valeur |
|---|---|
| Offres ingérées | 5,000 |
| Partitions créées | 69 |
| Date ingestion | 2026-05-25T15:27:00 |

### Répartition par source
| Source | Offres |
|---|---|
| linkedin | 1,696 |
| marocannonce | 1,612 |
| rekrute | 1,692 |

**Règle appliquée** : données copiées telles quelles depuis la source.
Aucune modification. La zone Bronze est immuable.

**Cas limites** : dates dans plusieurs formats (`YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`).
Traitées au niveau Silver uniquement. En Bronze, stockées telles quelles.

---

## 2. Transformation Silver — Nettoyage

### 2.1 Normalisation des dates

| Métrique | Valeur |
|---|---|
| Dates valides | 5,000 |
| Dates invalides/inconnues | 0 |
| Incohérences corrigées (pub > exp) | 253 |

**Règle** : 3 formats de date détectés et normalisés vers `YYYY-MM-DD`.
Dates d'expiration antérieures à la date de publication → invalidées (mise à NULL).

### 2.2 Normalisation des villes

| Métrique | Valeur |
|---|---|
| Offres avant traitement | 5,000 |
| Villes distinctes (après) | 11 |
| Offres ville inconnue | 0 |

**Règle** : mapping regex case-insensitive + suppression espaces.
Ex : `"CASABLANCA"`, `"casa"`, `"Casablanca "` → `"Casablanca"`
Région administrative ajoutée depuis un dictionnaire statique.

### 2.3 Standardisation des contrats

| Type | Offres |
|---|---|
| CDD | 1,340 |
| Stage | 1,014 |
| CDI | 1,003 |
| Freelance | 979 |
| Alternance | 664 |

**Règle** : expressions régulières sur la valeur brute en minuscules.
Ex : `"Contrat à durée indéterminée"`, `"cdi"`, `"Permanent"` → `"CDI"`

### 2.4 Normalisation des salaires

| Métrique | Valeur |
|---|---|
| Offres avec salaire valide | 3,918 (78.4%) |
| Offres sans salaire (null/confidentiel) | 1,082 |
| Salaire médian global (MAD) | 17,500 |

**Règle** : extraction regex des montants numériques.
Conversion EUR → MAD au taux 1 EUR = 10.8 MAD (taux 2024).
Conversion "K" → x1000. Validation entre 3 000 et 100 000 MAD.

**Cas limites traités** :
- `"Selon profil"`, `"Confidentiel"`, `null` → `salaire_connu = False`
- Montants EUR → convertis en MAD
- `"15K-20K"` → `sal_min=15000, sal_max=20000`

### 2.5 Parsing de l'expérience

| Métrique | Valeur |
|---|---|
| Expérience parsée | 4,708 |
| Non parsée | 292 |

**Règle** : parsing en deux passes — fourchettes puis minimum seul.
Mots-clés `débutant/junior` → (0, 2 ans), `senior/expert` → (5+, None).

### 2.6 Normalisation des titres de poste

| Profil normalisé | Offres |
|---|---|
| Data Analyst | 861 |
| Data Engineer | 694 |
| Autre IT | 564 |
| Data Scientist | 461 |
| Développeur Backend | 412 |
| Architecte IT | 336 |
| Cybersécurité | 321 |
| Chef de Projet IT | 235 |
| Développeur Full Stack | 205 |
| Développeur Frontend | 201 |
| DevOps / SRE | 173 |
| Développeur Mobile | 119 |
| Cloud Engineer | 114 |
| QA / Test Engineer | 104 |
| Admin Systèmes & Réseaux | 103 |
| Tech Lead | 97 |

**Règle** : 20+ patterns regex couvrant les principales familles IT.
Non classifiés conservés comme `"Autre IT"` avec flag pour audit.

---

## 3. Extraction NLP — Compétences Silver

| Métrique | Valeur |
|---|---|
| Lignes compétences générées | 35,042 |
| Offres avec ≥1 compétence | 5,000 |
| Offres sans compétence détectée | 0 |
| Compétences uniques détectées | 70 |
| Moyenne compétences par offre | 7.0 |

### Top 10 compétences

| Compétence | Famille | Mentions |
|---|---|---|
| docker | devops | 2,176 |
| sql | langages | 1,897 |
| git | devops | 1,881 |
| agile | methodologies | 1,771 |
| aws | cloud | 1,769 |
| gcp | cloud | 1,706 |
| javascript | langages | 1,224 |
| dbt | data_engineering | 1,128 |
| azure | cloud | 1,013 |
| kubernetes | devops | 1,004 |

**Stratégie NLP** :
- Sources : `competences_brut` + `description` concaténés
- Matching par word boundary regex (`\b alias \b`)
- Alias triés par longueur décroissante (évite les faux positifs)
- Dédupliquer par offre : 1 compétence max par offre peu importe le nombre de mentions

**Cas limites** :
- `"node"` vs `"node.js"` : résolu par tri longueur décroissante
- Séparateurs variés (`/`, `•`, `;`) : nettoyés avant matching
- Offres sans description : traçées comme `non_détecté`

---

## 4. Agrégation Gold — Tables analytiques

| Métrique | Valeur |
|---|---|
| Tables Gold produites | 5 |
| Lignes analytiques générées | 3,507 |
| Format de sortie | Parquet Snappy |
| Moteur d'agrégation | DuckDB SQL |
| Source offres | `data_lake/silver/offres_clean/offres_clean.parquet` |
| Source compétences | `data_lake/silver/competences_extraites/competences.parquet` |

**Objectif** : transformer les données Silver nettoyées en tables pré-agrégées directement exploitables par le notebook analytique, DuckDB et les dashboards RH.

### 4.1 Tables produites

| Table | Lignes | Colonnes | Usage analytique |
|---|---:|---:|---|
| `top_competences.parquet` | 409 | 6 | Compétences les plus demandées par profil |
| `salaires_par_profil.parquet` | 357 | 11 | Statistiques salariales par profil, ville et contrat |
| `offres_par_ville.parquet` | 2,275 | 8 | Volume d'offres par ville, profil et mois |
| `entreprises_recruteurs.parquet` | 100 | 8 | Classement des entreprises les plus recruteuses |
| `tendances_mensuelles.parquet` | 366 | 7 | Évolution mensuelle des volumes et salaires |

### 4.2 Top compétences par profil

| Colonne clé | Description |
|---|---|
| `profil` | Profil IT normalisé |
| `famille` | Famille de compétence |
| `competence` | Compétence détectée |
| `nb_offres_mentionnent` | Nombre d'offres distinctes mentionnant la compétence |
| `pct_offres_total` | Part de la compétence dans l'ensemble des offres |
| `rang_dans_profil` | Rang de la compétence à l'intérieur du profil |

**Règle** : agrégation sur les compétences détectées uniquement (`competence != 'non_détecté'`).
Le rang est calculé par fenêtre SQL `RANK()` sur chaque profil, en ordre décroissant du nombre d'offres.

### 4.3 Salaires par profil

| Indicateur | Description |
|---|---|
| `nb_offres` | Volume total dans le segment profil/ville/contrat |
| `nb_offres_avec_salaire` | Offres avec salaire exploitable |
| `salaire_median_mad` | Médiane des salaires en MAD |
| `salaire_moyen_mad` | Moyenne des salaires en MAD |
| `salaire_q1_mad`, `salaire_q3_mad` | Quartiles salariaux |
| `salaire_min_observe`, `salaire_max_observe` | Bornes observées après nettoyage |

**Règle** : regroupement par `profil_normalise`, `ville_std` et `type_contrat_std`.
Les segments trop petits sont exclus avec `HAVING COUNT(*) >= 5` pour éviter des statistiques peu robustes.

### 4.4 Offres par ville et tendances

| Table | Grain analytique | KPIs |
|---|---|---|
| `offres_par_ville.parquet` | Ville × profil × année × mois | `nb_offres`, `nb_offres_remote`, `pct_remote` |
| `tendances_mensuelles.parquet` | Année × mois × profil | `nb_offres`, `salaire_moyen_mois`, `evolution_pct` |

**Règles** :
- le remote est détecté par mots-clés dans le champ `teletravail` : `télétravail`, `remote`, `hybride`
- l'évolution mensuelle est calculée avec `LAG()` par profil
- les lignes sans `annee` ou `mois` sont exclues des tendances temporelles

### 4.5 Entreprises recruteurs

| Indicateur | Description |
|---|---|
| `nb_offres_publiees` | Volume d'offres publiées par entreprise et ville |
| `nb_profils_differents` | Diversité des profils recrutés |
| `salaire_moyen_propose` | Salaire moyen proposé lorsque connu |
| `profils_recrutes` | Liste distincte des profils recrutés |
| `premiere_offre`, `derniere_offre` | Fenêtre temporelle d'activité observée |

**Règle** : seules les entreprises non vides avec au moins 3 offres sont conservées.
La table est limitée au Top 100 des recruteurs par volume d'offres.

### 4.6 Contrôles post-construction

| Contrôle | Résultat |
|---|---|
| Existence des 5 fichiers Parquet | OK |
| Lecture DuckDB de chaque table | OK |
| Comptage des lignes | OK |
| Vérification des colonnes attendues | OK |

**Sortie finale** : les tables Gold sont stockées dans `data_lake/gold/` et servent de couche de consommation pour `analysis/analyse_marche_it_maroc.ipynb`.
