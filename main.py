"""
=============================================================
MAIN — Mexora RH Intelligence Data Lake
=============================================================
Orchestration complète du pipeline de données :
  1. Ingestion Bronze (données brutes)
  2. Transformation Silver (nettoyage + standardisation)
  3. Extraction compétences Silver NLP
  4. Agrégation Gold (statistiques + enrichissements)

Usage :
  python main.py

=============================================================
"""

import sys
from pathlib import Path
from datetime import datetime
from pipeline.gold_aggregation import construire_gold, verifier_gold

# Ajout du répertoire pipeline au path
sys.path.insert(0, str(Path(__file__).parent / 'pipeline'))

from pipeline.bronze_ingestion import ingerer_bronze, verifier_bronze
from pipeline.silver_transform import transformer_silver, sauvegarder_silver_offres
from pipeline.silver_nlp import extraire_competences, sauvegarder_silver_competences, generer_rapport_nlp


def main():
    print("=" * 65)
    print("  MEXORA RH INTELLIGENCE — Pipeline Data Lake")
    print("=" * 65)
    print(f"  Démarrage : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # ── Chemins ─────────────────────────────────────────────────
    BASE_DIR   = Path(__file__).parent
    LAKE       = BASE_DIR / 'data_lake'
    SOURCE     = BASE_DIR / 'data_sources' / 'offres_emploi_it_maroc.json'
    REFERENTIEL = BASE_DIR / 'data_sources' / 'referentiel_competences_it.json'

    # Vérification des fichiers source
    if not SOURCE.exists():
        print(f"\n❌ Fichier source non trouvé : {SOURCE}")
        print("   Générez le fichier avec : python generate_data.py")
        return

    if not REFERENTIEL.exists():
        print(f"\n❌ Référentiel non trouvé : {REFERENTIEL}")
        return

    # ────────────────────────────────────────────────────────────
    # ÉTAPE 1 : BRONZE INGESTION
    # ────────────────────────────────────────────────────────────
    print("\n\n🔶 ÉTAPE 1/3 : INGESTION BRONZE")
    print("-" * 40)
    stats_bronze = ingerer_bronze(str(SOURCE), str(LAKE))
    verifier_bronze(str(LAKE))

    # ────────────────────────────────────────────────────────────
    # ÉTAPE 2 : SILVER TRANSFORM
    # ────────────────────────────────────────────────────────────
    print("\n\n🥈 ÉTAPE 2/3 : TRANSFORMATION SILVER")
    print("-" * 40)
    df_silver = transformer_silver(str(LAKE))
    chemin_silver = sauvegarder_silver_offres(df_silver, str(LAKE))

    # ────────────────────────────────────────────────────────────
    # ÉTAPE 3 : SILVER NLP (extraction compétences)
    # ────────────────────────────────────────────────────────────
    print("\n\n🔬 ÉTAPE 3/3 : EXTRACTION COMPÉTENCES (NLP)")
    print("-" * 40)
    df_comp = extraire_competences(df_silver, str(REFERENTIEL))
    chemin_comp = sauvegarder_silver_competences(df_comp, str(LAKE))

    # ────────────────────────────────────────────────────────────
    # ÉTAPE 4 : GOLD AGGREGATION
    # ────────────────────────────────────────────────────────────

    print("\n\n🏆 ÉTAPE 4/4 : AGRÉGATION GOLD")
    print("-" * 40)
    stats_gold = construire_gold(str(LAKE))
    verifier_gold(str(LAKE))

    # ────────────────────────────────────────────────────────────
    # RAPPORT FINAL
    # ────────────────────────────────────────────────────────────
    generer_rapport_pipeline(stats_bronze, df_silver, df_comp, BASE_DIR)

    print("\n\n" + "=" * 65)
    print("  ✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    print(f"  Fin : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)
    print(f"\n  Fichiers produits :")
    print(f"    Bronze  → {LAKE}/bronze/  (partitions JSON)")
    print(f"    Silver  → {chemin_silver}")
    print(f"    Silver  → {chemin_comp}")
    print(f"  Utiliser : python pipeline/gold_aggregation.py\n")


def generer_rapport_pipeline(stats_bronze, df_silver, df_comp, base_dir):
    """
    Génère le fichier rapport_pipeline.md documentant
    toutes les transformations appliquées.
    """
    rapport_path = base_dir / 'rapport_pipeline.md'

    df_real = df_comp[df_comp['competence'] != 'non_détecté']

    contenu = f"""# Rapport Pipeline — Mexora RH Intelligence Data Lake

Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 1. Ingestion Bronze

| Métrique | Valeur |
|---|---|
| Offres ingérées | {stats_bronze['total']:,} |
| Partitions créées | {stats_bronze['nb_fichiers_crees']} |
| Date ingestion | {stats_bronze['date_ingestion'][:19]} |

### Répartition par source
| Source | Offres |
|---|---|
""" + "\n".join(
        f"| {src} | {count:,} |"
        for src, count in sorted(stats_bronze['par_source'].items())
    ) + f"""

**Règle appliquée** : données copiées telles quelles depuis la source.
Aucune modification. La zone Bronze est immuable.

**Cas limites** : dates dans plusieurs formats (`YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`).
Traitées au niveau Silver uniquement. En Bronze, stockées telles quelles.

---

## 2. Transformation Silver — Nettoyage

### 2.1 Normalisation des dates

| Métrique | Valeur |
|---|---|
| Dates valides | {df_silver['date_publication_std'].notna().sum():,} |
| Dates invalides/inconnues | {df_silver['date_publication_std'].isna().sum():,} |
| Incohérences corrigées (pub > exp) | {int(df_silver.get('date_anomalie', False).sum())} |

**Règle** : 3 formats de date détectés et normalisés vers `YYYY-MM-DD`.
Dates d'expiration antérieures à la date de publication → invalidées (mise à NULL).

### 2.2 Normalisation des villes

| Métrique | Valeur |
|---|---|
| Offres avant traitement | {len(df_silver):,} |
| Villes distinctes (après) | {df_silver['ville_std'].nunique()} |
| Offres ville inconnue | {(df_silver['ville_std'] == 'Inconnue').sum()} |

**Règle** : mapping regex case-insensitive + suppression espaces.
Ex : `"CASABLANCA"`, `"casa"`, `"Casablanca "` → `"Casablanca"`
Région administrative ajoutée depuis un dictionnaire statique.

### 2.3 Standardisation des contrats

| Type | Offres |
|---|---|
""" + "\n".join(
        f"| {ct} | {count:,} |"
        for ct, count in df_silver['type_contrat_std'].value_counts().items()
    ) + f"""

**Règle** : expressions régulières sur la valeur brute en minuscules.
Ex : `"Contrat à durée indéterminée"`, `"cdi"`, `"Permanent"` → `"CDI"`

### 2.4 Normalisation des salaires

| Métrique | Valeur |
|---|---|
| Offres avec salaire valide | {df_silver['salaire_connu'].sum():,} ({df_silver['salaire_connu'].mean()*100:.1f}%) |
| Offres sans salaire (null/confidentiel) | {(~df_silver['salaire_connu']).sum():,} |
| Salaire médian global (MAD) | {df_silver.loc[df_silver['salaire_connu'], 'salaire_median_mad'].median():,.0f} |

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
| Expérience parsée | {df_silver['experience_min_ans'].notna().sum():,} |
| Non parsée | {df_silver['experience_min_ans'].isna().sum():,} |

**Règle** : parsing en deux passes — fourchettes puis minimum seul.
Mots-clés `débutant/junior` → (0, 2 ans), `senior/expert` → (5+, None).

### 2.6 Normalisation des titres de poste

| Profil normalisé | Offres |
|---|---|
""" + "\n".join(
        f"| {profil} | {count:,} |"
        for profil, count in df_silver['profil_normalise'].value_counts().items()
    ) + f"""

**Règle** : 20+ patterns regex couvrant les principales familles IT.
Non classifiés conservés comme `"Autre IT"` avec flag pour audit.

---

## 3. Extraction NLP — Compétences Silver

| Métrique | Valeur |
|---|---|
| Lignes compétences générées | {len(df_comp):,} |
| Offres avec ≥1 compétence | {df_real['id_offre'].nunique():,} |
| Offres sans compétence détectée | {(df_comp['competence'] == 'non_détecté').sum():,} |
| Compétences uniques détectées | {df_real['competence'].nunique()} |
| Moyenne compétences par offre | {len(df_real)/max(df_real['id_offre'].nunique(), 1):.1f} |

### Top 10 compétences

| Compétence | Famille | Mentions |
|---|---|---|
""" + "\n".join(
        f"| {comp} | {df_real[df_real['competence'] == comp]['famille'].iloc[0] if len(df_real[df_real['competence'] == comp]) > 0 else '?'} | {count:,} |"
        for comp, count in df_real['competence'].value_counts().head(10).items()
    ) + """

**Stratégie NLP** :
- Sources : `competences_brut` + `description` concaténés
- Matching par word boundary regex (`\\b alias \\b`)
- Alias triés par longueur décroissante (évite les faux positifs)
- Dédupliquer par offre : 1 compétence max par offre peu importe le nombre de mentions

**Cas limites** :
- `"node"` vs `"node.js"` : résolu par tri longueur décroissante
- Séparateurs variés (`/`, `•`, `;`) : nettoyés avant matching
- Offres sans description : traçées comme `non_détecté`
"""

    with open(rapport_path, 'w', encoding='utf-8') as f:
        f.write(contenu)

    print(f"\n[MAIN] Rapport pipeline généré : {rapport_path}")


if __name__ == "__main__":
    main()
