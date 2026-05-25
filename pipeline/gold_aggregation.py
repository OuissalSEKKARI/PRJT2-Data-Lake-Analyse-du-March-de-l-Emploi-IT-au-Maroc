"""
=============================================================
GOLD AGGREGATION — Mexora RH Intelligence
=============================================================
Rôle : Construire les tables analytiques Gold depuis Silver

Tables produites (5) :
  1. top_competences.parquet       — Compétences les plus demandées par profil
  2. salaires_par_profil.parquet   — Statistiques salariales par profil/ville/contrat
  3. offres_par_ville.parquet      — Volume d'offres par ville, profil et mois
  4. entreprises_recruteurs.parquet — Top entreprises recruteurs
  5. tendances_mensuelles.parquet  — Évolution mensuelle par profil

Outil : DuckDB (requêtes SQL directement sur fichiers Parquet)
Format de sortie : Parquet (Snappy)

Usage :
  python pipeline/gold_aggregation.py
  ou appelé depuis main.py via construire_gold()

=============================================================
"""

import duckdb
from pathlib import Path
from datetime import datetime


# ══════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════

def construire_gold(data_lake_root: str) -> dict:
    """
    Construit toutes les tables Gold depuis les données Silver.
    Utilise DuckDB pour les requêtes SQL directement sur les fichiers Parquet.

    Arguments :
        data_lake_root : chemin vers le répertoire racine du Data Lake

    Retourne :
        dict : statistiques de construction (nb lignes par table)
    """
    print("=" * 60)
    print("[GOLD] Démarrage de la construction des tables Gold")
    print("=" * 60)

    # ── Chemins ──────────────────────────────────────────────────
    silver_offres = f"{data_lake_root}/silver/offres_clean/offres_clean.parquet"
    silver_comp   = f"{data_lake_root}/silver/competences_extraites/competences.parquet"
    gold_path     = Path(data_lake_root) / 'gold'
    gold_path.mkdir(parents=True, exist_ok=True)

    # Vérification des fichiers source
    if not Path(silver_offres).exists():
        raise FileNotFoundError(
            f"[GOLD] ❌ Fichier Silver introuvable : {silver_offres}\n"
            "         Exécutez d'abord : python pipeline/silver_transform.py"
        )
    if not Path(silver_comp).exists():
        raise FileNotFoundError(
            f"[GOLD] ❌ Fichier Silver introuvable : {silver_comp}\n"
            "         Exécutez d'abord : python pipeline/silver_nlp.py"
        )

    print(f"\n[GOLD] Source offres  : {silver_offres}")
    print(f"[GOLD] Source compét. : {silver_comp}")
    print(f"[GOLD] Destination    : {gold_path}\n")

    # Connexion DuckDB (en mémoire)
    con = duckdb.connect()

    stats = {}

    # ── TABLE 1 : Top compétences par profil ─────────────────────
    print("[GOLD] Construction top_competences...")
    df_top_comp = con.execute(f"""
        SELECT
            profil,
            famille,
            competence,
            COUNT(DISTINCT id_offre)                               AS nb_offres_mentionnent,
            ROUND(
                COUNT(DISTINCT id_offre) * 100.0 /
                (SELECT COUNT(DISTINCT id_offre) FROM '{silver_offres}'),
                2
            )                                                      AS pct_offres_total,
            RANK() OVER (
                PARTITION BY profil
                ORDER BY COUNT(DISTINCT id_offre) DESC
            )                                                      AS rang_dans_profil
        FROM '{silver_comp}'
        WHERE competence != 'non_détecté'
        GROUP BY profil, famille, competence
        ORDER BY profil, rang_dans_profil
    """).df()

    chemin_top_comp = gold_path / 'top_competences.parquet'
    df_top_comp.to_parquet(chemin_top_comp, index=False)
    stats['top_competences'] = len(df_top_comp)
    print(f"[GOLD]    → {len(df_top_comp):,} lignes — top_competences.parquet ✓")

    # ── TABLE 2 : Salaires par profil, ville et type de contrat ──
    print("[GOLD] Construction salaires_par_profil...")
    df_salaires = con.execute(f"""
        SELECT
            profil_normalise                                        AS profil,
            ville_std                                               AS ville,
            type_contrat_std                                        AS type_contrat,
            COUNT(*)                                                AS nb_offres,
            COUNT(*) FILTER (WHERE salaire_connu)                  AS nb_offres_avec_salaire,
            ROUND(
                MEDIAN(salaire_median_mad) FILTER (WHERE salaire_connu),
                0
            )                                                      AS salaire_median_mad,
            ROUND(
                AVG(salaire_median_mad)    FILTER (WHERE salaire_connu),
                0
            )                                                      AS salaire_moyen_mad,
            ROUND(
                PERCENTILE_CONT(0.25) WITHIN GROUP
                    (ORDER BY salaire_median_mad)
                    FILTER (WHERE salaire_connu),
                0
            )                                                      AS salaire_q1_mad,
            ROUND(
                PERCENTILE_CONT(0.75) WITHIN GROUP
                    (ORDER BY salaire_median_mad)
                    FILTER (WHERE salaire_connu),
                0
            )                                                      AS salaire_q3_mad,
            ROUND(
                MIN(salaire_min_mad) FILTER (WHERE salaire_connu),
                0
            )                                                      AS salaire_min_observe,
            ROUND(
                MAX(salaire_max_mad) FILTER (WHERE salaire_connu),
                0
            )                                                      AS salaire_max_observe
        FROM '{silver_offres}'
        GROUP BY profil_normalise, ville_std, type_contrat_std
        HAVING COUNT(*) >= 5
        ORDER BY nb_offres DESC
    """).df()

    chemin_salaires = gold_path / 'salaires_par_profil.parquet'
    df_salaires.to_parquet(chemin_salaires, index=False)
    stats['salaires_par_profil'] = len(df_salaires)
    print(f"[GOLD]    → {len(df_salaires):,} lignes — salaires_par_profil.parquet ✓")

    # ── TABLE 3 : Volume d'offres par ville, profil et mois ──────
    print("[GOLD] Construction offres_par_ville...")
    df_villes = con.execute(f"""
        SELECT
            ville_std                                               AS ville,
            region_admin,
            profil_normalise                                        AS profil,
            annee,
            mois,
            COUNT(*)                                                AS nb_offres,
            COUNT(*) FILTER (
                WHERE teletravail ILIKE '%télétravail%'
                   OR teletravail ILIKE '%remote%'
                   OR teletravail ILIKE '%hybride%'
            )                                                       AS nb_offres_remote,
            ROUND(
                COUNT(*) FILTER (
                    WHERE teletravail ILIKE '%télétravail%'
                       OR teletravail ILIKE '%remote%'
                       OR teletravail ILIKE '%hybride%'
                ) * 100.0 / NULLIF(COUNT(*), 0),
                1
            )                                                       AS pct_remote
        FROM '{silver_offres}'
        GROUP BY ville_std, region_admin, profil_normalise, annee, mois
        ORDER BY nb_offres DESC
    """).df()

    chemin_villes = gold_path / 'offres_par_ville.parquet'
    df_villes.to_parquet(chemin_villes, index=False)
    stats['offres_par_ville'] = len(df_villes)
    print(f"[GOLD]    → {len(df_villes):,} lignes — offres_par_ville.parquet ✓")

    # ── TABLE 4 : Entreprises les plus recruteurs ─────────────────
    print("[GOLD] Construction entreprises_recruteurs...")
    df_entreprises = con.execute(f"""
        SELECT
            entreprise,
            ville_std                                               AS ville,
            COUNT(*)                                                AS nb_offres_publiees,
            COUNT(DISTINCT profil_normalise)                        AS nb_profils_differents,
            ROUND(
                AVG(salaire_median_mad) FILTER (WHERE salaire_connu),
                0
            )                                                       AS salaire_moyen_propose,
            ARRAY_AGG(DISTINCT profil_normalise
                      ORDER BY profil_normalise)                    AS profils_recrutes,
            MIN(date_publication_std)                               AS premiere_offre,
            MAX(date_publication_std)                               AS derniere_offre
        FROM '{silver_offres}'
        WHERE entreprise IS NOT NULL
          AND entreprise != ''
        GROUP BY entreprise, ville_std
        HAVING COUNT(*) >= 3
        ORDER BY nb_offres_publiees DESC
        LIMIT 100
    """).df()

    chemin_entreprises = gold_path / 'entreprises_recruteurs.parquet'
    df_entreprises.to_parquet(chemin_entreprises, index=False)
    stats['entreprises_recruteurs'] = len(df_entreprises)
    print(f"[GOLD]    → {len(df_entreprises):,} lignes — entreprises_recruteurs.parquet ✓")

    # ── TABLE 5 : Tendances mensuelles ───────────────────────────
    print("[GOLD] Construction tendances_mensuelles...")
    df_tendances = con.execute(f"""
        SELECT
            annee,
            mois,
            profil_normalise                                        AS profil,
            COUNT(*)                                                AS nb_offres,
            ROUND(
                AVG(salaire_median_mad) FILTER (WHERE salaire_connu),
                0
            )                                                       AS salaire_moyen_mois,
            LAG(COUNT(*)) OVER (
                PARTITION BY profil_normalise
                ORDER BY annee, mois
            )                                                       AS nb_offres_mois_precedent,
            ROUND(
                (COUNT(*) - LAG(COUNT(*)) OVER (
                    PARTITION BY profil_normalise
                    ORDER BY annee, mois
                )) * 100.0 / NULLIF(
                    LAG(COUNT(*)) OVER (
                        PARTITION BY profil_normalise
                        ORDER BY annee, mois
                    ), 0
                ),
                1
            )                                                       AS evolution_pct
        FROM '{silver_offres}'
        WHERE annee IS NOT NULL AND mois IS NOT NULL
        GROUP BY annee, mois, profil_normalise
        ORDER BY profil_normalise, annee, mois
    """).df()

    chemin_tendances = gold_path / 'tendances_mensuelles.parquet'
    df_tendances.to_parquet(chemin_tendances, index=False)
    stats['tendances_mensuelles'] = len(df_tendances)
    print(f"[GOLD]    → {len(df_tendances):,} lignes — tendances_mensuelles.parquet ✓")

    con.close()

    # ── Rapport de construction ───────────────────────────────────
    print(f"\n[GOLD] ✅ Construction terminée — {sum(stats.values()):,} lignes au total")
    print(f"[GOLD] Tables produites dans : {gold_path}")
    for table, nb in stats.items():
        taille_ko = (gold_path / f'{table}.parquet').stat().st_size // 1024
        print(f"         {table:<35s} : {nb:>6,} lignes  ({taille_ko} Ko)")

    return stats


# ══════════════════════════════════════════════════════════════
# VÉRIFICATION POST-CONSTRUCTION
# ══════════════════════════════════════════════════════════════

def verifier_gold(data_lake_root: str) -> None:
    """
    Vérifie rapidement que les 5 tables Gold sont lisibles
    et affiche un aperçu de chacune.
    """
    gold_path = Path(data_lake_root) / 'gold'
    tables = [
        'top_competences',
        'salaires_par_profil',
        'offres_par_ville',
        'entreprises_recruteurs',
        'tendances_mensuelles',
    ]

    con = duckdb.connect()
    print("\n[GOLD] Vérification des tables Gold :")

    for table in tables:
        chemin = gold_path / f'{table}.parquet'
        if not chemin.exists():
            print(f"   ❌ {table}.parquet — MANQUANT")
            continue

        nb = con.execute(f"SELECT COUNT(*) FROM '{chemin}'").fetchone()[0]
        cols = con.execute(f"DESCRIBE SELECT * FROM '{chemin}'").df()['column_name'].tolist()
        print(f"   ✓  {table:<35s} {nb:>6,} lignes | {len(cols)} colonnes")
        print(f"      Colonnes : {', '.join(cols)}")

    con.close()


# ══════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    BASE_DIR = Path(__file__).parent.parent
    LAKE     = BASE_DIR / 'data_lake'

    print("=" * 60)
    print("  GOLD AGGREGATION — Mexora RH Intelligence")
    print(f"  Démarrage : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    stats = construire_gold(str(LAKE))
    verifier_gold(str(LAKE))

    print(f"\n  Fin : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)