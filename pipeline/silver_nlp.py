"""
=============================================================
SILVER NLP — Mexora RH Intelligence
=============================================================
Rôle : Extraction des compétences IT depuis le texte libre

C'est la transformation la plus originale du projet.
Elle convertit du texte non structuré (description, compétences_brut)
en données structurées (une ligne par compétence détectée).

Stratégie d'extraction :
  1. Concaténer 'competences_brut' et 'description' de chaque offre
  2. Nettoyer le texte (normalisation, ponctuation)
  3. Faire correspondre avec le référentiel de compétences
     via des expressions régulières (word boundary matching)
  4. Dédupliquer les compétences par offre

Sortie : DataFrame long format (1 ligne = 1 compétence dans 1 offre)

Auteur : [Ton Nom]
Date   : Novembre 2024
=============================================================
"""

import json
import re
import pandas as pd
import pyarrow
from pathlib import Path


# ══════════════════════════════════════════════════════════════
# CHARGEMENT DU RÉFÉRENTIEL
# ══════════════════════════════════════════════════════════════

def charger_referentiel(referentiel_path: str) -> dict:
    """
    Charge le référentiel de compétences IT et construit
    un dictionnaire plat alias → {competence, famille}.

    Exemple :
      "pyspark" → {competence: "spark", famille: "data_engineering"}
      "reactjs"  → {competence: "react", famille: "frameworks_web"}

    Les alias sont triés par longueur décroissante pour éviter
    les faux positifs (ex: "node" avant "node.js").
    """
    with open(referentiel_path, 'r', encoding='utf-8') as f:
        referentiel = json.load(f)

    dict_competences = {}  # alias.lower() → {competence, famille}

    for famille, competences in referentiel['familles'].items():
        for nom_normalise, aliases in competences.items():
            for alias in aliases:
                alias_lower = alias.lower().strip()
                dict_competences[alias_lower] = {
                    'competence': nom_normalise,
                    'famille': famille
                }

    # Trier par longueur décroissante : important pour éviter
    # que "node" matche avant "node.js"
    aliases_tries = sorted(dict_competences.keys(), key=len, reverse=True)

    nb_aliases = len(dict_competences)
    nb_familles = len(referentiel['familles'])
    print(f"[NLP] Référentiel chargé : {nb_aliases} alias, {nb_familles} familles")

    return dict_competences, aliases_tries


# ══════════════════════════════════════════════════════════════
# NETTOYAGE DU TEXTE
# ══════════════════════════════════════════════════════════════

def nettoyer_texte(texte: str) -> str:
    """
    Prépare le texte pour l'extraction de compétences.

    Transformations :
      - Passage en minuscules
      - Séparateurs variés → espaces (/, •, ;, |, \n)
      - Suppression des accents qui pourraient gêner le matching
      - Conservation des caractères alphanumériques, espaces, points, tirets
    """
    if not texte or not isinstance(texte, str):
        return ''

    t = texte.lower()

    # Remplacer séparateurs courants par des espaces
    t = re.sub(r'[/•;|\\]', ' ', t)
    t = re.sub(r'\n+', ' ', t)

    # Normaliser les espaces multiples
    t = re.sub(r'\s+', ' ', t)

    # Conserver lettres, chiffres, espaces, points, tirets, #
    # (# pour C#, . pour Node.js)
    t = re.sub(r'[^\w\s.\-#]', ' ', t)

    return t.strip()


# ══════════════════════════════════════════════════════════════
# EXTRACTION PRINCIPALE
# ══════════════════════════════════════════════════════════════

def extraire_competences(
    df: pd.DataFrame,
    referentiel_path: str
) -> pd.DataFrame:
    """
    Extrait les compétences IT depuis deux sources textuelles :
      1. 'competences_brut' : liste semi-structurée (plus fiable)
      2. 'description'      : texte libre (plus riche, plus bruité)

    Pour chaque offre, produit N lignes dans le DataFrame résultat
    (une ligne par compétence unique détectée).

    Si aucune compétence n'est trouvée, insère une ligne avec
    competence='non_détecté' pour traçabilité.

    Arguments :
        df               : DataFrame Silver des offres nettoyées
        referentiel_path : chemin vers referentiel_competences_it.json

    Retourne :
        DataFrame au format long :
        id_offre | profil | ville | competence | famille | date_pub | annee | mois
    """
    dict_comp, aliases_tries = charger_referentiel(referentiel_path)

    resultats = []
    nb_sans_comp = 0
    nb_avec_comp = 0

    print(f"\n[NLP] Extraction des compétences pour {len(df)} offres...")
    print(f"[NLP] Sources : 'competences_brut' + 'description'")

    for idx, offre in df.iterrows():

        # ── Construire le texte source ────────────────────────
        texte_comp_brut = str(offre.get('competences_brut') or '')
        texte_description = str(offre.get('description') or '')

        # Concaténer et nettoyer
        texte_complet = nettoyer_texte(texte_comp_brut + ' ' + texte_description)

        # ── Recherche de compétences ──────────────────────────
        competences_trouvees = {}  # nom_normalise → info (pour dédupliquer)

        for alias in aliases_tries:
            # word boundary : évite de matcher "sql" dans "nosql"
            # re.escape : gère les caractères spéciaux (node.js, c#)
            try:
                pattern = r'\b' + re.escape(alias) + r'\b'
                if re.search(pattern, texte_complet):
                    info = dict_comp[alias]
                    nom_normalise = info['competence']

                    # Garder seulement la première occurrence (via alias le plus long)
                    if nom_normalise not in competences_trouvees:
                        competences_trouvees[nom_normalise] = info
            except re.error:
                # En cas d'alias avec caractère spécial non géré
                continue

        # ── Construire les lignes résultat ────────────────────
        date_pub = str(offre.get('date_publication_std') or
                       offre.get('date_publication') or '')

        info_commune = {
            'id_offre': offre.get('id_offre'),
            'profil':   offre.get('profil_normalise', 'Autre IT'),
            'ville':    offre.get('ville_std', 'Inconnue'),
            'date_pub': date_pub[:10] if date_pub else None,
            'annee':    date_pub[:4] if date_pub else None,
            'mois':     date_pub[5:7] if len(date_pub) >= 7 else None,
            'source':   offre.get('source'),
            'entreprise': offre.get('entreprise'),
        }

        if competences_trouvees:
            nb_avec_comp += 1
            for nom_comp, info in competences_trouvees.items():
                ligne = {**info_commune,
                         'competence': nom_comp,
                         'famille': info['famille']}
                resultats.append(ligne)
        else:
            # Traçabilité : offre sans compétence détectée
            nb_sans_comp += 1
            ligne = {**info_commune,
                     'competence': 'non_détecté',
                     'famille': 'inconnu'}
            resultats.append(ligne)

        # Affichage progression tous les 1000 offres
        if (idx + 1) % 1000 == 0:
            print(f"[NLP]    → {idx + 1}/{len(df)} offres traitées...")

    # ── Résumé ────────────────────────────────────────────────
    df_comp = pd.DataFrame(resultats)

    total_lignes = len(df_comp)
    lignes_reelles = df_comp[df_comp['competence'] != 'non_détecté']
    nb_competences_uniques = lignes_reelles['competence'].nunique()
    moy_comp_par_offre = len(lignes_reelles) / max(nb_avec_comp, 1)

    print(f"\n[NLP] ✅ Extraction terminée :")
    print(f"[NLP]    → {total_lignes:,} lignes générées")
    print(f"[NLP]    → {nb_avec_comp:,}/{len(df)} offres avec au moins 1 compétence")
    print(f"[NLP]    → {nb_sans_comp:,} offres sans compétence détectée")
    print(f"[NLP]    → {nb_competences_uniques} compétences uniques détectées")
    print(f"[NLP]    → {moy_comp_par_offre:.1f} compétences en moyenne par offre")

    # Distribution par famille
    print(f"\n[NLP] Distribution par famille :")
    dist_familles = (
        lignes_reelles['famille'].value_counts().head(10)
    )
    for famille, count in dist_familles.items():
        print(f"           {famille:25s} : {count:5d} mentions")

    return df_comp


# ══════════════════════════════════════════════════════════════
# SAUVEGARDE SILVER COMPÉTENCES
# ══════════════════════════════════════════════════════════════

def sauvegarder_silver_competences(
    df_comp: pd.DataFrame,
    data_lake_root: str
) -> Path:
    """
    Sauvegarde le DataFrame de compétences en format Parquet.

    Le format long (1 ligne = 1 compétence × offre) est optimal
    pour les requêtes analytiques de type COUNT / GROUP BY sur DuckDB.
    """
    silver_path = Path(data_lake_root) / 'silver' / 'competences_extraites'
    silver_path.mkdir(parents=True, exist_ok=True)

    chemin = silver_path / 'competences.parquet'
    df_comp.to_parquet(chemin, index=False, compression='snappy')

    taille_ko = chemin.stat().st_size // 1024
    print(f"\n[NLP] Sauvegardé : competences.parquet ({taille_ko} Ko)")

    return chemin


def generer_rapport_nlp(df_comp: pd.DataFrame) -> str:
    """
    Génère un résumé statistique de l'extraction NLP.
    Utile pour le rapport_pipeline.md.
    """
    df_reelles = df_comp[df_comp['competence'] != 'non_détecté']

    top_comp = df_reelles['competence'].value_counts().head(15)
    top_familles = df_reelles['famille'].value_counts()

    rapport = []
    rapport.append("## Résumé extraction NLP Silver")
    rapport.append(f"- Lignes totales : {len(df_comp):,}")
    rapport.append(f"- Compétences réelles : {len(df_reelles):,}")
    rapport.append(f"- Offres avec compétence : {df_reelles['id_offre'].nunique():,}")
    rapport.append(f"- Compétences uniques : {df_reelles['competence'].nunique()}")
    rapport.append("")
    rapport.append("### Top 15 compétences :")
    for comp, count in top_comp.items():
        rapport.append(f"  - {comp}: {count}")
    rapport.append("")
    rapport.append("### Distribution par famille :")
    for famille, count in top_familles.items():
        rapport.append(f"  - {famille}: {count}")

    return "\n".join(rapport)


# ══════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import pyarrow.parquet as pq

    BASE_DIR = Path(__file__).parent.parent
    LAKE = BASE_DIR / 'data_lake'
    REFERENTIEL = BASE_DIR / 'data_sources' / 'referentiel_competences_it.json'

    # Charger le Silver des offres nettoyées
    silver_offres_path = LAKE / 'silver' / 'offres_clean' / 'offres_clean.parquet'
    if not silver_offres_path.exists():
        print("[NLP] ❌ Fichier Silver offres non trouvé.")
        print("[NLP]    Exécutez d'abord : python pipeline/silver_transform.py")
        exit(1)

    print(f"[NLP] Chargement : {silver_offres_path}")
    df_silver = pd.read_parquet(silver_offres_path)
    print(f"[NLP] {len(df_silver)} offres chargées depuis Silver")

    # Extraction des compétences
    df_comp = extraire_competences(df_silver, str(REFERENTIEL))

    # Sauvegarde
    chemin = sauvegarder_silver_competences(df_comp, str(LAKE))

    # Rapport
    rapport = generer_rapport_nlp(df_comp)
    print("\n" + rapport)

    # Aperçu
    print(f"\n[NLP] Aperçu des 10 premières lignes :")
    print(df_comp.head(10).to_string())
