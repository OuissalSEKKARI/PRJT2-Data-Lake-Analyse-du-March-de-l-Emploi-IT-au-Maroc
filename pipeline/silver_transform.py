"""
=============================================================
SILVER TRANSFORM — Mexora RH Intelligence
=============================================================
Rôle : Nettoyer et standardiser les données Bronze → Silver

Transformations appliquées :
  1. Normalisation des villes (casa → Casablanca, etc.)
  2. Standardisation des types de contrat
  3. Parsing des salaires (MAD, K, EUR → MAD mensuel)
  4. Parsing de l'expérience en valeur numérique
  5. Normalisation des titres de poste en profils IT
  6. Correction des dates incohérentes
  7. Partitionnement Silver par ville et par mois

Format de sortie : Parquet (compressé Snappy)

Auteur : [Ton Nom]
Date   : Novembre 2024
=============================================================
"""

import json
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime


# ══════════════════════════════════════════════════════════════
# CHARGEMENT DEPUIS BRONZE
# ══════════════════════════════════════════════════════════════

def charger_depuis_bronze(data_lake_root: str) -> pd.DataFrame:
    """
    Charge et consolide toutes les offres depuis la zone Bronze.
    Parcourt tous les fichiers offres_raw.json et les fusionne.
    """
    print("\n[SILVER] Chargement des données depuis Bronze...")
    all_offres = []
    bronze_path = Path(data_lake_root) / 'bronze'
    fichiers_trouves = 0

    for json_file in sorted(bronze_path.rglob('offres_raw.json')):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        offres = data.get('offres', [])
        all_offres.extend(offres)
        fichiers_trouves += 1

    df = pd.DataFrame(all_offres)
    print(f"[SILVER]    → {fichiers_trouves} fichiers Bronze lus")
    print(f"[SILVER]    → {len(df)} offres chargées")
    print(f"[SILVER]    → Colonnes : {list(df.columns)}")
    return df


# ══════════════════════════════════════════════════════════════
# 1. NORMALISATION DES VILLES
# ══════════════════════════════════════════════════════════════

MAPPING_VILLES = {
    # Casablanca
    r'\bcasa\b': 'Casablanca',
    r'casablanca': 'Casablanca',
    r'dar el beida': 'Casablanca',

    # Rabat
    r'rabat': 'Rabat',
    r'rabat[\s\-]salé': 'Rabat',

    # Tanger
    r'tanger': 'Tanger',
    r'tanger[\s\-]assilah': 'Tanger',
    r'tanger[\s\-]tétouan': 'Tanger',

    # Marrakech
    r'marrakech': 'Marrakech',
    r'marrakesh': 'Marrakech',

    # Fès
    r'f[eè]s': 'Fès',
    r'fez': 'Fès',

    # Agadir
    r'agadir': 'Agadir',

    # Meknès
    r'mekn[eè]s': 'Meknès',

    # Autres villes
    r'oujda': 'Oujda',
    r'tétouan': 'Tétouan',
    r'tetouan': 'Tétouan',
    r'kénitra': 'Kénitra',
    r'kenitra': 'Kénitra',
    r'salé': 'Salé',
    r'sale': 'Salé',
}

REGIONS_ADMIN = {
    'Casablanca': 'Casablanca-Settat',
    'Rabat': 'Rabat-Salé-Kénitra',
    'Salé': 'Rabat-Salé-Kénitra',
    'Kénitra': 'Rabat-Salé-Kénitra',
    'Tanger': 'Tanger-Tétouan-Al Hoceïma',
    'Tétouan': 'Tanger-Tétouan-Al Hoceïma',
    'Marrakech': 'Marrakech-Safi',
    'Fès': 'Fès-Meknès',
    'Meknès': 'Fès-Meknès',
    'Agadir': 'Souss-Massa',
    'Oujda': 'Oriental',
}


def normaliser_villes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise les noms de villes :
      - Supprime les espaces superflus
      - Convertit en minuscules pour la comparaison
      - Applique le mapping vers le nom officiel
      - Ajoute la région administrative
    """
    avant = df['ville'].isna().sum()
    df['ville_std'] = df['ville'].fillna('Inconnue').str.strip()

    for pattern, ville_std in MAPPING_VILLES.items():
        masque = df['ville_std'].str.lower().str.contains(pattern, regex=True, na=False)
        df.loc[masque, 'ville_std'] = ville_std

    # Les villes non reconnues → mettre une majuscule propre
    villes_connues = set(MAPPING_VILLES.values())
    masque_inconnu = ~df['ville_std'].isin(villes_connues)
    df.loc[masque_inconnu, 'ville_std'] = (
        df.loc[masque_inconnu, 'ville_std'].str.title()
    )

    # Ajout de la région administrative
    df['region_admin'] = df['ville_std'].map(REGIONS_ADMIN).fillna('Autre')

    apres = (df['ville_std'] == 'Inconnue').sum()
    print(f"[SILVER] Villes normalisées : {avant} nulls → {apres} inconnues")
    print(f"[SILVER]    Top villes : {df['ville_std'].value_counts().head(5).to_dict()}")
    return df


# ══════════════════════════════════════════════════════════════
# 2. STANDARDISATION DES TYPES DE CONTRAT
# ══════════════════════════════════════════════════════════════

MAPPING_CONTRATS = {
    r'cdi|contrat.*dur.*ind[ét]|permanent|ind[ét]terminé': 'CDI',
    r'cdd|contrat.*dur.*d[ét]|temporaire|déterminé': 'CDD',
    r'freelance|indépendant|consultant|mission': 'Freelance',
    r'stage|internship|intern|stagiaire': 'Stage',
    r'alternance|apprentissage': 'Alternance',
}


def standardiser_contrats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise les types de contrat vers des valeurs standardisées :
    CDI, CDD, Freelance, Stage, Alternance, Autre
    """
    df['type_contrat_std'] = 'Autre'
    col = df['type_contrat'].fillna('').str.lower().str.strip()

    for pattern, contrat_std in MAPPING_CONTRATS.items():
        masque = col.str.contains(pattern, regex=True, na=False)
        df.loc[masque, 'type_contrat_std'] = contrat_std

    dist = df['type_contrat_std'].value_counts().to_dict()
    print(f"[SILVER] Contrats standardisés : {dist}")
    return df


# ══════════════════════════════════════════════════════════════
# 3. NORMALISATION DES SALAIRES
# ══════════════════════════════════════════════════════════════

TAUX_EUR_MAD = 10.8  # Taux de change fixe pour 2024


def normaliser_salaires(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrait et normalise les salaires en MAD mensuel brut.

    Règles appliquées :
      - Fourchettes → extraire min et max, calculer médiane
      - "K" → multiplier par 1000 (15K = 15000)
      - EUR → convertir en MAD (taux fixe 1 EUR = 10.8 MAD)
      - "Selon profil", "Confidentiel", null → salaire_connu = False
      - Validation : salaires IT Maroc entre 3 000 et 100 000 MAD
    """

    def parser_salaire(valeur):
        """Retourne (sal_min, sal_max, salaire_connu)"""
        # Valeurs manquantes ou non-numériques
        if pd.isna(valeur):
            return None, None, False

        s = str(valeur).lower().strip()
        s = s.replace('\u202f', '').replace('\xa0', '').replace(' ', '')

        # Valeurs textuelles indiquant un salaire non communiqué
        if any(mot in s for mot in ['null', 'confidentiel', 'selonprofil',
                                     'àncgocier', 'négocier', 'compétitif']):
            return None, None, False

        if not any(c.isdigit() for c in s):
            return None, None, False

        # Détecter la devise
        est_eur = 'eur' in s or '€' in s

        # Nettoyer la devise
        s = re.sub(r'(eur|€|mad|dh|dirham)', '', s)

        # Convertir "K" → milliers (15k → 15000)
        s = re.sub(
            r'(\d+(?:[.,]\d+)?)k',
            lambda m: str(int(float(m.group(1).replace(',', '.')) * 1000)),
            s
        )

        # Extraire tous les nombres
        nombres = re.findall(r'\d+(?:[.,]\d+)?', s)
        if not nombres:
            return None, None, False

        montants = [float(n.replace(',', '.')) for n in nombres]

        # Conversion EUR → MAD si nécessaire
        if est_eur:
            montants = [m * TAUX_EUR_MAD for m in montants]

        # Déterminer min/max
        if len(montants) >= 2:
            sal_min = min(montants[:2])
            sal_max = max(montants[:2])
        else:
            sal_min = sal_max = montants[0]

        # Validation de cohérence (bornes réalistes pour IT Maroc)
        if sal_min < 3000 or sal_max > 100000:
            return None, None, False

        return round(sal_min), round(sal_max), True

    print("[SILVER] Normalisation des salaires...")

    # Appliquer le parseur sur chaque ligne
    resultats = df['salaire_brut'].apply(
        lambda x: pd.Series(
            parser_salaire(x),
            index=['salaire_min_mad', 'salaire_max_mad', 'salaire_connu']
        )
    )
    df = pd.concat([df, resultats], axis=1)
    df['salaire_median_mad'] = (
        (df['salaire_min_mad'] + df['salaire_max_mad']) / 2
    ).round(0)

    pct_connu = df['salaire_connu'].mean() * 100
    nb_connus = df['salaire_connu'].sum()
    print(f"[SILVER]    → {nb_connus} offres avec salaire valide ({pct_connu:.1f}%)")
    print(f"[SILVER]    → {len(df) - nb_connus} offres sans salaire ou invalide")
    return df


# ══════════════════════════════════════════════════════════════
# 4. NORMALISATION DE L'EXPÉRIENCE
# ══════════════════════════════════════════════════════════════

def normaliser_experience(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme l'expérience en valeurs numériques (années).

    Exemples de parsing :
      "3-5 ans"          → experience_min=3, experience_max=5
      "3 à 5 ans"        → experience_min=3, experience_max=5
      "min 3 ans"        → experience_min=3, experience_max=None
      "Débutant accepté" → experience_min=0, experience_max=2
      "Senior (7+ ans)"  → experience_min=7, experience_max=None
      None               → experience_min=None, experience_max=None
    """

    def parser_experience(valeur):
        if pd.isna(valeur):
            return None, None

        s = str(valeur).lower().strip()

        # Cas débutant / junior
        if any(mot in s for mot in ['débutant', 'junior', 'stage',
                                     'sans expérience', '0-1', 'moins d']):
            return 0, 2

        # Cas senior / confirmé / expert
        if any(mot in s for mot in ['senior', 'confirmé', 'expert', 'lead', 'principal']):
            nb = re.search(r'(\d+)', s)
            return (int(nb.group(1)) if nb else 5), None

        # Fourchette : "3-5 ans" ou "3 à 5 ans" ou "3 years to 5 years"
        fourchette = re.search(r'(\d+)\s*(?:[-àa]|to)\s*(\d+)', s)
        if fourchette:
            return int(fourchette.group(1)), int(fourchette.group(2))

        # Minimum seul : "min 3 ans", "au moins 3 ans", "3 ans"
        min_seul = re.search(r'(\d+)\s*(?:ans?|years?|an)', s)
        if min_seul:
            return int(min_seul.group(1)), None

        # Nombre seul sans unité
        nombre = re.search(r'(\d+)', s)
        if nombre:
            return int(nombre.group(1)), None

        return None, None

    resultats = df['experience_requise'].apply(
        lambda x: pd.Series(
            parser_experience(x),
            index=['experience_min_ans', 'experience_max_ans']
        )
    )
    df = pd.concat([df, resultats], axis=1)

    nb_parse = df['experience_min_ans'].notna().sum()
    print(f"[SILVER] Expérience parsée : {nb_parse}/{len(df)} offres ({nb_parse/len(df)*100:.1f}%)")
    return df


# ══════════════════════════════════════════════════════════════
# 5. NORMALISATION DES TITRES DE POSTE
# ══════════════════════════════════════════════════════════════

MAPPING_PROFILS = {
    r'data\s*eng(ineer|ineer\w*|\.)?|ingénieur\s+data|dev\s+data\s+eng|data\s+eng\.': 'Data Engineer',
    r'etl\s*dev|pipeline\s*dev|ingénieur\s+etl': 'Data Engineer',
    r'big\s*data': 'Data Engineer',

    r'data\s*anal(yst|yste|ytics)?|analyste?\s+data|bi\s+anal': 'Data Analyst',
    r'business\s+intel(ligence)?|ingénieur\s+bi|développeur\s+bi|bi\s+dev': 'Data Analyst',
    r'reporting\s+(anal|spec|officer)|analyste\s+reporting': 'Data Analyst',
    r'analyste\s+données?': 'Data Analyst',

    r'data\s*sci(entist|ence)?|machine\s*learn|ml\s*eng|ia\s*eng': 'Data Scientist',
    r'deep\s*learn|nlp\s*eng|computer\s*vision|intelligence\s+artificielle': 'Data Scientist',

    r'full\s*stack|fullstack': 'Développeur Full Stack',
    r'back[\s\-]*end|backend': 'Développeur Backend',
    r'front[\s\-]*end|frontend': 'Développeur Frontend',
    r'dev(eloppeur|eloper)?\s+mobile|ios\s+dev|android\s+dev|mobile\s+dev': 'Développeur Mobile',
    r'flutter\s+dev|react\s+native\s+dev': 'Développeur Mobile',

    r'devops|sre|site\s*reliab': 'DevOps / SRE',
    r'cloud\s*(arch|eng|admin)|aws\s+eng|gcp\s+eng|azure\s+eng': 'Cloud Engineer',
    r'sys(admin|tème)|réseau\s+inf|network\s+eng|admin\s+sys': 'Admin Systèmes & Réseaux',

    r'cyber|sécurité\s+info|pentester|soc\s+anal': 'Cybersécurité',

    r'chef\s+de\s+proj(et)?|project\s+man|scrum\s*master': 'Chef de Projet IT',
    r'architect(e)?\s+(log|tech|data|cloud|sol)': 'Architecte IT',

    r'développeur?\s+php|php\s+dev': 'Développeur Backend',
    r'développeur?\s+\.?net|\.net\s+dev': 'Développeur Backend',
    r'java\s+dev|développeur?\s+java': 'Développeur Backend',

    r'testeur|qa\s+eng|quality\s+assur|test\s+eng': 'QA / Test Engineer',
    r'tech\s+lead|lead\s+dev|lead\s+eng': 'Tech Lead',
}


def normaliser_titres_postes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise les intitulés de poste vers des profils IT standardisés.
    Les titres non reconnus sont classés 'Autre IT'.
    """
    df['profil_normalise'] = 'Autre IT'
    df['titre_normalise_src'] = df['titre_poste'].fillna('').str.lower().str.strip()

    for pattern, profil in MAPPING_PROFILS.items():
        masque = df['titre_normalise_src'].str.contains(
            pattern, regex=True, na=False
        )
        df.loc[masque, 'profil_normalise'] = profil

    # Statistiques
    dist = df['profil_normalise'].value_counts()
    non_classes = (df['profil_normalise'] == 'Autre IT').sum()

    print(f"[SILVER] Profils normalisés :")
    for profil, count in dist.head(10).items():
        print(f"           {profil:35s} : {count:4d}")
    print(f"[SILVER]    → {non_classes} offres classées 'Autre IT' sur {len(df)}")

    return df


# ══════════════════════════════════════════════════════════════
# 6. NETTOYAGE DES DATES
# ══════════════════════════════════════════════════════════════

def normaliser_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise les dates au format YYYY-MM-DD.
    Détecte et corrige les incohérences (publication > expiration).
    Extrait l'année et le mois pour le partitionnement.
    """
    formats_date = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']

    def parser_date(valeur):
        if pd.isna(valeur) or str(valeur).strip() == '':
            return None
        for fmt in formats_date:
            try:
                return datetime.strptime(str(valeur).strip(), fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    df['date_publication_std'] = df['date_publication'].apply(parser_date)
    df['date_expiration_std'] = df['date_expiration'].apply(parser_date)

    # Détecter les incohérences : date_pub > date_exp
    masque_incoherent = (
        df['date_publication_std'].notna() &
        df['date_expiration_std'].notna() &
        (df['date_publication_std'] > df['date_expiration_std'])
    )
    nb_incoherents = masque_incoherent.sum()

    # Corriger : invalider la date d'expiration incohérente
    df.loc[masque_incoherent, 'date_expiration_std'] = None
    df.loc[masque_incoherent, 'date_anomalie'] = True
    df['date_anomalie'] = df['date_anomalie'].fillna(False)

    # Extraire année et mois
    df['annee'] = df['date_publication_std'].str[:4]
    df['mois'] = df['date_publication_std'].str[5:7]

    print(f"[SILVER] Dates : {nb_incoherents} incohérences corrigées (pub > exp)")
    print(f"[SILVER]    Dates valides : {df['date_publication_std'].notna().sum()}/{len(df)}")

    return df


# ══════════════════════════════════════════════════════════════
# PIPELINE COMPLET SILVER
# ══════════════════════════════════════════════════════════════

def transformer_silver(data_lake_root: str) -> pd.DataFrame:
    """
    Pipeline complet Bronze → Silver.
    Applique toutes les transformations dans l'ordre logique.
    """
    print("\n" + "=" * 60)
    print("[SILVER] Démarrage du pipeline de transformation Silver")
    print("=" * 60)

    df = charger_depuis_bronze(data_lake_root)
    nb_initial = len(df)

    print(f"\n--- Étape 1/6 : Normalisation des dates ---")
    df = normaliser_dates(df)

    print(f"\n--- Étape 2/6 : Normalisation des villes ---")
    df = normaliser_villes(df)

    print(f"\n--- Étape 3/6 : Standardisation des contrats ---")
    df = standardiser_contrats(df)

    print(f"\n--- Étape 4/6 : Normalisation des salaires ---")
    df = normaliser_salaires(df)

    print(f"\n--- Étape 5/6 : Normalisation de l'expérience ---")
    df = normaliser_experience(df)

    print(f"\n--- Étape 6/6 : Normalisation des titres de poste ---")
    df = normaliser_titres_postes(df)

    print(f"\n[SILVER] ✅ Pipeline terminé : {nb_initial} → {len(df)} offres")
    return df


def sauvegarder_silver_offres(df: pd.DataFrame, data_lake_root: str):
    """
    Sauvegarde le DataFrame Silver au format Parquet (compressé Snappy).
    Snappy offre un bon compromis vitesse de lecture / taux de compression.
    """
    silver_path = Path(data_lake_root) / 'silver' / 'offres_clean'
    silver_path.mkdir(parents=True, exist_ok=True)

    chemin = silver_path / 'offres_clean.parquet'
    df.to_parquet(chemin, index=False, compression='snappy')

    taille_ko = chemin.stat().st_size // 1024
    print(f"\n[SILVER] Sauvegardé : offres_clean.parquet ({taille_ko} Ko)")
    print(f"[SILVER] Colonnes Silver : {list(df.columns)}")
    print(f"[SILVER] Lignes : {len(df)}")
    return chemin


if __name__ == "__main__":
    BASE_DIR = Path(__file__).parent.parent
    LAKE = BASE_DIR / 'data_lake'

    df_silver = transformer_silver(str(LAKE))
    chemin = sauvegarder_silver_offres(df_silver, str(LAKE))

    print(f"\n[SILVER] Aperçu du fichier Parquet Silver :")
    print(df_silver[['id_offre', 'profil_normalise', 'ville_std',
                      'type_contrat_std', 'salaire_median_mad',
                      'experience_min_ans', 'annee']].head(10).to_string())
