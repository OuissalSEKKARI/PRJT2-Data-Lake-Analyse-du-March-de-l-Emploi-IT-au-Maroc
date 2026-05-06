"""
=============================================================
BRONZE INGESTION — Mexora RH Intelligence
=============================================================

"""

import json
import os
from datetime import datetime
from pathlib import Path


def ingerer_bronze(filepath_source: str, data_lake_root: str) -> dict:
    """
    Charge les données brutes dans la zone Bronze sans aucune modification.
    Partitionne par source et par mois de publication.

    Arguments :
        filepath_source : chemin vers le fichier JSON source
        data_lake_root  : répertoire racine du Data Lake

    Retourne :
        dict : statistiques d'ingestion (total, par source, par mois)
    """
    print("=" * 60)
    print("[BRONZE] Démarrage de l'ingestion Bronze")
    print(f"[BRONZE] Source : {filepath_source}")
    print("=" * 60)

    # ── Chargement du fichier source ─────────────────────────────
    with open(filepath_source, 'r', encoding='utf-8') as f:
        data = json.load(f)

    offres = data.get('offres', [])
    print(f"[BRONZE] {len(offres)} offres trouvées dans le fichier source")

    stats = {
        'total': len(offres),
        'par_source': {},
        'par_mois': {},
        'nb_fichiers_crees': 0,
        'date_ingestion': datetime.now().isoformat()
    }

    # ── Partitionnement : source / mois ──────────────────────────
    # On regroupe les offres par (source, mois_publication)
    # pour créer un fichier JSON par partition
    partitions = {}

    for offre in offres:
        # Récupérer la source de l'offre
        source = offre.get('source', 'inconnu').lower().strip().replace(' ', '_')

        # Récupérer la date de publication (peut être dans différents formats)
        date_pub = offre.get('date_publication', '')

        # Essayer de parser la date pour extraire AAAA_MM
        mois_partition = 'date_inconnue'
        if date_pub:
            # Essayer différents formats de date présents dans les données
            formats_date = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']
            for fmt in formats_date:
                try:
                    dt = datetime.strptime(str(date_pub).strip(), fmt)
                    mois_partition = dt.strftime('%Y_%m')
                    break
                except ValueError:
                    continue

        cle_partition = f"{source}/{mois_partition}"

        if cle_partition not in partitions:
            partitions[cle_partition] = []
        partitions[cle_partition].append(offre)

    # ── Écriture dans Bronze ─────────────────────────────────────
    # IMPORTANT : on écrit les offres TELLES QUELLES, aucune modification
    nb_fichiers = 0

    for partition, offres_partition in sorted(partitions.items()):
        chemin_dir = Path(data_lake_root) / 'bronze' / partition
        chemin_dir.mkdir(parents=True, exist_ok=True)

        chemin_fichier = chemin_dir / 'offres_raw.json'

        # On ajoute uniquement des métadonnées d'ingestion (hors données)
        contenu_fichier = {
            'metadata': {
                'source_fichier': filepath_source,
                'date_ingestion': datetime.now().isoformat(),
                'partition': partition,
                'nb_offres': len(offres_partition),
                'note': 'Données brutes — non modifiées — zone IMMUABLE'
            },
            'offres': offres_partition  # ← données originales intactes
        }

        with open(chemin_fichier, 'w', encoding='utf-8') as f:
            json.dump(contenu_fichier, f, ensure_ascii=False, indent=2)

        nb_fichiers += 1

        # Mise à jour des stats
        source_nom = partition.split('/')[0]
        mois_nom = partition.split('/')[1] if '/' in partition else 'inconnu'

        stats['par_source'][source_nom] = (
            stats['par_source'].get(source_nom, 0) + len(offres_partition)
        )
        stats['par_mois'][mois_nom] = (
            stats['par_mois'].get(mois_nom, 0) + len(offres_partition)
        )

    stats['nb_fichiers_crees'] = nb_fichiers

    # ── Rapport d'ingestion ───────────────────────────────────────
    print(f"\n[BRONZE] ✅ Ingestion terminée")
    print(f"[BRONZE]    → {stats['total']} offres ingérées")
    print(f"[BRONZE]    → {nb_fichiers} partitions créées")
    print(f"\n[BRONZE] Répartition par source :")
    for src, count in sorted(stats['par_source'].items()):
        print(f"           {src:20s} : {count:4d} offres")

    print(f"\n[BRONZE] Répartition par mois (extrait) :")
    for mois, count in sorted(stats['par_mois'].items())[:6]:
        print(f"           {mois:15s} : {count:4d} offres")
    if len(stats['par_mois']) > 6:
        print(f"           ... et {len(stats['par_mois']) - 6} autres mois")

    return stats


def verifier_bronze(data_lake_root: str) -> dict:
    """
    Vérifie l'intégrité de la zone Bronze après ingestion.
    Compte les fichiers et les offres pour confirmer que rien n'a été perdu.
    """
    bronze_path = Path(data_lake_root) / 'bronze'
    total_offres = 0
    total_fichiers = 0
    sources_trouvees = set()

    for json_file in sorted(bronze_path.rglob('offres_raw.json')):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        nb = data['metadata']['nb_offres']
        total_offres += nb
        total_fichiers += 1

        # Extraire la source depuis le chemin
        parties = json_file.parts
        for i, partie in enumerate(parties):
            if partie == 'bronze' and i + 1 < len(parties):
                sources_trouvees.add(parties[i + 1])
                break

    print(f"\n[BRONZE] Vérification d'intégrité :")
    print(f"         Fichiers JSON    : {total_fichiers}")
    print(f"         Offres totales   : {total_offres}")
    print(f"         Sources          : {', '.join(sorted(sources_trouvees))}")

    return {
        'total_offres': total_offres,
        'total_fichiers': total_fichiers,
        'sources': list(sources_trouvees)
    }


if __name__ == "__main__":
    # ── Chemins ──────────────────────────────────────────────────
    BASE_DIR = Path(__file__).parent.parent
    SOURCE   = BASE_DIR / 'data_sources' / 'offres_emploi_it_maroc.json'
    LAKE     = BASE_DIR / 'data_lake'

    # ── Exécution ─────────────────────────────────────────────────
    stats = ingerer_bronze(str(SOURCE), str(LAKE))
    verifier_bronze(str(LAKE))
