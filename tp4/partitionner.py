"""TP 4 - Mission C : partitionnement year=/month=/day=/country= et risque des partitions trop fines.

1. Écrit les 30 jours d'événements en Parquet, partitionnés comme proposé dans le TP :
   events/year=AAAA/month=MM/day=JJ/country=XX/part-0.parquet
2. Montre l'élagage de partitions (partition pruning) : une requête « un jour, un pays » ne lit qu'un fichier.
3. Chiffre le nombre de fichiers et leur taille moyenne selon la finesse du partitionnement.
Usage : python3 partitionner.py [dossier_de_sortie]
"""
import os, shutil, sys
import pyarrow as pa
import pyarrow.dataset as ds

from donnees import generer_evenements

SORTIE = sys.argv[1] if len(sys.argv) > 1 else "/content/tp4/events_partitionnes"
shutil.rmtree(SORTIE, ignore_errors=True)

df = generer_evenements()
df["year"] = df["ts"].dt.strftime("%Y")
df["month"] = df["ts"].dt.strftime("%m")
df["day"] = df["ts"].dt.strftime("%d")
df = df.rename(columns={"pays": "country"})

# ---------------------------------------------------------------- 1. écriture partitionnée (style Hive : cle=valeur)
partitionnement = ds.partitioning(
    pa.schema([("year", pa.string()), ("month", pa.string()), ("day", pa.string()), ("country", pa.string())]),
    flavor="hive")
ds.write_dataset(pa.Table.from_pandas(df, preserve_index=False), SORTIE, format="parquet",
                 partitioning=partitionnement, basename_template="part-{i}.parquet")

fichiers = [os.path.join(r, f) for r, _, fs in os.walk(SORTIE) for f in fs]
taille = sum(os.path.getsize(f) for f in fichiers)
print(f"Partitionnement year/month/day/country : {len(fichiers)} fichiers, "
      f"{taille / 1e6:.1f} Mo au total, {taille / len(fichiers) / 1e3:.0f} Ko en moyenne par fichier")
exemple = sorted(fichiers)[0]
print("Exemple de chemin :", os.path.relpath(exemple, os.path.dirname(SORTIE)))

# ---------------------------------------------------------------- 2. élagage de partitions
dataset = ds.dataset(SORTIE, format="parquet", partitioning=partitionnement)
requetes = {
    "un jour + un pays (day=15, country=FR)":
        (ds.field("day") == "15") & (ds.field("country") == "FR"),
    "un jour, tous les pays (day=15)": ds.field("day") == "15",
    "un pays, tout le mois (country=FR)": ds.field("country") == "FR",
    "un produit (pas une clé de partition)": ds.field("produit") == "P7",
}
print("\nÉlagage de partitions : fichiers réellement ouverts par requête")
for libelle, filtre in requetes.items():
    lus = len(list(dataset.get_fragments(filter=filtre)))
    print(f"  {libelle:<42} -> {lus:>3} fichier(s) sur {len(fichiers)}")

# ---------------------------------------------------------------- 3. finesse du partitionnement
lignes_par_jour = len(df) / 30
octets_par_ligne = taille / len(df)
print("\nFinesse du partitionnement (sur 1 an, 8 pays, volume de ce jeu de test) :")
print(f"  {'Partitionnement':<40}{'Nb de fichiers/an':>18}{'Taille moyenne':>16}")
for libelle, nb_par_jour in [("year/month", 1 / 30), ("year/month/day", 1), ("year/month/day/country", 8),
                             ("year/month/day/hour/country", 24 * 8), ("year/month/day/hour/minute (1/min)", 1440)]:
    nb_an = round(365 * nb_par_jour)
    moyenne = lignes_par_jour / nb_par_jour * octets_par_ligne
    unite = f"{moyenne / 1e6:.1f} Mo" if moyenne >= 1e6 else f"{moyenne / 1e3:.1f} Ko"
    print(f"  {libelle:<40}{nb_an:>18,}{unite:>16}".replace(",", " "))
