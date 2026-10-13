"""TP 4 - Question 29 : comparer CSV gzip, Avro, Parquet et ORC sur un cas analytique.

Génère 30 jours d'événements (un lot par minute, comme l'application du TP), les écrit dans les 4 formats,
puis mesure pour chacun :
  - la taille sur disque ;
  - le temps d'une requête analytique typique : « chiffre d'affaires par pays pour un jour donné »,
    qui n'a besoin que de 3 colonnes (date, pays, montant) sur 8.
Usage : python3 comparer_formats.py [dossier_de_sortie]
"""
import gzip, os, sys, time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.orc as orc
import pyarrow.parquet as pq
import fastavro

from donnees import generer_evenements

DOSSIER = sys.argv[1] if len(sys.argv) > 1 else "/content/tp4/formats"
os.makedirs(DOSSIER, exist_ok=True)
JOUR_CIBLE = "2026-10-15"

# ---------------------------------------------------------------- données : 30 jours x 1 440 minutes x 10 événements
df = generer_evenements()
n = len(df)
table = pa.Table.from_pandas(df, preserve_index=False)
print(f"Jeu de données : {n:_} événements, {df.shape[1]} colonnes, 30 jours".replace("_", " "))

# ---------------------------------------------------------------- écriture
chemins = {
    "CSV (brut)": f"{DOSSIER}/events.csv",
    "CSV gzip": f"{DOSSIER}/events.csv.gz",
    "Avro (deflate)": f"{DOSSIER}/events.avro",
    "Parquet (snappy)": f"{DOSSIER}/events.parquet",
    "ORC (zlib)": f"{DOSSIER}/events.orc",
}
df.to_csv(chemins["CSV (brut)"], index=False)
with open(chemins["CSV (brut)"], "rb") as src, gzip.open(chemins["CSV gzip"], "wb") as dst:
    dst.write(src.read())

schema_avro = fastavro.parse_schema({
    "type": "record", "name": "Event", "fields": [
        {"name": "event_id", "type": "long"},
        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "date", "type": "string"}, {"name": "pays", "type": "string"},
        {"name": "produit", "type": "string"}, {"name": "quantite", "type": "int"},
        {"name": "canal", "type": "string"}, {"name": "montant", "type": "double"}]})
with open(chemins["Avro (deflate)"], "wb") as f:
    fastavro.writer(f, schema_avro, df.to_dict("records"), codec="deflate")

pq.write_table(table, chemins["Parquet (snappy)"], compression="snappy", row_group_size=100_000)
orc.write_table(table, chemins["ORC (zlib)"], compression="zlib")

# ---------------------------------------------------------------- requête : CA par pays pour JOUR_CIBLE
COLS = ["date", "pays", "montant"]

def agreger(d):
    d = d[d["date"] == JOUR_CIBLE]
    return d.groupby("pays")["montant"].sum().round(2)

def lire_csv(chemin):
    options = pacsv.ConvertOptions(column_types={"date": pa.string()})
    return agreger(pacsv.read_csv(chemin, convert_options=options).to_pandas())   # lit et décompresse tout

def lire_avro(chemin):
    with open(chemin, "rb") as f:                                # orienté ligne : lit toutes les colonnes
        return agreger(pd.DataFrame.from_records(
            ({c: r[c] for c in COLS} for r in fastavro.reader(f))))

def lire_parquet(chemin):                                        # colonnes + filtre poussé (statistiques)
    return agreger(pq.read_table(chemin, columns=COLS, filters=[("date", "=", JOUR_CIBLE)]).to_pandas())

def lire_orc(chemin):
    return agreger(ds.dataset(chemin, format="orc").to_table(
        columns=COLS, filter=ds.field("date") == JOUR_CIBLE).to_pandas())

lecteurs = {"CSV (brut)": lire_csv, "CSV gzip": lire_csv, "Avro (deflate)": lire_avro,
            "Parquet (snappy)": lire_parquet, "ORC (zlib)": lire_orc}

lignes, reference = [], None
for nom, chemin in chemins.items():
    t0 = time.perf_counter()
    resultat = lecteurs[nom](chemin)
    duree = time.perf_counter() - t0
    if reference is None:
        reference = resultat
    lignes.append({"Format": nom,
                   "Taille (Mo)": round(os.path.getsize(chemin) / 1e6, 1),
                   "Requête (s)": round(duree, 2),
                   "Résultat identique": bool(np.allclose(resultat.sort_index(), reference.sort_index()))})

bilan = pd.DataFrame(lignes)
taille_csv = bilan.loc[0, "Taille (Mo)"]
bilan["Taille vs CSV"] = (bilan["Taille (Mo)"] / taille_csv * 100).round(0).astype(int).astype(str) + " %"
print(f"\nRequête : chiffre d'affaires par pays le {JOUR_CIBLE} (3 colonnes sur 8, 1 jour sur 30)\n")
print(bilan.to_string(index=False))
print("\nRésultat de la requête :")
print(reference.to_string())
bilan.to_csv(f"{DOSSIER}/bilan_formats.csv", index=False)
