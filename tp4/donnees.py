"""TP 4 - Jeu d'événements synthétique commun aux démonstrations des Missions C (partitionnement et formats).

30 jours d'événements, 10 par minute, comme l'application du TP qui dépose un lot par minute.
Graine fixe : les résultats sont identiques à chaque exécution.
"""
import numpy as np
import pandas as pd

PAYS = np.array(["FR", "BE", "ES", "DE", "IT", "UK", "NL", "PT"])
POIDS_PAYS = [.35, .15, .12, .12, .10, .08, .05, .03]


def generer_evenements(jours=30, par_minute=10, graine=42):
    rng = np.random.default_rng(graine)
    minutes = pd.date_range("2026-10-01", periods=jours * 1440, freq="min")
    n = len(minutes) * par_minute
    ts = pd.to_datetime(np.repeat(minutes.values, par_minute))
    quantite = rng.integers(1, 20, n).astype(np.int32)
    df = pd.DataFrame({
        "event_id": np.arange(n, dtype=np.int64),
        "ts": ts,
        "date": ts.strftime("%Y-%m-%d"),
        "pays": rng.choice(PAYS, n, p=POIDS_PAYS),
        "produit": np.char.add("P", rng.integers(1, 60, n).astype(str)),
        "quantite": quantite,
        "canal": rng.choice(["web", "magasin", "mobile"], n),
        "montant": np.round(quantite * np.round(rng.uniform(5, 140, n), 2), 2),
    })
    return df
