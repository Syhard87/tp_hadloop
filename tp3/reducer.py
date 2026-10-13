#!/usr/bin/env python3
"""Reducer Hadoop Streaming - TP 3 : somme du chiffre d'affaires par pays.

Entrée (stdin)  : lignes "pays<TAB>montant" TRIÉES par pays (garanti par le Shuffle/Sort de Hadoop,
                  ou par `sort` en test local). Toutes les lignes d'un même pays sont donc consécutives.
Sortie (stdout) : "pays<TAB>total" arrondi à 2 décimales, une ligne par pays.
La liste des pays n'est pas codée en dur : un nouveau groupe commence dès que la clé change.
"""
import sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

CENTIME = Decimal("0.01")


def emettre(pays, total):
    print(f"{pays}\t{total.quantize(CENTIME, rounding=ROUND_HALF_UP)}")


def main():
    pays_courant = None
    total = Decimal("0")

    for brute in sys.stdin:
        ligne = brute.rstrip("\r\n")
        if not ligne:
            continue
        try:
            pays, montant_txt = ligne.split("\t", 1)
            montant = Decimal(montant_txt)
        except (ValueError, InvalidOperation):
            sys.stderr.write("reporter:counter:Reducer,Ligne_invalide,1\n")
            continue

        if pays != pays_courant:
            if pays_courant is not None:
                emettre(pays_courant, total)       # la clé change : le groupe précédent est complet
            pays_courant = pays
            total = Decimal("0")
        total += montant

    if pays_courant is not None:                   # dernier groupe
        emettre(pays_courant, total)


if __name__ == "__main__":
    main()
