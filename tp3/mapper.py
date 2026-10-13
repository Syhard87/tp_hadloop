#!/usr/bin/env python3
"""Mapper Hadoop Streaming - TP 3 : chiffre d'affaires par pays et qualité des données.

Entrée (stdin)  : une ligne CSV par vente  date;pays;produit;quantite;prix_unitaire
                  (séparateur ';' ou ',' détecté automatiquement)
Sortie (stdout) : "pays<TAB>montant" pour chaque ligne valide, montant = quantite x prix
Erreurs (stderr): un compteur Hadoop par ligne rejetée, plus un message lisible

Règles de qualité, contrôlées dans cet ordre (une ligne rejetée incrémente un seul compteur) :
  1. 5 champs attendus                      -> Format_invalide
  2. le pays est obligatoire                -> Pays_manquant
  3. quantité : entier strictement positif  -> Quantite_invalide (non entier ou nul) / Valeur_negative (< 0)
  4. prix : nombre strictement positif      -> Prix_invalide (non numérique ou nul)   / Valeur_negative (< 0)
L'en-tête ne produit aucune sortie (compteur En_tete).
"""
import sys
from decimal import Decimal, InvalidOperation

GROUPE = "Qualite"


def compteur(nom, valeur=1):
    """Incrémente un compteur Hadoop : Streaming lit ce format sur stderr."""
    sys.stderr.write(f"reporter:counter:{GROUPE},{nom},{valeur}\n")


def rejeter(raison, ligne):
    compteur(raison)
    sys.stderr.write(f"REJET {raison} : {ligne}\n")


def controler_quantite(texte):
    """Renvoie (quantite, None) si valide, sinon (None, nom_du_compteur)."""
    try:
        quantite = int(texte)
    except ValueError:
        return None, "Quantite_invalide"      # 'abc', '2.5', '' ...
    if quantite < 0:
        return None, "Valeur_negative"
    if quantite == 0:
        return None, "Quantite_invalide"      # entier mais pas strictement positif
    return quantite, None


def controler_prix(texte):
    """Renvoie (prix, None) si valide, sinon (None, nom_du_compteur)."""
    try:
        prix = Decimal(texte)
    except InvalidOperation:
        return None, "Prix_invalide"
    if not prix.is_finite():
        return None, "Prix_invalide"          # 'NaN', 'Infinity'
    if prix < 0:
        return None, "Valeur_negative"
    if prix == 0:
        return None, "Prix_invalide"
    return prix, None


def main():
    for brute in sys.stdin:
        ligne = brute.strip().lstrip("﻿")   # retire \n, \r (fichiers Windows) et BOM
        if not ligne:
            compteur("Ligne_vide")
            continue

        separateur = ";" if ";" in ligne else ","
        champs = [c.strip() for c in ligne.split(separateur)]

        if champs[0].lower() == "date":           # en-tête : aucune sortie
            compteur("En_tete")
            continue

        if len(champs) != 5:
            rejeter("Format_invalide", ligne)
            continue

        _date, pays, _produit, quantite_txt, prix_txt = champs

        if not pays:
            rejeter("Pays_manquant", ligne)
            continue

        quantite, erreur = controler_quantite(quantite_txt)
        if erreur:
            rejeter(erreur, ligne)
            continue

        prix, erreur = controler_prix(prix_txt)
        if erreur:
            rejeter(erreur, ligne)
            continue

        compteur("Lignes_valides")
        montant = quantite * prix                  # Decimal : calcul exact, sans erreur d'arrondi
        print(f"{pays}\t{montant:f}")              # valeur exacte : l'arrondi est fait une seule fois, au Reducer


if __name__ == "__main__":
    main()
