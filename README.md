# TP Hadoop avancés : TP 1, Partie A (Google Colab)

Énoncé : [`sources/TP_Hadoop_avances_avec_TP_ecosysteme.pdf`](sources/TP_Hadoop_avances_avec_TP_ecosysteme.pdf), Partie A pages 5 à 9.

## Lancer le TP

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/syhard87/tp_hadloop/blob/claude/tp-hadoop-avances-partie-a-mwgfq7/notebooks/TP1_PartieA_Colab.ipynb)

1. Cliquer sur le badge (ou, dans Colab : *Fichier → Ouvrir un notebook → GitHub → `syhard87/tp_hadloop`*).
2. *Fichier → Enregistrer une copie dans Drive* pour garder vos sorties.
3. Exécuter la cellule « Espace partagé », puis les cellules 0 à 11 dans l'ordre.

## Espace partagé

| Quoi | Où | Comment partager |
|---|---|---|
| Énoncé, notebook, réponses | ce dépôt GitHub | *Settings → Collaborators → Add people* |
| Preuves d'exécution (sorties `fsck`, `jps`, md5…) | `MyDrive/TP_Hadoop_partage/preuves_tp` (créé par le notebook) | Drive : clic droit sur le dossier → *Partager* |
| Notebook exécuté | votre copie Colab dans Drive | Colab : bouton *Partager* en haut à droite |

## Contenu

```
sources/TP_Hadoop_avances_avec_TP_ecosysteme.pdf   énoncé du TP
notebooks/TP1_PartieA_Colab.ipynb                  cellules 0 à 11 + réponses du « Travail demandé »
resultats/reference_fsck_300Mo.txt                 sortie de référence (exécution de test hors Colab)
```

## Corrections apportées aux cellules du PDF

Testées avec Hadoop 3.5.0 et Java 17, en tant que `root` comme dans Colab :

- **Cellule 4** : avec les cellules du PDF, `start-dfs.sh` échoue avec
  `ERROR: Attempting to operate on hdfs namenode as root ... there is no HDFS_NAMENODE_USER defined`.
  → `HDFS_NAMENODE_USER`, `HDFS_DATANODE_USER` et `HDFS_SECONDARYNAMENODE_USER` valent désormais `root` dans `hadoop-env.sh`.
- **Cellule 3** : `sed 's|^export JAVA_HOME=...'` ne modifie rien, car la ligne d'origine est commentée (`# export JAVA_HOME=`).
  → `JAVA_HOME` est ajouté explicitement dans `hadoop-env.sh`.
- La cellule 4 peut être rejouée sans erreur (arrêt des démons avant le reformatage).
- Les sorties importantes sont enregistrées dans `/content/preuves_tp` (point 8 de la checklist).

## Résultats obtenus (exécution de test)

```
len=134217728 Live_repl=1
len=134217728 Live_repl=1
len=46137344  Live_repl=1
Under-replicated blocks: 3 (100.0 %)   Missing replicas: 6 (66.666664 %)   Status: HEALTHY
md5 local == md5 HDFS
```

Les réponses rédigées aux 5 questions du « Travail demandé » se trouvent à la fin du notebook.
