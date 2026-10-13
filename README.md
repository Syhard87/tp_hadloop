# TP Hadoop avancés (Google Colab)

Énoncé : [`sources/TP_Hadoop_avances_avec_TP_ecosysteme.pdf`](sources/TP_Hadoop_avances_avec_TP_ecosysteme.pdf). TP 1 Partie A : pages 5 à 9. TP 2 : pages 12 et 13.

## Lancer un TP

| TP | Notebook |
|---|---|
| TP 1 - Architecte HDFS, Partie A | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/syhard87/tp_hadloop/blob/claude/tp-hadoop-avances-partie-a-mwgfq7/notebooks/TP1_PartieA_Colab.ipynb) |
| TP 2 - Administrateur HDFS | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/syhard87/tp_hadloop/blob/claude/tp-hadoop-avances-partie-a-mwgfq7/notebooks/TP2_Administrateur_HDFS.ipynb) |

1. Cliquer sur le badge (ou, dans Colab : *Fichier → Ouvrir un notebook → GitHub → `syhard87/tp_hadloop`*).
2. Cliquer sur **Se connecter** en haut à droite et attendre la coche verte.
3. *Fichier → Enregistrer une copie dans Drive* pour garder vos sorties.
4. Exécuter les cellules dans l'ordre, de haut en bas.

## Espace partagé

| Quoi | Où | Comment partager |
|---|---|---|
| Énoncé, notebook, réponses | ce dépôt GitHub | *Settings → Collaborators → Add people* |
| Preuves d'exécution (sorties `fsck`, `jps`, md5…) | `MyDrive/TP_Hadoop_partage/preuves_tp` et `preuves_tp2` (créés par les notebooks) | Drive : clic droit sur le dossier → *Partager* |
| Notebook exécuté | votre copie Colab dans Drive | Colab : bouton *Partager* en haut à droite |

## Contenu

```
sources/TP_Hadoop_avances_avec_TP_ecosysteme.pdf   énoncé du TP
notebooks/TP1_PartieA_Colab.ipynb                  TP 1 : cellules 0 à 11 + réponses du « Travail demandé »
notebooks/TP2_Administrateur_HDFS.ipynb            TP 2 : missions A à D + réponses
scripts/setup_hadoop_colab.sh                      installation Hadoop 3.5.0 HDFS + YARN dans Colab (rejouable)
data/logs_web.csv                                  jeu de données du TP 2 (fourni par l'enseignant)
resultats/reference_fsck_300Mo.txt                 sortie de référence (exécution de test hors Colab)
```

## TP 1 : corrections apportées aux cellules du PDF

Testées avec Hadoop 3.5.0 et Java 17, en tant que `root` comme dans Colab :

- **Cellule 4** : avec les cellules du PDF, `start-dfs.sh` échoue avec
  `ERROR: Attempting to operate on hdfs namenode as root ... there is no HDFS_NAMENODE_USER defined`.
  → `HDFS_NAMENODE_USER`, `HDFS_DATANODE_USER` et `HDFS_SECONDARYNAMENODE_USER` valent désormais `root` dans `hadoop-env.sh`.
- **Cellule 3** : `sed 's|^export JAVA_HOME=...'` ne modifie rien, car la ligne d'origine est commentée (`# export JAVA_HOME=`).
  → `JAVA_HOME` est ajouté explicitement dans `hadoop-env.sh`.
- La cellule 4 peut être rejouée sans erreur (arrêt des démons avant le reformatage).
- Les sorties importantes sont enregistrées dans `/content/preuves_tp` (point 8 de la checklist).

## TP 1 : résultats obtenus (exécution de test)

```
len=134217728 Live_repl=1
len=134217728 Live_repl=1
len=46137344  Live_repl=1
Under-replicated blocks: 3 (100.0 %)   Missing replicas: 6 (66.666664 %)   Status: HEALTHY
md5 local == md5 HDFS
```

Les réponses rédigées aux 5 questions du « Travail demandé » se trouvent à la fin du notebook.

## TP 2 : jeu de données

Uniquement `data/logs_web.csv`, fourni par l'enseignant : 200 lignes, 192 IP distinctes, le 01/10/2026 de 08h01 à 11h29.

| Statut | Requêtes |
|---|---|
| 200 | 146 |
| 404 | 48 (24 `/ancienne-page`, 24 `/produits/xyz`) |
| 301 | 5 |
| 500 | 1 (`/connexion`) |

Le notebook le charge dans `raw/web_logs/ingestion_date=2026-10-11/`. Il en dérive deux fichiers directement dans HDFS :
- `rejects/web_logs/…/logs_404.csv` : les 48 lignes 404, qui servent de 2e fichier pour `fsck` ;
- `analytics/web_logs/statuts.csv` : le nombre de requêtes par statut, suivi du marqueur `_READY`.
