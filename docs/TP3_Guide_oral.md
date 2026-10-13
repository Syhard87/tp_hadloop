# TP 3 - MapReduce avancé : chiffre d'affaires et qualité des données
## Guide complet pour l'oral

> Tous les chiffres de ce document ont été **obtenus en exécutant réellement** le TP : Hadoop 3.5.0, Java 17,
> HDFS et YARN en mode pseudo-distribué, comme dans Colab. Ils seront identiques chez vous, sauf les identifiants
> (`application_…`, `blk_…`) et les durées.
>
> Fichiers : `tp3/mapper.py`, `tp3/reducer.py`, `notebooks/TP3_MapReduce_Qualite.ipynb`, `data/ventes_tp3.csv`.

---

## Sommaire
1. [Proposition de plan pour l'oral (répartition du groupe)](#1-proposition-de-plan-pour-loral)
2. [Les notions à maîtriser (définitions)](#2-les-notions-à-maîtriser)
3. [Le déroulé complet, étape par étape, et ce qui se passe à l'intérieur](#3-le-déroulé-complet-étape-par-étape)
4. [Réponses détaillées aux Missions A à E](#4-réponses-détaillées-aux-missions)
5. [Bonus : combiner, plusieurs reducers, données de l'enseignant](#5-bonus)
6. [Limites et améliorations](#6-limites-et-améliorations)
7. [Questions probables du jury et réponses](#7-questions-probables-du-jury)
8. [Antisèche des commandes](#8-antisèche-des-commandes)

---

## 1. Proposition de plan pour l'oral

| # | Partie | Contenu | Support |
|---|---|---|---|
| 1 | **Contexte et architecture** | Le besoin (CA par pays et données sales), HDFS, YARN, le principe de MapReduce, le schéma du pipeline | §2 et §3.1 |
| 2 | **Le code** | Mapper (règles de qualité, compteurs, `Decimal`), Reducer (regroupement par changement de clé, arrondi) | §4 Missions A et B |
| 3 | **Tests et exécution** | Test local avec les tubes Unix (`sort` = Shuffle/Sort), insertion dans HDFS, lancement du job sur YARN | §4 Missions C et D |
| 4 | **Analyse et ouverture** | Compteurs, bilan lues/valides/rejetées/groupes, erreur du dossier output, combiner, limites | §4 Mission E, §5 et §6 |

**Phrase d'accroche possible** : « Notre job ne se contente pas de calculer un chiffre d'affaires : il **mesure la qualité**
des données qu'il traite. Sur 9 ventes, 3 étaient invalides, et Hadoop nous le dit grâce à des compteurs métier, sans
qu'on ait à relire le fichier. »

**Phrase de conclusion possible** : « Le même code, sans aucune modification, traite le fichier de l'enseignant
(41 ventes, 6 pays). Il passerait de la même façon à des milliards de lignes, car Hadoop découpe les données, distribue
le calcul là où sont les données et regroupe les résultats par clé. »

---

## 2. Les notions à maîtriser

### 2.1 Hadoop
Framework open source (Apache) pour **stocker** et **traiter** de très gros volumes de données sur un **cluster de
machines ordinaires**. Il a trois briques principales :

| Brique | Rôle | Question à laquelle elle répond |
|---|---|---|
| **HDFS** | Stockage distribué | *Où sont les données ?* |
| **YARN** | Gestion des ressources et ordonnancement | *Qui calcule, avec quelles ressources ?* |
| **MapReduce** | Modèle de programmation du traitement | *Comment calculer en parallèle ?* |

Le principe fondateur est de **déplacer le calcul vers les données** plutôt que les données vers le calcul : on envoie un
script de quelques Ko là où sont stockés des téraoctets.

### 2.2 HDFS (Hadoop Distributed File System)
| Élément | Définition |
|---|---|
| **NameNode** | Le « chef » : il garde **les métadonnées** en mémoire (arborescence, droits, liste des blocs de chaque fichier, emplacement de chaque bloc). Il ne stocke **aucune donnée**. |
| **DataNode** | Les « ouvriers » : ils stockent physiquement les **blocs** sur leur disque, envoient un **heartbeat** toutes les 3 s au NameNode et un **rapport de blocs** périodique. |
| **SecondaryNameNode** | Fait des **checkpoints** : il fusionne `fsimage` (photo des métadonnées) et `edits` (journal des modifications) pour éviter que le journal grossisse indéfiniment. **Ce n'est PAS un NameNode de secours** (piège classique). |
| **Bloc** | Unité de découpage des fichiers, **128 Mo** par défaut (`dfs.blocksize`). Un fichier de 275 octets occupe 1 bloc qui ne prend que 275 octets sur disque. |
| **Réplication** | Chaque bloc est copié sur **plusieurs DataNodes**, 3 par défaut (`dfs.replication`). Elle vaut 1 dans Colab, qui n'a qu'un DataNode. |
| **Heartbeat** | Signal périodique DataNode → NameNode. Sans heartbeat pendant environ 10 min 30 s, le DataNode est déclaré mort et ses blocs sont re-répliqués. |
| **Write-once, read-many** | On écrit un fichier une fois et on le lit souvent. **Pas de modification au milieu d'un fichier** : seulement l'ajout en fin (`append`), la troncature (`truncate`) ou le remplacement complet. |
| **Safe mode** | Mode lecture seule au démarrage, tant que le NameNode n'a pas reçu assez de rapports de blocs. |

### 2.3 YARN (Yet Another Resource Negotiator)
| Élément | Définition |
|---|---|
| **ResourceManager (RM)** | Le chef du cluster pour les **ressources** (CPU, mémoire). Il reçoit les applications et attribue des conteneurs. |
| **NodeManager (NM)** | Un par machine. Il lance et surveille les **conteneurs**, et rend compte au RM. Il héberge aussi le service `mapreduce_shuffle`, qui sert les sorties Map aux Reducers (d'où `yarn.nodemanager.aux-services=mapreduce_shuffle` dans la configuration). |
| **ApplicationMaster (AM)** | Un par job. Il **pilote le job** : il demande des conteneurs au RM, lance les tâches Map puis Reduce, relance les tâches en échec et **agrège les compteurs**. |
| **Conteneur** | Une portion de ressources (mémoire et vcores) sur un nœud, dans laquelle s'exécute une tâche. |

### 2.4 MapReduce
Modèle en **deux fonctions** écrites par le développeur. **Tout le reste est fait par le framework.**

| Phase | Qui | Rôle | Dans notre TP |
|---|---|---|---|
| **InputFormat / InputSplit** | Framework | Découpe l'entrée en *splits*, en général un par bloc. **Un split = une tâche Map.** | `TextInputFormat` : 1 enregistrement = 1 ligne. 2 splits ici. |
| **Map** | **Développeur** | Transforme chaque enregistrement en 0, 1 ou plusieurs paires **(clé, valeur)** | `mapper.py` : ligne → `(pays, montant)` si valide, rien sinon |
| **Combiner** (optionnel) | Développeur | Pré-agrège **côté Map** pour réduire le réseau | Bonus : `reducer.py` réutilisé |
| **Partitioner** | Framework | Décide **quel Reducer** reçoit chaque clé : `hash(clé) mod nbReducers` | 1 reducer, donc tout va au reducer 0 |
| **Shuffle** | Framework | **Transfert réseau** des sorties Map vers les Reducers | `Reduce shuffle bytes=78` |
| **Sort** | Framework | **Tri par clé** et fusion. Toutes les valeurs d'une clé arrivent **consécutivement**. | Simulé par `sort` en local |
| **Reduce** | **Développeur** | Agrège les valeurs de chaque clé | `reducer.py` : somme par pays |
| **OutputFormat** | Framework | Écrit le résultat dans HDFS : un fichier `part-0000N` par Reducer et `_SUCCESS` | `part-00000` |

### 2.5 Hadoop Streaming
Un utilitaire (`hadoop-streaming-3.5.0.jar`) qui permet d'écrire le Mapper et le Reducer **dans n'importe quel langage**
(Python, Bash…), au lieu de Java. Le contrat est simple :
- **stdin** : les données d'entrée, ligne par ligne ;
- **stdout** : les paires `clé<TAB>valeur`. Tout ce qui précède la 1re tabulation est la clé ;
- **stderr** : les journaux, et les lignes spéciales `reporter:counter:Groupe,Compteur,N` (compteurs) ou `reporter:status:…`.

### 2.6 Compteurs (counters)
Des **statistiques agrégées** sur tout le job. Chaque tâche incrémente ses compteurs, et l'ApplicationMaster les additionne
pour l'ensemble des tâches **réussies** (les tentatives échouées ne comptent pas).
- **Compteurs du framework** : `Map input records`, `Reduce output records`, octets lus et écrits, etc.
- **Compteurs métier** (définis par nous) : groupe `Qualite`, avec `Pays_manquant`, `Quantite_invalide`, `Valeur_negative`…

Intérêt : **mesurer la qualité sans produire un 2e fichier ni relire les données**, et pouvoir déclencher une alerte
(par exemple « plus de 5 % de rejets, on bloque la publication »).

### 2.7 Qualité des données
Les dimensions classiques, et celles que notre Mapper contrôle :

| Dimension | Définition | Contrôle dans le TP |
|---|---|---|
| **Complétude** | Les champs obligatoires sont renseignés | Pays non vide → `Pays_manquant` |
| **Validité** | Le format et le type sont respectés | Quantité entière, prix numérique, 5 champs → `Quantite_invalide`, `Prix_invalide`, `Format_invalide` |
| **Cohérence métier** | Les valeurs sont plausibles | Quantité et prix > 0 → `Valeur_negative` |
| **Unicité** | Pas de doublons | **Non contrôlée** par les règles du TP 3 (voir §6) |
| **Exactitude** | La valeur est conforme à la réalité | Hors périmètre : il faudrait un référentiel, comme `produits.csv` |

---

## 3. Le déroulé complet, étape par étape

### 3.1 Vue d'ensemble
```
 [local]                [HDFS]                           [YARN : traitement distribué]                      [HDFS]
ventes.csv ──put──▶ /tp_avance/ca/input ──split──▶ MAP×2 ──partition/sort──▶ SHUFFLE ──merge──▶ REDUCE×1 ──▶ /tp_avance/ca/output
                        (1 bloc)                 mapper.py   (disque local)    (réseau)          reducer.py      part-00000 + _SUCCESS
                                                     │                                               │
                                                     └──────── compteurs ───▶ ApplicationMaster ◀───┘──▶ affichés en fin de job
```

### 3.2 Insertion des données : `hdfs dfs -mkdir -p` puis `hdfs dfs -put`
```bash
hdfs dfs -mkdir -p /tp_avance/ca/input
hdfs dfs -put -f ventes.csv /tp_avance/ca/input/
```
Ce qui se passe :
1. `mkdir` : le **NameNode** crée les répertoires dans ses métadonnées. Aucun DataNode n'est concerné.
2. `put` : le client demande au NameNode de **créer** le fichier. Le NameNode vérifie les droits, que le chemin n'existe
   pas (`-f` force l'écrasement) et que le cluster n'est pas en safe mode.
3. Le NameNode **alloue un bloc** et renvoie la liste des DataNodes cibles (1 ici, 3 sur un vrai cluster).
4. Le client **envoie les données directement aux DataNodes**, par paquets de 64 Ko. Avec une réplication de 3, c'est un
   **pipeline** DN1 → DN2 → DN3, et les accusés de réception remontent en sens inverse.
5. À la fermeture du fichier, le NameNode **valide** le fichier, qui devient visible (`ls`).

Résultat obtenu : `ventes.csv`, 275 octets, 1 bloc, réplication 1.

### 3.3 Lecture : `ls`, `cat`, `tail`, `stat`, `fsck`, `get`
```bash
hdfs dfs -ls -R /tp_avance                       # métadonnées seulement (NameNode)
hdfs dfs -cat /tp_avance/ca/input/ventes.csv     # contenu
hdfs dfs -stat "%n %b %r %o" <fichier>           # nom, taille, réplication, taille de bloc
hdfs fsck <fichier> -files -blocks -locations    # blocs et emplacements
hdfs dfs -get -f <fichier_hdfs> <local>          # copie HDFS -> local
```
Ce qui se passe : le client demande au NameNode **la liste des blocs et des DataNodes** qui les détiennent, puis lit
**directement sur le DataNode le plus proche**. Les données ne transitent **jamais** par le NameNode, qui serait sinon
un goulot d'étranglement. `ls` et `stat` n'interrogent que le NameNode.

### 3.4 Soumission du job MapReduce
```bash
STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name 'hadoop-streaming-*.jar' | head -1)
hdfs dfs -rm -r -f /tp_avance/ca/output
hadoop jar "$STREAMING_JAR" -D mapreduce.job.name=TP_Avance_CA_Qualite -D mapreduce.job.reduces=1 \
  -files mapper.py,reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /tp_avance/ca/input -output /tp_avance/ca/output 2>&1 | tee execution.log
```
Étapes réelles, visibles dans `execution.log` :

| # | Étape | Trace dans le journal |
|---|---|---|
| 1 | Le client **vérifie que `-output` n'existe pas** (sinon il s'arrête **immédiatement**) | (Mission E22) |
| 2 | Le client se connecte au **ResourceManager** | `Connecting to ResourceManager at /0.0.0.0:8032` |
| 3 | Il copie dans un dossier de **staging HDFS** le jar, la configuration et les fichiers `-files` (mapper.py, reducer.py) | `JobResourceUploader … /tmp/hadoop-yarn/staging/root/.staging/job_…` |
| 4 | Il calcule les **splits** de l'entrée | `Total input files to process : 1`, puis `number of splits:2` |
| 5 | Il **soumet** l'application à YARN | `Submitted application application_…` |
| 6 | Le RM alloue un conteneur et un NodeManager y lance l'**ApplicationMaster** | `Running job: job_…` |
| 7 | L'AM demande **2 conteneurs Map**, de préférence sur les nœuds qui ont les données (**localité**) | `Data-local map tasks=2` |
| 8 | Chaque tâche Map télécharge les `-files` du **cache distribué** et lance `python3 mapper.py`. Le RecordReader lui envoie ses lignes sur stdin. | `map 100%` |
| 9 | Les paires de stdout sont **partitionnées, triées en mémoire**, puis écrites (*spill*) sur le **disque local** du nœud, pas dans HDFS | `Spilled Records=12` |
| 10 | La tâche Reduce **récupère** (shuffle) les sorties des 2 maps, les **fusionne** (merge sort) et lance `python3 reducer.py` | `Shuffled Maps =2`, `Merged Map outputs=2` |
| 11 | La sortie du Reducer est écrite dans un dossier temporaire, puis **validée** (*commit*) dans `/tp_avance/ca/output/part-00000`. `_SUCCESS` est créé. | `completed successfully` |
| 12 | L'AM **agrège les compteurs** de toutes les tâches et le client les affiche | `Counters: 59` |

**Pourquoi 2 tâches Map pour un fichier de 275 octets ?** Hadoop Streaming utilise l'ancienne API `mapred`, dont le
`FileInputFormat` vise par défaut **2 splits** (`mapreduce.job.maps=2`). Le fichier est donc coupé en deux morceaux
d'environ 137 octets. **Aucune ligne n'est coupée** : le lecteur du 1er split lit jusqu'à la fin de la ligne qui
chevauche la frontière, et celui du 2e split ignore cette ligne partielle. Sur un vrai cluster, on aurait environ
**1 split par bloc de 128 Mo**.

### 3.5 Lecture du résultat
```bash
hdfs dfs -ls /tp_avance/ca/output        # _SUCCESS (0 octet) + part-00000 (27 octets)
hdfs dfs -cat /tp_avance/ca/output/part-00000
```
```
BE	55.00
ES	30.00
FR	66.00
```

### 3.6 Modification : ce qui est possible dans HDFS
HDFS est **write-once** : on ne peut pas modifier une ligne au milieu d'un fichier. Les seules possibilités :

| Opération | Commande | Démonstration dans le notebook |
|---|---|---|
| Ajouter en fin de fichier | `hdfs dfs -appendToFile nouvelle_vente.csv /tp_avance/ca/input/ventes.csv` | Ajout de `2026-10-06;IT;P07;5;8.00` : 275 → 300 octets |
| Remplacer le fichier entier | `hdfs dfs -put -f ventes.csv /tp_avance/ca/input/ventes.csv` | Retour à 275 octets |
| Tronquer | `hdfs dfs -truncate <longueur> <fichier>` | (cité seulement) |
| Renommer ou déplacer | `hdfs dfs -mv <src> <dst>` | Opération **de métadonnées uniquement**, instantanée, aucun bloc déplacé |
| Copier | `hdfs dfs -cp <src> <dst>` | Les blocs sont réellement dupliqués |

**Un résultat MapReduce ne se met pas à jour, il se recalcule.** Après l'ajout de la vente IT, la relance du job donne
`Map input records=11`, `Map output records=7`, `Reduce output records=4`, et **IT 40.00** apparaît (5 × 8,00).
Le Reducer n'a pas été modifié : la preuve que la liste des pays n'est pas codée en dur.

### 3.7 Suppression
```bash
hdfs dfs -rm <fichier>                 # un fichier
hdfs dfs -rm -r -f /tp_avance/ca/output   # un dossier, récursif ; -f = pas d'erreur s'il n'existe pas
```
Ce qui se passe : le NameNode **retire l'entrée de ses métadonnées** (l'opération est immédiate). Les blocs physiques sont
effacés **de façon asynchrone** par les DataNodes, sur ordre du NameNode transmis dans la réponse au heartbeat.
**Corbeille** : `fs.trash.interval=0` dans notre configuration, donc la suppression est **définitive**. Avec une valeur
positive, le fichier irait d'abord dans `/user/root/.Trash` (on peut le contourner avec `-skipTrash`).

---

## 4. Réponses détaillées aux Missions

### Mission A - Mapper (`tp3/mapper.py`)

**15. Lire chaque ligne depuis l'entrée standard.**
`for brute in sys.stdin:`. Hadoop Streaming lance `python3 mapper.py` dans chaque conteneur Map et **écrit sur son stdin**
les lignes du split qui lui est attribué. Le script ne sait pas qu'il tourne sur Hadoop : c'est ce qui permet de le
tester localement avec `cat ventes.csv | ./mapper.py`. On nettoie chaque ligne avec `strip()` (retire `\n`, et `\r` pour
les fichiers Windows) et on retire un éventuel BOM UTF-8.

**16. Contrôler le format et les règles de qualité.** Dans cet ordre :
1. Ligne vide → ignorée (compteur `Ligne_vide`).
2. **En-tête** (1er champ = `date`) → **aucune sortie** (compteur `En_tete`), comme l'exige l'énoncé.
3. **5 champs** attendus, sinon `Format_invalide`. Le séparateur `;` ou `,` est détecté.
4. **Pays obligatoire** : vide → `Pays_manquant`.
5. **Quantité entier strictement positif** : `int()` échoue (`abc`, `2.5`) ou vaut 0 → `Quantite_invalide` ; négative → `Valeur_negative`.
6. **Prix numérique strictement positif** : non numérique, `NaN` ou 0 → `Prix_invalide` ; négatif → `Valeur_negative`.

Une ligne rejetée incrémente **un seul compteur**, celui de la **première règle violée**. On a donc toujours l'égalité :
**lignes lues = en-tête + valides + rejetées**, ce qui facilite les contrôles.

**17. Émettre pays et montant pour les lignes valides.**
`print(f"{pays}\t{montant:f}")`, avec `montant = quantite × prix`.
- La **tabulation** sépare la **clé** (pays), qui sert au tri et au regroupement, de la **valeur** (montant).
- Le calcul est fait en **`Decimal`** et non en `float` : `0.1 + 0.2` vaut `0.30000000000000004` en float. Pour de
  l'argent, on veut un calcul **exact**.
- Le montant est émis **sans arrondi**. On arrondit **une seule fois, à la fin**, dans le Reducer : arrondir chaque ligne
  puis additionner accumulerait les erreurs. Par exemple, 3 × 1,005 = 3,015 serait arrondi trop tôt.

**18. Émettre un compteur différent par type d'anomalie.**
```
reporter:counter:Qualite,Pays_manquant,1
reporter:counter:Qualite,Quantite_invalide,1
reporter:counter:Qualite,Valeur_negative,1
```
- Ces lignes sont écrites sur **stderr**, jamais sur stdout. **stdout est réservé aux données** : un compteur écrit sur
  stdout deviendrait une fausse paire clé/valeur envoyée au Reducer.
- Hadoop Streaming **intercepte** les lignes `reporter:counter:` sur stderr et incrémente le compteur `Qualite / <Nom>`.
- On écrit aussi un message lisible, `REJET <raison> : <ligne>`, qui finit dans les journaux de la tâche et se consulte avec
  `yarn logs -applicationId <id>`.

**Sortie du Mapper sur les données de l'énoncé** (`sortie_map.txt`, dans l'ordre du fichier) :
```
FR	20.00
BE	15.00
FR	36.00
BE	40.00
ES	30.00
FR	10.00
```

### Mission B - Reducer (`tp3/reducer.py`)

**Additionner les montants par pays.** L'entrée du Reducer est **triée par clé**, grâce au Shuffle/Sort. Toutes les lignes
d'un même pays sont donc **consécutives**. Algorithme :
```
pays_courant = None, total = 0
pour chaque ligne "pays<TAB>montant" :
    si pays != pays_courant :      # changement de clé : le groupe précédent est complet
        émettre(pays_courant, total)  (sauf la 1re fois)
        pays_courant = pays ; total = 0
    total += montant
émettre(pays_courant, total)       # ne pas oublier le dernier groupe
```
- **Mémoire constante** : on ne garde qu'un pays et un total à la fois. Ça marche avec 3 lignes comme avec des milliards.
- C'est l'équivalent Streaming de `reduce(clé, liste_de_valeurs)` en Java.

**Arrondi à deux décimales.** `total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)`. C'est l'arrondi **commercial**
(0,005 → 0,01). La fonction `round()` de Python fait un arrondi « bancaire » (au pair le plus proche) et, avec des float,
`round(2.675, 2)` donne `2.67`.

**Ne pas coder en dur la liste des pays.** Les groupes sont détectés **dynamiquement**, par changement de clé. Aucune liste,
aucun `if pays == "FR"`. Preuves : l'ajout d'une vente IT fait apparaître IT, et le fichier de l'enseignant (Allemagne,
Belgique, Espagne, France, Italie, Royaume-Uni) fonctionne **sans modifier une ligne de code**.

Robustesse : une ligne mal formée en entrée du Reducer est ignorée et comptée (`Reducer / Ligne_invalide`) au lieu de
faire planter la tâche.

### Mission C - Test local

```bash
cat ventes.csv | ./mapper.py 1> sortie_map.txt 2> erreurs_map.txt
cat ventes.csv | ./mapper.py | sort | ./reducer.py
```
- `1>` redirige **stdout** (les données) et `2>` redirige **stderr** (les compteurs et les rejets) vers deux fichiers distincts.
  C'est la séparation que fait Hadoop.
- `./mapper.py` fonctionne grâce au **shebang** `#!/usr/bin/env python3` et au droit d'exécution `chmod +x`.

**Comparer la sortie du Mapper et le résultat final.**

| | Sortie du Mapper | Résultat final |
|---|---|---|
| Nombre de lignes | 6, une par **vente valide** | 3, une par **pays** |
| Ordre | Ordre du fichier | **Trié** par pays |
| Doublons de clé | Oui (FR 3 fois, BE 2 fois) | Non, une ligne par clé |
| Valeurs | Montant d'**une** vente | **Somme** des ventes, arrondie |
| Rôle | **Filtrer et transformer** | **Agréger** |

**Expliquer pourquoi `sort` simule le Shuffle/Sort.** Entre Map et Reduce, Hadoop **partitionne** les paires par clé, les
**trie par clé**, les **transfère** à chaque Reducer et les **fusionne**. Le Reducer reçoit donc les valeurs d'une clé
**à la suite**. La clé étant **en début de ligne**, `sort` (tri lexicographique) rend adjacentes les lignes d'un même pays :
c'est exactement l'hypothèse dont dépend le Reducer.
- **Preuve par l'absurde** : sans `sort`, `cat ventes.csv | ./mapper.py | ./reducer.py` donne **6 lignes** (FR 20, BE 15,
  FR 36, BE 40, ES 30, FR 10). Les totaux sont faux, car le Reducer croit que chaque changement de clé termine un groupe.
- **Différences avec le vrai Shuffle** : `sort` est un seul processus sur une seule machine. Hadoop trie **en parallèle**
  (chaque Map trie sa sortie, chaque Reducer fusionne des flux déjà triés), **par partition**, et transporte les données
  sur le **réseau**. Hadoop compare les clés **octet par octet**. Pour le reproduire exactement, il faut utiliser
  `LC_ALL=C sort`, sinon la langue du système peut changer l'ordre des accents et des majuscules.

**Vérifier manuellement les totaux.**

| Ligne | Pays | Calcul | Montant | Statut |
|---|---|---|---|---|
| 2 | FR | 2 × 10,00 | 20,00 | ✅ |
| 3 | BE | 1 × 15,00 | 15,00 | ✅ |
| 4 | FR | 3 × 12,00 | 36,00 | ✅ |
| 5 | BE | 4 × 10,00 | 40,00 | ✅ |
| 6 | ES | 2 × 15,00 | 30,00 | ✅ |
| 7 | FR | 1 × 10,00 | 10,00 | ✅ |
| 8 | DE | `abc` × 20,00 | - | ❌ Quantite_invalide |
| 9 | *(vide)* | 2 × 9,50 | - | ❌ Pays_manquant |
| 10 | FR | -1 × 30,00 | - | ❌ Valeur_negative |

- **FR** = 20 + 36 + 10 = **66,00**
- **BE** = 15 + 40 = **55,00**
- **ES** = **30,00**
- **DE est absent** : sa seule ligne est invalide.

Le notebook refait aussi le calcul indépendamment avec pandas et confirme l'égalité.

### Mission D - Exécution Hadoop

| Élément | Explication |
|---|---|
| `STREAMING_JAR=$(find … -name 'hadoop-streaming-*.jar' \| head -1)` | Cherche le jar Streaming sans écrire sa version en dur ; ici `hadoop-streaming-3.5.0.jar` |
| `hdfs dfs -rm -r -f /tp_avance/ca/output` | Hadoop **refuse** d'écrire dans un dossier existant (voir E22). `-f` évite une erreur au 1er lancement. |
| `-D mapreduce.job.name=TP_Avance_CA_Qualite` | Nom visible dans `yarn application -list` et dans l'interface web (port 8088) |
| `-D mapreduce.job.reduces=1` | 1 Reducer, donc **1 seul fichier** `part-00000` et un tri global |
| `-files mapper.py,reducer.py` | **Cache distribué** : les scripts sont copiés dans HDFS (staging), puis dans le dossier de travail de chaque conteneur |
| `-mapper "python3 mapper.py"` / `-reducer "python3 reducer.py"` | Commandes exécutées dans les conteneurs. Avec `python3` explicite, le droit `+x` n'est pas nécessaire côté cluster. |
| `-input` / `-output` | Chemins **HDFS** |
| `2>&1 \| tee execution.log` | Fusionne stderr dans stdout (Hadoop écrit son journal sur stderr), puis affiche **et** enregistre |

**Résultat** : `Job … completed successfully`, `/tp_avance/ca/output` contient `_SUCCESS` (0 octet) et `part-00000`
(27 octets), avec **BE 55.00, ES 30.00, FR 66.00**, identique au test local (`diff` vide).

### Mission E - Analyse

**19. Compteurs du framework** (extraits de `execution.log`) :

| Compteur | Valeur | Signification |
|---|---|---|
| `Map input records` | **10** | Lignes lues par les Mappers, en-tête compris |
| `Map output records` | **6** | Paires `(pays, montant)` émises |
| `Reduce input groups` | **3** | Clés distinctes reçues par le Reducer |
| `Reduce input records` | 6 | Paires reçues par le Reducer (= Map output, sans combiner) |
| `Reduce output records` | **3** | Lignes du résultat final |
| `Launched map tasks` / `reduce tasks` | 2 / 1 | 2 splits, 1 reducer demandé |
| `Data-local map tasks` | 2 | Les Maps ont tourné là où étaient les données |
| `Spilled Records` | 12 | 6 écrits sur disque côté Map + 6 lors de la fusion côté Reduce |
| `Reduce shuffle bytes` | 78 | Octets transférés de Map vers Reduce |

**20. Compteurs métier** (groupe `Qualite`) :

| Compteur | Valeur | Ligne concernée |
|---|---|---|
| `En_tete` | 1 | `date;pays;produit;quantite;prix_unitaire` |
| `Lignes_valides` | 6 | |
| `Pays_manquant` | 1 | `2026-10-04;;P05;2;9.50` |
| `Quantite_invalide` | 1 | `2026-10-04;DE;P04;abc;20.00` |
| `Valeur_negative` | 1 | `2026-10-05;FR;P06;-1;30.00` |

On retrouve les lignes rejetées dans les journaux des conteneurs avec `yarn logs -applicationId <id> | grep REJET`.

**21. Lignes lues, valides, rejetées et groupes Reduce.**
- **Lignes lues** (`Map input records` = 10) : tout ce que le framework a fourni aux Mappers, **y compris l'en-tête**.
  C'est un compteur **technique**, indépendant de notre logique.
- **Lignes valides** (`Lignes_valides` = `Map output records` = 6) : les lignes qui passent toutes les règles. Elles sont
  égales ici car chaque ligne valide émet **exactement une** paire. Un Mapper pourrait émettre 0 ou plusieurs paires par ligne.
- **Lignes rejetées** (1 + 1 + 1 = 3) : les lignes lues mais **sans sortie**, à cause d'une anomalie. L'en-tête n'est
  **pas** une anomalie : il est ignoré par conception, d'où un compteur séparé.
- **Groupes Reduce** (`Reduce input groups` = 3) : le nombre de **clés distinctes** après le Shuffle/Sort. Il dépend du
  **nombre de pays**, pas du nombre de lignes : 1 million de ventes sur 3 pays donnerait toujours 3 groupes.

**Bilan** : 10 lues = 1 en-tête + 6 valides + 3 rejetées → 6 paires → 3 groupes → 3 lignes de résultat.
Taux de rejet : 3 / 9 ventes = **33 %**, ce qui justifierait une alerte en production.

**22. Provoquer l'erreur de dossier output existant, puis la corriger.**
On relance le job **sans** `hdfs dfs -rm -r -f /tp_avance/ca/output` :
```
ERROR streaming.StreamJob: Error Launching job : Output directory hdfs://localhost:9000/tp_avance/ca/output already exists
```
Le code retour vaut **5**. La vérification a lieu **côté client, avant la soumission**
(`FileOutputFormat.checkOutputSpecs`, qui lève une `FileAlreadyExistsException`) : **aucune application YARN n'est créée**,
ce qui se vérifie dans `yarn application -list`. C'est une **sécurité** : un résultat peut avoir coûté des heures de
calcul, et Hadoop refuse de l'écraser silencieusement. C'est aussi lié à la sémantique *write-once* de HDFS.

**Corrections** :
1. Supprimer l'ancien résultat, puis relancer : `hdfs dfs -rm -r -f /tp_avance/ca/output` (code retour 0, même résultat).
2. Ou écrire dans un **nouveau** dossier, pour garder l'historique : `-output /tp_avance/ca/output_v2`, ou
   `-output /tp_avance/ca/output_$(date +%Y%m%d_%H%M)`.

C'est l'incident **D** du TP 5 (`FileAlreadyExistsException`).

---

## 5. Bonus

### 5.1 Le même code sur les ventes de l'enseignant (`data/ventes.csv`, 41 ventes, séparateur `,`)
| Pays | CA |
|---|---|
| Allemagne | 589,89 |
| Belgique | 4 380,96 |
| Espagne | 3 733,88 |
| France | 1 439,31 |
| Italie | 1 277,81 |
| Royaume-Uni | 1 993,61 |

`Map input records=42`, `Lignes_valides=41`, `Reduce input groups=6`, aucun rejet. Les totaux ont été vérifiés par un
calcul indépendant (`awk`), identique à l'octet près.

⚠️ La ligne `2026-09-11,Allemagne,P001,2,13.38` est **en double**, et elle est comptée deux fois : le CA de l'Allemagne est
surévalué de 26,76 €. Les règles du TP 3 ne contrôlent pas l'**unicité** (voir §6).

### 5.2 Combiner
`-combiner "python3 reducer.py"` exécute le Reducer **côté Map**, avant le Shuffle. C'est possible car la somme est
**associative et commutative**, et parce que la sortie du Reducer a le même format (`pays<TAB>montant`) que son entrée.

| | Sans combiner | Avec combiner |
|---|---|---|
| Map output records | 41 | 41 |
| Combine output records | - | **11** |
| Reduce input records | 41 | **11** |
| Reduce shuffle bytes | 738 | **213** (−71 %) |
| Résultat | | **identique** |

⚠️ Un combiner ne convient pas à tous les calculs. Pour une **moyenne**, faire la moyenne des moyennes donne un résultat
faux : il faudrait émettre des couples (somme, nombre). Hadoop peut aussi exécuter le combiner **0, 1 ou plusieurs fois** :
le résultat ne doit pas en dépendre.

### 5.3 Plusieurs Reducers
Avec `-D mapreduce.job.reduces=2`, on obtient 2 fichiers. `part-00000` contient Espagne, France et Royaume-Uni ;
`part-00001` contient Allemagne, Belgique et Italie. Le **Partitioner** par défaut (`hash(clé) mod 2`) garantit qu'**un
même pays va toujours au même Reducer**, donc chaque total reste correct. En revanche, le résultat n'est plus trié
globalement : il l'est seulement à l'intérieur de chaque fichier.

---

## 6. Limites et améliorations

| Limite | Amélioration |
|---|---|
| **Doublons non détectés** (Allemagne, 2026-09-11) | Un job de déduplication en amont : clé = ligne entière, le Reducer n'émet qu'une occurrence (compteur `Doublon`). Ou une règle métier sur un identifiant de vente. |
| Les lignes rejetées ne sont **que comptées** (et visibles dans les journaux) | Les écrire dans `/data/rejects` (zone du TP 2), avec un 2e job ou `MultipleOutputs` en Java, pour pouvoir les corriger et les rejouer |
| Codes pays hétérogènes (`FR` / `France`) | Normalisation avec un **référentiel** de pays (jointure) |
| Pas de contrôle du produit ni de la date | Jointure avec `produits.csv` (prix de référence, produit existant) et validation de la date |
| Une seule règle comptée par ligne | Choix assumé (bilan exact). Variante : compter toutes les anomalies d'une ligne, mais le bilan ne s'additionne plus. |
| MapReduce écrit sur disque entre chaque étape, donc lent pour l'itératif | **Spark**, qui calcule en mémoire, ou **Hive** pour une syntaxe SQL (`SELECT pays, SUM(quantite*prix) … GROUP BY pays`). Voir TP 6. |
| CSV, format texte | **Parquet** ou **ORC**, formats colonnes compressés (TP 4) |

---

## 7. Questions probables du jury

**Q : Quelle est la différence entre HDFS et YARN ?**
HDFS **stocke**, avec le NameNode pour les métadonnées et les DataNodes pour les blocs. YARN **alloue les ressources et
exécute** les traitements, avec le ResourceManager et les NodeManagers. On peut utiliser HDFS sans YARN, par exemple pour
copier des fichiers, mais pas lancer de job MapReduce sur le cluster.

**Q : Le SecondaryNameNode remplace-t-il le NameNode en cas de panne ?**
**Non.** Il ne fait que des checkpoints (fusion de `fsimage` et `edits`). La haute disponibilité passe par un **Standby
NameNode**, avec des JournalNodes et ZooKeeper.

**Q : Pourquoi le NameNode ne voit-il jamais passer les données ?**
Pour ne pas devenir un goulot d'étranglement. Il ne renvoie que des **adresses de blocs**, et les clients lisent et
écrivent directement sur les DataNodes.

**Q : Combien de tâches Map et pourquoi ?**
Une par **split**. Ici 2, car Streaming utilise l'ancienne API, qui vise 2 splits par défaut, même pour un petit fichier.
Sur un vrai cluster, c'est environ une par bloc de 128 Mo. Le nombre de **Reducers**, lui, est **choisi** par le
développeur (`mapreduce.job.reduces`).

**Q : Pourquoi le Mapper écrit-il les compteurs sur stderr et pas sur stdout ?**
stdout est le **canal de données**. Tout ce qui y est écrit devient une paire clé/valeur envoyée au Reducer. stderr est
le canal de **contrôle** : Streaming y intercepte les lignes `reporter:counter:`.

**Q : Comment Hadoop sait-il quelle est la clé ?**
Par convention de Streaming : tout ce qui précède la **première tabulation** est la clé, le reste est la valeur.

**Q : Que se passe-t-il entre le Map et le Reduce ?**
Le **Shuffle and Sort** :
1. **côté Map** : partitionnement (`hash(clé) mod R`), tri en mémoire, *spill* sur le disque local (et combiner s'il existe) ;
2. **côté Reduce** : récupération des partitions par HTTP auprès du service `mapreduce_shuffle` des NodeManagers, puis
   fusion des flux triés.

C'est souvent l'étape **la plus coûteuse** d'un job.

**Q : Pourquoi les sorties Map ne sont-elles pas écrites dans HDFS ?**
Ce sont des données **intermédiaires et temporaires**. Les répliquer dans HDFS coûterait cher pour rien : si un nœud
tombe, on relance simplement la tâche Map.

**Q : Que se passe-t-il si une tâche Map plante ?**
L'ApplicationMaster la relance, si possible sur un autre nœud, jusqu'à 4 tentatives (`mapreduce.map.maxattempts`).
Les compteurs des tentatives échouées ne sont pas comptés. Si une tâche échoue 4 fois, le job échoue.

**Q : Pourquoi `Decimal` plutôt que `float` ?**
Les `float` sont en binaire : `0.1 + 0.2 = 0.30000000000000004`. Sur des montants, les erreurs s'accumulent. `Decimal`
calcule en base 10, exactement.

**Q : Pourquoi arrondir dans le Reducer et pas dans le Mapper ?**
Arrondir chaque ligne puis additionner accumule les erreurs d'arrondi. On garde la précision jusqu'au bout et on arrondit
**une seule fois** le total. C'est aussi ce que demande l'énoncé.

**Q : Pourquoi `Map output records` = `Lignes_valides` ?**
Parce que chaque ligne valide émet exactement une paire. Ce n'est pas une règle générale : un WordCount émet une paire
par mot.

**Q : Pourquoi l'en-tête a-t-il son propre compteur ?**
Ce n'est pas une anomalie. Le compter à part permet l'égalité lues = en-tête + valides + rejetées. Remarque : avec
plusieurs fichiers d'entrée, il y aurait un en-tête par fichier, ce que le compteur montrerait.

**Q : Votre Reducer marcherait-il avec 1 milliard de lignes ?**
Oui. Il ne garde en mémoire qu'**un pays et un total**, car il exploite le tri garanti par Hadoop. Une version naïve avec un
dictionnaire `{pays: total}` marcherait aussi ici (peu de pays), mais pas avec des millions de clés.

**Q : Pourquoi `-files` ?**
Pour copier les scripts sur chaque nœud de calcul via le **cache distribué**. Sans cette option, les conteneurs ne
trouveraient pas `mapper.py`, qui n'est présent que sur la machine cliente.

**Q : Comment voir les lignes rejetées après le job ?**
`yarn logs -applicationId <id> | grep REJET`, grâce à l'agrégation des journaux activée par
`yarn.log-aggregation-enable=true`. Ou l'interface web de YARN (port 8088).

**Q : Peut-on modifier une ligne de `ventes.csv` dans HDFS ?**
Pas en place : HDFS est *write-once*. On peut ajouter en fin de fichier (`-appendToFile`), remplacer le fichier
(`-put -f`) ou le tronquer. Ensuite, on **relance** le job : le résultat ne se met pas à jour tout seul.

**Q : Combiner, oui ou non ici ?**
Oui : la somme est associative et commutative. Sur le fichier de l'enseignant, le shuffle passe de 738 à 213 octets pour
un résultat identique. Ce ne serait pas valable tel quel pour une moyenne.

**Q : Que se passerait-il avec 2 Reducers ?**
2 fichiers `part-00000` et `part-00001`, chaque pays dans un seul des deux grâce au partitioner. Les totaux restent justes,
mais il n'y a plus de tri global.

**Q : Quelle différence entre MapReduce et Spark ?**
MapReduce écrit sur **disque** entre chaque étape et se limite à Map puis Reduce. Spark garde les données **en mémoire**,
enchaîne de nombreuses transformations et est beaucoup plus rapide pour l'itératif et l'interactif. Spark s'exécute
souvent **sur YARN** et lit dans **HDFS** : il remplace MapReduce, pas HDFS.

**Q : Qu'auriez-vous fait en Hive ?**
```sql
CREATE EXTERNAL TABLE ventes (dt STRING, pays STRING, produit STRING, quantite STRING, prix STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LOCATION '/tp_avance/ca/input'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT pays, ROUND(SUM(CAST(quantite AS INT) * CAST(prix AS DECIMAL(10,2))), 2) AS ca
FROM ventes
WHERE pays <> '' AND CAST(quantite AS INT) > 0 AND CAST(prix AS DECIMAL(10,2)) > 0
GROUP BY pays;
```
On perd cependant les **compteurs par type d'anomalie**, sauf à écrire une requête de contrôle séparée.

---

## 8. Antisèche des commandes

```bash
# Services
jps                                   # NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
hdfs dfsadmin -report                 # DataNodes vivants, capacité
yarn node -list                       # NodeManagers

# HDFS : insertion, lecture, modification, suppression
hdfs dfs -mkdir -p /tp_avance/ca/input
hdfs dfs -put -f ventes.csv /tp_avance/ca/input/
hdfs dfs -ls -R /tp_avance
hdfs dfs -cat /tp_avance/ca/input/ventes.csv
hdfs dfs -stat "%n %b %r %o" /tp_avance/ca/input/ventes.csv
hdfs fsck /tp_avance/ca/input/ventes.csv -files -blocks -locations
hdfs dfs -appendToFile nouvelle_vente.csv /tp_avance/ca/input/ventes.csv
hdfs dfs -get -f /tp_avance/ca/output/part-00000 resultat.txt
hdfs dfs -mv <src> <dst> ; hdfs dfs -cp <src> <dst>
hdfs dfs -rm -r -f /tp_avance/ca/output

# Test local
cat ventes.csv | ./mapper.py 1> sortie_map.txt 2> erreurs_map.txt
cat ventes.csv | ./mapper.py | sort | ./reducer.py

# Job
STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name 'hadoop-streaming-*.jar' | head -1)
hadoop jar "$STREAMING_JAR" -D mapreduce.job.name=TP_Avance_CA_Qualite -D mapreduce.job.reduces=1 \
  -files mapper.py,reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /tp_avance/ca/input -output /tp_avance/ca/output 2>&1 | tee execution.log

# Analyse
grep -E "Map input records|Map output records|Reduce output records" execution.log
grep -A8 "^\s*Qualite" execution.log
yarn application -list -appStates ALL
yarn logs -applicationId <application_id> | grep REJET
```
