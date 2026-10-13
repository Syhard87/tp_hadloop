# TP 4 - Performance HDFS : petits fichiers, formats et partitionnement
## Guide pour le passage au tableau

> Tous les chiffres viennent d'une **exécution réelle** (Hadoop 3.5.0, Java 17, pseudo-distribué, comme dans Colab).
> Les durées varient un peu d'une machine à l'autre, mais les ordres de grandeur restent les mêmes.
> Notebook : `notebooks/TP4_Performance_HDFS.ipynb`. Scripts : `tp4/partitionner.py`, `tp4/comparer_formats.py`.

---

## 0. Les 6 chiffres à écrire au tableau

| | Petits fichiers | Compacté | |
|---|---|---|---|
| Fichiers / blocs | **200 / 200** | **1 / 1** | même donnée : 4 400 octets |
| Objets dans le NameNode | **401** | **3** | ÷ 134 |
| Durée du `put` | **55 s** | **2 s** | ≈ 25× |
| Tâches Map pour compter 200 lignes | **200** | **2** | ÷ 100 |
| Durée du job | **343 s** | **28 s** | ≈ 12× |
| Requête « 1 jour × 1 pays » sur 240 partitions | - | **1 fichier ouvert** | *partition pruning* |

**Phrase d'accroche** : « 4 400 octets, c'est 0,003 % d'un bloc HDFS. Pourtant, en 200 fichiers, ces 4 400 octets
mettent 12 fois plus de temps à être traités qu'en un seul fichier. Le problème n'est pas le volume : c'est le
**nombre d'objets**. »

---

## 1. Schémas à dessiner

### 1.1 Pourquoi les petits fichiers coûtent cher (Missions A et B)
```
                    ┌────────────────────────── NameNode (RAM) ──────────────────────────┐
  200 petits        │  /tp4/raw/events  (1 répertoire)                                    │
  fichiers   ──────▶│  event_001.csv → blk_1   event_002.csv → blk_2   …   (200 + 200)   │  ≈ 401 objets × 150 o
  (22 o chacun)     └─────────────────────────────────────────────────────────────────────┘
                              │ 200 blocs à suivre, 200 rapports de blocs
                              ▼
                    DataNode : 200 fichiers blk_ + 200 fichiers .meta sur le disque

  MapReduce :  200 fichiers ──▶ 200 splits ──▶ 200 tâches Map ──▶ 200 JVM démarrées pour 22 octets chacune

  ─────────────────────────────── après compaction ───────────────────────────────
  1 fichier (4 400 o) ──▶ NameNode : 1 fichier + 1 bloc + 1 répertoire = 3 objets ──▶ 1 à 2 tâches Map
```

### 1.2 Partitionnement (Mission C)
```
/data/curated/events/
└── year=2026/                      ← filtre le plus fréquent : la période
    └── month=10/
        ├── day=14/
        └── day=15/                 ← WHERE date = '2026-10-15'      → 8 fichiers lus sur 240
            ├── country=BE/
            ├── country=FR/         ← AND country = 'FR'             → 1 fichier lu sur 240
            │   └── part-0.parquet
            └── …
```

### 1.3 Ligne contre colonne (Question 29)
```
Format LIGNE (CSV, Avro)              Format COLONNE (Parquet, ORC)
date|pays|produit|qte|montant|…       date   : [d1 d2 d3 …]          ← lu
date|pays|produit|qte|montant|…       pays   : [FR BE FR …]          ← lu
date|pays|produit|qte|montant|…       produit: [P1 P7 P3 …]          ✗ ignoré
→ pour lire 3 colonnes,               montant: [12 40 7 …]           ← lu
  on lit TOUTES les colonnes          + min/max par bloc : on saute les blocs hors de la date demandée
```

---

## 2. Notions à maîtriser

| Notion | Définition courte |
|---|---|
| **Problème des petits fichiers** | Des fichiers bien plus petits qu'un bloc (128 Mo). Chacun coûte autant de métadonnées qu'un gros fichier et lance sa propre tâche. |
| **Objet de l'espace de noms** | Fichier, répertoire ou bloc connu du NameNode. Environ **150 octets de heap** chacun (ordre de grandeur admis). |
| **Heap du NameNode** | La mémoire Java du NameNode, qui contient **toute** l'arborescence et la carte des blocs. Elle limite la taille du cluster. |
| **Split** | Portion d'entrée traitée par une tâche Map. Un split ne chevauche **jamais** deux fichiers, donc au moins **1 split par fichier**. |
| **Compaction** | Regrouper de nombreux petits fichiers en quelques gros fichiers, sans changer les données. |
| **CombineTextInputFormat** | Format d'entrée MapReduce qui place **plusieurs petits fichiers dans un même split**. C'est un palliatif côté calcul, qui ne soulage pas le NameNode. |
| **HAR (Hadoop Archive)** | `hadoop archive` empaquète des fichiers dans un `.har` : moins d'objets pour le NameNode, mais l'archive est immuable et lente à lire. |
| **Quota de noms** | `hdfs dfsadmin -setQuota N <dossier>` : un dossier ne peut pas dépasser N objets. Protège le NameNode. |
| **Partitionnement (style Hive)** | Répertoires `cle=valeur`. Les moteurs de requête lisent la clé comme une colonne. |
| **Partition pruning (élagage)** | Le moteur n'ouvre que les répertoires qui correspondent au filtre `WHERE`. |
| **Format ligne / colonne** | Ligne : les enregistrements sont stockés à la suite (CSV, Avro). Colonne : les valeurs d'une colonne sont stockées ensemble (Parquet, ORC). |
| **Projection (column pruning)** | Ne lire que les colonnes demandées. Possible seulement avec un format colonne. |
| **Predicate pushdown** | Utiliser les statistiques min/max stockées dans le fichier pour sauter des blocs entiers. |
| **Compression découpable (splittable)** | Un fichier compressé peut-il être lu en plusieurs morceaux en parallèle ? **gzip : non**. bzip2 : oui. Parquet, ORC et Avro : oui, car ils compressent par blocs internes. |

---

## 3. Mission A - Reproduire le problème

**Ce que font les commandes** :
1. La boucle `for` crée 200 fichiers `event_001.csv` … `event_200.csv`, d'une ligne chacun (`seq -w` met des zéros
   devant : 001, 002…).
2. `hdfs dfs -put …/*.csv` les envoie **un par un**. Pour chacun, le client fait : création auprès du NameNode,
   allocation d'un bloc, pipeline d'écriture vers le DataNode, fermeture et confirmation au NameNode.
3. `hdfs dfs -count -q -h` affiche les quotas et les compteurs du dossier.
4. `hdfs dfs -du -h | head` montre la taille de chaque fichier.

**Sortie obtenue** :
```
QUOTA  REM_QUOTA  SPACE_QUOTA  REM_SPACE_QUOTA  DIR_COUNT  FILE_COUNT  CONTENT_SIZE  PATHNAME
 none        inf         none              inf          1         200         4.3 K  /tp4/raw/events
22  22  /tp4/raw/events/event_001.csv     (taille, et espace consommé avec la réplication)
```

**23. Compter les fichiers** : **200 fichiers** (`FILE_COUNT`), 1 répertoire, et **200 blocs** selon `fsck`
(`avg. block size 22 B`).

**24. Volume contre nombre d'objets** : 4 400 octets au total, soit 0,003 % d'un bloc de 128 Mo, mais **401 objets**
pour le NameNode (200 fichiers + 200 blocs + 1 répertoire). Dans un seul fichier, il n'en faudrait que **3**.
Le DataNode, lui, stocke **400 fichiers** sur son disque (200 blocs + 200 fichiers `.meta` de sommes de contrôle).
Le `put` a pris **55 s** pour 4,3 Ko, contre 2 s pour le même contenu en un seul fichier.

**25. Pourquoi les métadonnées sont surchargées** :
- Le NameNode garde toutes les métadonnées **en RAM**, environ 150 octets par objet, **quelle que soit la taille du
  fichier**. Un fichier de 22 octets coûte autant qu'un fichier de 128 Mo.
- **Extrapolation** : 1 fichier par minute donne 525 600 fichiers par an, soit environ 1 million d'objets et environ
  150 Mo de heap **par flux**. 100 applications représentent environ 15 Go de heap. Le NameNode est un serveur
  **unique** : sa mémoire plafonne le cluster, et les pauses du *garbage collector* le ralentissent.
- Chaque opération (`ls`, `open`, `put`) est une **requête RPC au NameNode**, et 200 fichiers multiplient ces requêtes
  par 200.
- **Côté calcul**, 1 fichier = au moins 1 split = 1 tâche Map = 1 JVM : on passe plus de temps à démarrer des JVM qu'à
  traiter des données.

**Bonus quota** : avec `hdfs dfsadmin -setQuota 50 /tp4/quota`, le 50e objet est refusé :
`The NameSpace quota (directories and files) of directory /tp4/quota is exceeded: quota=50 file count=51`.
Le répertoire compte pour 1, donc seuls 49 fichiers passent. C'est un garde-fou d'administrateur contre les petits fichiers.

---

## 4. Mission B - Compacter

**Ce que font les commandes** :
1. `cat …/*.csv > events_compactes.csv` **concatène** les 200 fichiers : 200 lignes, 4 400 octets.
2. `put -f` dépose le fichier dans `/tp4/curated/events`.
3. `fsck … -files -blocks -locations` affiche `4400 bytes, 1 block(s)`, `len=4400 Live_repl=1`.

**Comparaison** :

| | Avant | Après |
|---|---|---|
| Fichiers | 200 | **1** |
| Blocs | 200 | **1** |
| Objets NameNode | 401 | **3** |
| Durée du put | ≈ 55 s | ≈ 2 s |
| Tâches Map | 200 (343 s) | 2 (28 s) |

**Gain pour le NameNode** : 134 fois moins d'objets en mémoire, 200 fois moins de blocs dans les rapports des DataNodes,
et une seule requête `getBlockLocations` au lieu de 200 pour lire la donnée.

**Gain pour les traitements** : une seule ouverture de fichier, une lecture séquentielle, et 100 fois moins de tâches
Map, donc de JVM à démarrer. La mesure le montre : 343 s contre 28 s pour **compter les mêmes 200 lignes**.

**Points à mentionner** :
- **Vérifier avant de supprimer** : même nombre de lignes avant et après (200 = 200). On peut compacter directement
  depuis HDFS avec `hdfs dfs -getmerge` (le notebook vérifie que le résultat est identique) ou avec un job
  MapReduce ou Spark.
- **`CombineTextInputFormat`** réduit le nombre de tâches (1 au lieu de 200, 20 s), mais **ne réduit pas** le nombre
  d'objets du NameNode. C'est un palliatif, pas une solution.

---

## 5. Mission C - Partitionnement

**26. Clés** : `year`, `month` et `day` (date de l'**événement**), puis `country`.
`/data/curated/events/year=2026/month=10/day=15/country=FR/part-0.parquet`

**27. Ordre** : du plus général au plus fin, en suivant les filtres les plus fréquents.
1. **La date d'abord** : presque toutes les requêtes portent sur une période, et une plage de dates devient un
   sous-arbre contigu.
2. **Le pays ensuite**, pour affiner. Il a peu de valeurs (moins de 10).
3. **Le cycle de vie** suit le temps : supprimer octobre 2025 = supprimer **un** répertoire.
4. **L'écriture** suit le temps : le traitement du jour n'écrit que dans `day=` du jour.

**Démonstration de l'élagage** (`partitionner.py`, 240 fichiers) :
| Requête | Fichiers ouverts |
|---|---|
| 1 jour + 1 pays | **1** / 240 |
| 1 jour | 8 / 240 |
| 1 pays, tout le mois | 30 / 240 |
| 1 produit (pas une clé de partition) | **240** / 240 |

**28. Risque des partitions trop fines** : chaque combinaison crée un répertoire et au moins un fichier, ce qui
**recrée le problème des petits fichiers**. Avec notre volume (environ 14 000 événements par jour) :

| Partitionnement | Fichiers / an | Taille moyenne |
|---|---|---|
| year/month | 12 | 9,2 Mo |
| year/month/day | 365 | 306 Ko |
| year/month/day/country | 2 920 | **38 Ko** ⚠️ |
| + hour | 70 080 | 1,6 Ko ❌ |
| 1 fichier par minute | 525 600 | 0,2 Ko ❌ |

**Réponse nuancée, qui fera la différence** : la forme proposée par l'énoncé est bonne **si chaque partition jour × pays
pèse au moins plusieurs centaines de Mo**. Sinon, on s'arrête à `year/month/day` (voire `year/month`) et `country`
devient une **colonne triée**. Les statistiques min/max de Parquet suffisent alors à sauter les blocs inutiles.
Règle pratique : **on partitionne sur ce qui filtre souvent ET qui a une cardinalité faible à moyenne**. Jamais sur un
identifiant ou un horodatage précis.

**29. Formats** (mesuré sur 432 000 événements et 8 colonnes, requête « CA par pays un jour donné ») :

| | CSV gzip | Avro | Parquet | ORC |
|---|---|---|---|---|
| Organisation | ligne, texte | ligne, binaire | **colonne** | **colonne** |
| Schéma | non | oui (évolution de schéma) | oui | oui |
| Découpable | **non** | oui | oui | oui |
| Taille | 5,1 Mo | 5,2 Mo | 6,0 Mo | **3,2 Mo** |
| Requête | 0,17 s | 2,0 s* | **0,03 s** | 0,06 s |
| Point fort | universel, échange | ingestion, flux, Kafka | analyse, standard Spark | analyse Hive, très compact |

\* lecture Python ligne par ligne, ce qui exagère la lenteur, mais un format ligne doit bien tout décoder.

**Démonstration « gzip non découpable »** : avec des blocs de 1 Mo, le CSV (26 Mo, 25 blocs) donne **25 tâches Map**,
alors que le CSV gzip (5 Mo, **5 blocs**) n'en donne qu'**une**. Une seule tâche doit décompresser tout le fichier, en
lisant à distance 4 blocs sur 5 : on perd le parallélisme et la localité des données. Sur notre machine unique,
25 tâches coûtent plus qu'elles ne rapportent (55 s contre 19 s). Sur un vrai cluster, elles tourneraient **en parallèle**
sur 25 nœuds.

**Recommandation** : **Parquet** (ou ORC dans un environnement Hive) pour l'analyse, **Avro** pour l'ingestion, et
**pas de CSV gzip** pour l'analyse.

---

## 6. Mission D - Recommandation (15 lignes)

1. **Fichier par minute** : seulement en **zone d'atterrissage temporaire**, jamais comme stockage durable (525 600 fichiers par an et par flux).
2. **Compaction planifiée** à la clôture d'une période (chaque nuit pour la veille), et **sur seuil** (trop de fichiers ou taille moyenne trop faible).
3. **Compaction sûre** : écriture dans un dossier temporaire, contrôle des lignes, `mv` atomique et `_SUCCESS`, puis suppression des petits fichiers.
4. **Format** : **Parquet** compressé (snappy ou zstd) pour l'analyse, ORC si l'environnement est Hive ; Avro pour l'ingestion ; pas de CSV gzip.
5. **Partitionnement** : `year/month/day`, et `country` seulement si chaque partition jour × pays reste volumineuse. Sinon, une colonne triée.
6. **Taille cible** : pas de règle universelle. Des fichiers de l'ordre d'un ou plusieurs blocs (centaines de Mo), qui amortissent ouverture et tâche, mais en nombre suffisant pour occuper tous les cœurs. On ajuste au volume : si une journée pèse 300 Ko, on regroupe au mois.
7. **Contrôles automatisés** : nombre de fichiers et taille moyenne par partition (`-count`), objets du NameNode (`FilesTotal`, `BlocksTotal`), quotas de noms, égalité des lignes avant et après, `_SUCCESS`, `fsck`, rétention.

---

## 7. Questions probables et réponses

**Q : Un petit fichier gaspille-t-il 128 Mo de disque ?**
**Non.** Un bloc n'occupe sur disque que sa taille réelle (22 octets). Le coût est dans la **mémoire du NameNode** et dans
le **nombre de tâches**, pas dans l'espace disque.

**Q : D'où vient le chiffre de 150 octets par objet ?**
C'est un **ordre de grandeur** couramment admis pour un fichier, un répertoire ou un bloc dans la heap du NameNode.
Le chiffre exact dépend de la version et de la longueur des noms. Ce qui compte, c'est que le coût est **par objet, pas par octet**.

**Q : Pourquoi ne pas simplement augmenter la RAM du NameNode ?**
Ça repousse la limite sans régler le problème. Une heap énorme provoque de longues pauses du *garbage collector* et un
redémarrage plus lent (rechargement de `fsimage`). Les petits fichiers pénalisent aussi les **traitements**, pas
seulement le NameNode.

**Q : Pourquoi le `put` de 200 fichiers prend-il 55 s ?**
Pour chaque fichier, le client fait plusieurs allers-retours avec le NameNode (création, allocation du bloc, fermeture
avec attente de confirmation) et ouvre un pipeline vers le DataNode. Ce coût fixe se répète 200 fois, alors que le
transfert de 22 octets est négligeable.

**Q : CombineTextInputFormat résout-il le problème ?**
Seulement côté calcul : 1 tâche au lieu de 200. Le NameNode garde ses 401 objets. La vraie solution est la **compaction**.

**Q : Pourquoi la date avant le pays dans l'arborescence ?**
Parce que la date est le filtre le plus fréquent, que la rétention se fait par date (un répertoire à supprimer) et que
les données arrivent dans l'ordre chronologique.

**Q : Pourquoi ne pas partitionner par heure, voire par minute ?**
Ça multiplie les répertoires et les fichiers minuscules : 70 080 fichiers de 1,6 Ko par an pour l'heure. On recrée
exactement le problème de la Mission A.

**Q : Pourquoi Parquet est-il plus rapide ?**
Trois raisons :
1. il ne lit que les **colonnes demandées** (projection) ;
2. il **saute les blocs** dont les statistiques min/max excluent le filtre (*predicate pushdown*) ;
3. des valeurs similaires stockées ensemble se **compressent** très bien.

**Q : Pourquoi ORC est-il plus petit que Parquet ici ?**
Les deux sont des formats colonnes. ORC utilise ici zlib (compression plus forte, plus lente) et Parquet utilise snappy
(plus rapide, moins compact). Avec la même compression, l'écart se réduit. Le choix dépend surtout de l'écosystème :
ORC pour Hive, Parquet ailleurs.

**Q : Pourquoi gzip n'est-il pas découpable ?**
Un flux gzip se décompresse **depuis le début** : on ne peut pas commencer au milieu. Hadoop doit donc donner tout le
fichier à **une seule** tâche. bzip2 est découpable, et Parquet, ORC et Avro le sont aussi car ils compressent par blocs indépendants.

**Q : Quand garder Avro ?**
Pour l'**ingestion** et les **flux** (Kafka) : l'écriture ligne par ligne est rapide, le schéma voyage avec les données
et peut évoluer (ajout de champs). Ensuite, on convertit en Parquet pour l'analyse.

**Q : Quelle taille de fichier viser ?**
Il n'y a **pas de règle universelle**, c'est un compromis :
- **assez gros** pour amortir l'ouverture du fichier, les métadonnées et le lancement d'une tâche, donc de l'ordre d'un
  ou plusieurs blocs, soit des centaines de Mo ;
- **assez nombreux** pour paralléliser sur tous les cœurs du cluster.

On l'ajuste au **volume réel** et au **moteur** utilisé.

**Q : Comment détecter le problème en production ?**
- `hdfs dfs -count` par répertoire, en calculant la **taille moyenne** (`CONTENT_SIZE / FILE_COUNT`) ;
- les métriques du NameNode (`FilesTotal`, `BlocksTotal`, heap utilisée) via JMX ou l'interface web (port 9870) ;
- `fsck` (`avg. block size`) ;
- des alertes sur seuil ;
- des quotas de noms.

---

## 8. Antisèche
```bash
hdfs dfs -count -q -h -v <dossier>        # quotas, nb répertoires, nb fichiers, taille
hdfs dfs -du -h <dossier> | head          # taille par fichier
hdfs fsck <dossier> | grep -E "Total (files|blocks)|avg"   # blocs et taille moyenne
hdfs dfsadmin -setQuota 50 <dossier>      # quota de noms ; -clrQuota pour le retirer
hdfs dfs -getmerge <dossier_hdfs> <fichier_local>          # compaction simple
hadoop jar $J -inputformat org.apache.hadoop.mapred.lib.CombineTextInputFormat …   # palliatif côté calcul
hdfs dfs -D dfs.blocksize=1048576 -put …  # taille de bloc spécifique à un fichier
```
