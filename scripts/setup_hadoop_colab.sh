#!/usr/bin/env bash
# Installe et démarre Hadoop 3.5.0 en mode pseudo-distribué (HDFS + YARN) dans Google Colab.
# Rejouable : n'installe que ce qui manque et ne formate le NameNode qu'à la première exécution.
set -e

export HADOOP_VERSION=3.5.0
export HADOOP_HOME=/content/hadoop-$HADOOP_VERSION
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
CONF=$HADOOP_HOME/etc/hadoop

echo "=== 1. Paquets (Java 17, SSH, rsync) ==="
if [ ! -x $JAVA_HOME/bin/java ] || ! command -v sshd >/dev/null; then
  apt-get update -qq
  apt-get install -y -qq openjdk-17-jdk-headless openssh-server rsync wget > /dev/null
fi
$JAVA_HOME/bin/java -version 2>&1 | head -n 1

echo "=== 2. Hadoop $HADOOP_VERSION ==="
if [ ! -d $HADOOP_HOME ]; then
  wget -q https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -O /content/hadoop.tar.gz
  tar -xzf /content/hadoop.tar.gz -C /content
  rm -f /content/hadoop.tar.gz
fi
hadoop version | head -n 1

echo "=== 3. Configuration ==="
# Colab tourne en root : Hadoop 3 exige les variables *_USER (sinon start-dfs.sh échoue)
grep -q "TP_COLAB_FIX" $CONF/hadoop-env.sh || cat >> $CONF/hadoop-env.sh <<EOF
# TP_COLAB_FIX
export JAVA_HOME=$JAVA_HOME
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
EOF

cat > $CONF/core-site.xml <<'EOF'
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
EOF

cat > $CONF/hdfs-site.xml <<'EOF'
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.blocksize</name><value>134217728</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///content/hdfs/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///content/hdfs/data</value></property>
</configuration>
EOF

cat > $CONF/mapred-site.xml <<EOF
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
</configuration>
EOF

cat > $CONF/yarn-site.xml <<'EOF'
<configuration>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
  <property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value></property>
</configuration>
EOF

echo "=== 4. SSH sans mot de passe ==="
service ssh start > /dev/null
mkdir -p ~/.ssh
[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa > /dev/null
grep -qf ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys 2>/dev/null || cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null

echo "=== 5. Démarrage HDFS + YARN ==="
if [ ! -d /content/hdfs/name/current ]; then
  echo "Premier démarrage : formatage du NameNode"
  hdfs namenode -format -force -nonInteractive > /content/format_namenode.log 2>&1
fi
jps | grep -q " NameNode$" || start-dfs.sh
jps | grep -q " ResourceManager$" || start-yarn.sh
hdfs dfsadmin -safemode wait > /dev/null
sleep 5
jps
