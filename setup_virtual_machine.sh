#!/bin/bash
echo "installation de github..."
sudo apt install git

# repo github
echo "clonage du dépôt spark..."
git clone https://github.com/Didayolo/spark

# sage sources
echo "téléchargement de sage..."
wget www-ftp.lip6.fr/pub/math/sagemath/src/sage-7.5.1.tar.gz
echo "décompression du dossier sage..."
tar xzf sage-7.5.1.tar.gz

# verif des prerequis a l'installation de sage
echo "vérification de prérequis à l'installation de sage..."
which gcc make m4 perl ar ranlib tar python
# installation en cas de manque
echo "installation des prérequis..."
sudo apt install binutils gcc make m4 perl tar

# on compile
echo "compilation de sage..."
cd sage-7.5.1
make

#Téléchargement puis installation de spark depuis cette adresse:
echo "téléchargement et installation de spark..."
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz

#installation de la jvm (pour spark)
echo "installation de la jvm (pour spark)..."
sudo apt-add-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer

#ajout de 127.0.0.1 combispark dans /etc/hosts
sudo sed -i "1i\127.0.0.1 $(hostname)" /etc/hosts

#affichage des erreurs (warnings?) uniquement dans la console
cd /usr/lib/spark-2.1.0/conf
cp log4j.properties.template log4j.properties
remplacer : log4j.rootCategory=INFO, console
par log4j.rootCategory=ERROR(WARN), console

# optionnel
sudo apt install ipython
sudo apt install ipython-notebook
