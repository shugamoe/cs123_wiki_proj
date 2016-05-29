#!/bin/bash 
# This is a quick script utilizing the commands present on the cheatsheet from 
# on the class website to quickly mount our S3 Bucket to an instance.
#
# This was utilized in the ad-hoc cluster utilized to filter non-English 
# entries in the data.  See "filter_node.py".

read -p "Enter Key: " KEY 
read -p "Enter Secret Key: " SKEY

sudo yum -y groupinstall "Development Tools"
sudo yum -y install fuse fuse-devel autoconf automake curl-devel libxml2-devel openssl-devel mailcap
wget https://github.com/s3fs-fuse/s3fs-fuse/archive/v1.78.tar.gz
tar xzf v1.78.tar.gz 
cd s3fs-fuse-1.78
./autogen.sh 
./configure 
make
sudo make install
cd
echo "$KEY:$SKEY" > .passwd-s3fs
chmod 600 .passwd-s3fs 
mkdir s3_wiki
s3fs wikitrafv2big  s3_wiki