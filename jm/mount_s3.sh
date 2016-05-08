#!/bin/bash 

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
echo "access-key-id:secret" > .passwd-s3fs
chmod 600 .passwd-s3fs 
mkdir name-of-mount-directory
s3fs name-of-s3-bucket  name-of-mount-directory