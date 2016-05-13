#!/bin/bash 

read -p "The Public DNS Address" DNS

scp -i ~/cslab.pem ~/cs123_wiki_proj/jm/mount_s3.sh ec2-user@$DNS:~/
scp -i ~/cslab.pem ~/cs123_wiki_proj/jm/enonly.sh ec2-user@$DNS:~/
scp -i ~/cslab.pem ~/cs123_wiki_proj/jm/start_filter.sh ec2-user@$DNS:~/


