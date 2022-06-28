# Deployment of LakeFS: Storage on AWS S3 buckets. 

* Create an environment variable file to house all sensitive credentials. 
```bash
LAKEFS_CONFIG_FILE=./.lakefs-env
echo "AWS_ACCESS_KEY_ID=AKIATLEGUPNKMJLK7I7R" > $LAKEFS_CONFIG_FILE \
echo "AWS_SECRET_ACCESS_KEY=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" >> $LAKEFS_CONFIG_FILE \
echo "LAKEFS_BLOCKSTORE_S3_ENDPOINT=https://8000-devilisdefe-deltalakeet-2n9nl4ym92x.ws-eu47.gitpod.io" >> $LAKEFS_CONFIG_FILE \
echo "LAKEFS_BLOCKSTORE_TYPE=s3" >> $LAKEFS_CONFIG_FILE \
echo "LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE=true" >> $LAKEFS_CONFIG_FILE
```

* Start the LakeFS container using the command below
```bash
docker-compose -f docker-compose.LakeFS.yaml up -d
```


* After Lake house is runnning, perform the following to setup LakeFS CLI. 
NOTE PLEASE MAKE SURE YOU SET UP LakeFS CLI before you log in to the LakeFS UI. 
```bash
wget https://github.com/treeverse/lakeFS/releases/download/v0.68.0/lakeFS_0.68.0_Linux_x86_64.tar.gz
```

* EXtract LakeFS binary file from the zipped file. 
```bash
tar -xf lakeFS_0.68.0_Linux_x86_64.tar.gz
```

* MOve the binary file to the right location.
```bash
mv lakectl /usr/local/bin
```

* List the available repository.
```bash
lakectl repo list
```

### Configure the LakeFS CLI 
* Now configure the LakeFS CLI with your AWS credentials.
```bash
lakectl config
```

* Use the LakeFS credentail you downloaded when you sign up to configure the CLI. 
```
lakectl config
Config file /home/gitpod/.lakectl.yaml will be used
Access key ID: AKIAJTJTCTTW37F6T4FQ
Secret access key: ****************************************
✔ Server endpoint URL: http://localhost:8000█
```


When you login to the LakeFS UI configure LakeFS UI with `storage_namespace=s3://new-wave-delta-lake/`


* Create LakeFS repository on the LakeFS UI, in my case my storage namespace is `s3://new-wave-delta-lake/`




### Configure the AWS CLI

* Get the AWS CLI package by using the command below.
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
```

* Unzip the file.
```bash
unzip awscliv2.zip
```

* Install the AWS CLI package and move the installation to the bin folder. 
```bash
./aws/install -i /usr/local/aws-cli -b /usr/local/bin
```

* Check the version of AWS installed.
```bash
aws --version
```

* Configure the AWS CLI
```bash
AWS Access Key ID [None]: AKIATLEGUPNKFGG2BAUA
AWS Secret Access Key [None]: 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
Default region name [None]: us-east-1
Default output format [None]: json
aws iam list-users
```

* List the AWS S3 bucket that is attached to LakeFS
```bash
aws --endpoint-url=https://8000-devilisdefe-deltalakeet-2n9nl4ym92x.ws-eu47.gitpod.io --profile local s3 ls
```

Sample S3 Bucket

My S3 Bucket: s3://new-wave-delta-lake/delta-lake-object/ 

* Kill the deployment using the command below. 
```bash
docker kill $(docker ps -q)
```


s3://new-wave-delta-lake/bronze/
s3://dataenginedemo/
https://dataenginedemo.s3.amazonaws.com/Unsaved/


aws s3 mb s3://bronze-/ --endpoint-url http://localhost:8000
make_bucket: lakefs



MAKE SURE YOU CREATE AWS S3 bucket on the UI first then, also configure the permision. 
lakectl repo create lakefs://repo s3://lakefsstoragea -d main

lakectl repo create lakefs://bronze s3://new-wave-delta-lake -d main

lakectl repo create lakefs://silver s3://new-wave-delta-lake-silver -d main

lakectl repo create lakefs://gold s3://new-wave-delta-lake-gold -d main



AWS S3 BUCKET PERMISION

```BASH
{
	"Version": "2012-10-17",
	"Id": "Policy1590051531320",
	"Statement": [
		{
			"Sid": "Stmt1590051522178",
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::XXXXXXXXXXX:role/delta-table"
			},
			"Action": [
				"s3:GetObject",
				"s3:GetObjectVersion",
				"s3:PutObject",
				"s3:AbortMultipartUpload",
				"s3:ListMultipartUploadParts",
				"s3:GetBucketVersioning",
				"s3:ListBucket",
				"s3:GetBucketLocation",
				"s3:ListBucketMultipartUploads",
				"s3:ListBucketVersions"
			],
			"Resource": [
				"arn:aws:s3:::lakefsstoragea",
				"arn:aws:s3:::lakefsstoragea/*"
			]
		}
	]
}
```


Repository: lakefs://repo
Repository 'repo' created:
storage namespace: s3://lakefs
default branch: main
timestamp: 1625919817

