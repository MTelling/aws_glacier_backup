#!/bin/bash
# dependencies, jq, parallel and awscli
# Original file is glacierupload.sh

# Test that parameters exist
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
	echo; echo;
	echo "Syntax: $0 [ Source File Name ] [ Archive Description ] [ Vault Name ]"
	echo; echo;
 	exit 1
fi

# get the arguments
fileName=$1
archiveDescription=$2
vaultName = $3

# manipulate vaultname and bytesize here
chunkMb=32
byteSize=$( bc <<<"1024*1024*$chunkMb" )

# calculate needed values 
archiveCheckSum=$(/usr/local/bin/awstreehash $fileName | head -c 64)
archiveSize=$(stat --printf="%s" $fileName)

# we make the parts folder of the 25 first characters of the sha sum of the file name
# this name should be unique to avoid collisions on folders
partsFolder="parts_$(sha256sum $fileName | cut -c1-25)"
echo "Creating [ $partsFolder ]"
mkdir $partsFolder

# now split the file
echo "Splitting file"
split -b $byteSize $fileName "$partsFolder/part_"

# Initiate the multipart download to AWS
init=$(aws glacier initiate-multipart-upload --account-id - --part-size $byteSize --vault-name $vaultName --archive-description $archiveDescription)
echo "Initialized multipart upload"

# Get the upload ID
uploadId=$(echo $init | jq '.uploadId' | xargs)
echo "Upload ID [ $uploadId ]"

# Create temp file to hold upload commands
commandFile="$partsFolder/commands.txt"
touch $commandFile

# get the list of part files to upload.
files=$(ls $partsFolder | grep "^part_")

# Populate the commands script with upload command for each part
currentByte=0
for f in $files
  do
     byteStart=$currentByte
     byteEnd=$((currentByte+byteSize-1))
     if [ $byteEnd -ge $archiveSize ]
     then
         byteEnd=$((archiveSize-1))
     fi
     echo aws glacier upload-multipart-part --body "$partsFolder/$f" --checksum $(/usr/local/bin/awstreehash "$partsFolder/$f" | head -c 64) --range "'"'bytes '"$byteStart"'-'"$byteEnd"'/*'"'" --account-id - --vault-name $vaultName --upload-id $uploadId >> $commandFile
     currentByte=$((currentByte+byteSize))
  done

# run upload commands in parallel
#   --load 100% option only gives new jobs out if the core is than 100% active
#   -a commands.txt runs every line of that file in parallel, in potentially random order
#   --notice supresses citation output to the console
#   --bar provides a command line progress bar
parallel --load 100% -a $commandFile --no-notice --bar --jobs 20 --eta

# Complete the multipart download
aws glacier complete-multipart-upload --archive-size $archiveSize --checksum $archiveCheckSum --upload-id $uploadId --account-id - --vault-name $vaultName

# Clean up
rm -rf $partsFolder
