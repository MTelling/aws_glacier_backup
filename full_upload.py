# Python script to backup photos to Glacier. 
# 
# Author: Morten Telling
#
# Credits to tbumi for inspiration for some parts (https://github.com/tbumi/glacier-upload)

import click
import os, math, datetime
import boto3
from botocore.client import ClientError
from treehash import TreeHash

NUM_THREADS = 10
MAX_RETRIES = 10

def get_treehash_for_data(data):
	treehash = TreeHash()
	treehash.update(data)
	return treehash.hexdigest()

def get_treehash(archive):
	treehash = TreeHash()
	file = open(archive, 'r')
	treehash.update(file.read())
	file.close()
	return treehash.hexdigest()

def exit_if_not_archive_exists(archive):
	try:
		if not os.path.exists(archive):
			raise Exception("File not found") 
	except Exception, e: 
		click.secho("Archive not found '{0}'. Exiting!\n".format(archive), fg='red')
		exit(1)

def exit_if_not_vault_exists(glacier_client, vault_name):
	try:
		response = glacier_client.describe_vault(vaultName=vault_name)
	except Exception, e:
		click.echo("")
		click.echo(e)
		click.echo("")
		click.secho("Glacier vault not found! Exiting!\n", fg='red')
		exit(1)

def exit_if_not_bucket_exists(bucket, s3):
	try:
	    s3.meta.client.head_bucket(Bucket=bucket.name)
	except ClientError, e:
		click.echo("")
		click.echo(e)
		click.echo("")
		click.secho("S3 bucket not found! Exiting!\n", fg='red')
		exit(1)

def exit_if_not_part_size_is_right(part_size):
	if not math.log(part_size, 2).is_integer():
		click.secho("Part size must be a power of 2.", fg='red')
		exit(1)
	if part_size < 1 or part_size > 4096:
		click.secho("Part size must larger than 1 and less than 4096 MB.", fg='red')
		exit(1)

def verify_all_prereqs(archive, glacier_client, vault_name, bucket, s3, parts_size):
	exit_if_not_archive_exists(archive)
	exit_if_not_vault_exists(glacier_client, vault_name)
	exit_if_not_bucket_exists(bucket, s3)
	exit_if_not_part_size_is_right(parts_size)


def get_size(archive):
	return os.path.getsize(archive)

def upload_single_archive_to_glacier(archive, description, glacier_client, 
	                                 vault_name, treehash):
	file = open(archive, 'r')
	click.echo("Archive will be uploaded as single file to glacier.")
	click.secho("Upload started!", fg='green')
	response = err = None
	try:
		response = glacier_client.upload_archive(
		    archiveDescription=description,
		    body=file,
		    checksum=treehash,
		    vaultName=vault_name,
		)

		click.echo(response)
		click.secho("Upload completed!", fg='green')
	except Exception, e:
		err = e

	file.close()
	return response, err

def upload_part(archive, archive_size, part_offset, part_size_bytes, 
	            parts_count, glacier_client, vault_name, upload_id):
	file = open(archive, 'r')
	file.seek(part_offset)
	part = file.read(part_size_bytes)
	
	byte_range = 'bytes {}-{}/{}'.format(
    	part_offset, part_offset + len(part) - 1, archive_size)
	part_number = part_offset // part_size_bytes
	percentage = part_number / parts_count

	click.echo("Uploading part {0} of {1}. {2}%".format(
		part_number, parts_count, percentage))


	for i in range(MAX_RETRIES):
		checksum = get_treehash_for_data(part)
		try:
			response = glacier_client.upload_multipart_part(
			vaultName=vault_name, uploadId=upload_id,
			range=byte_range, body=part, checksum=checksum)
			file.close()
			return
		except Exception, e:
			click.echo()
			click.echo(e)
			click.echo()
			click.secho("Upload error for part {0}. Retry no. {1}".format(
			part_number, i), fg='red')
			continue


	click.echo()
	click.secho("!! ERROR !!", fg='red')
	click.secho("Part {0} of {1} failed to upload after {2} retries.".format(
		part_number, parts_count, MAX_RETRIES), fg='red')
	click.secho("Upload Id: {0}".format(upload_id), fg='red')
	click.secho("Upload needs to be manually aborted or retried.")
	click.secho("!!!!!!")
	exit(1)

def initiate_multipart_upload(glacier_client, vault_name, description, part_size_bytes):
	try:
		response = glacier_client.initiate_multipart_upload(
			vaultName=vault_name,
			archiveDescription=description,
			partSize=str(part_size_bytes)
		)
	except Exception, e:
		click.echo("")
		click.echo(e)
		click.echo("")
		click.secho("Could not initiate multipart upload! Exiting!\n", fg='red')
		exit(1)

	return response['uploadId']

def abort_multipart_upload(glacier_client, vault_name, upload_id):
	try:
		response = glacier_client.abort_multipart_upload(
			vaultName=vault_name,
			uploadId=upload_id,
		)
	except Exception, e:
		click.echo("")
		click.echo(e)
		click.echo("")
		click.secho("Upload ID: {0}".format(upload_id), fg='red')
		click.secho("Could not abort multipart upload! Please abort manually!\n", fg='red')
		exit(1)

def complete_multipart_upload(glacier_client, vault_name, archive_size, 
	                          checksum, upload_id):
	try:
		response = glacier_client.complete_multipart_upload(
			vaultName=vault_name,
			uploadId=upload_id,
			archiveSize=str(archive_size),
			checksum=checksum
		)
	except Exception, e:
		click.echo("")
		click.echo(e)
		click.echo("")
		click.secho("Upload ID: {0}".format(upload_id), fg='red')
		click.secho("Could not complete multipart upload! Aborting upload!\n", fg='red')
		abort_multipart_upload(glacier_client, vault_name, upload_id)
		exit(1)


def upload_multipart_archive_to_glacier(archive, archive_size, part_size_bytes, 
		                                description, glacier_client, vault_name,
		                                checksum):
	click.echo("Archive will be uploaded as multi-part upload to glacier.")
	response = err = None
	part_offsets = [offset for offset in range(0, archive_size, part_size_bytes)]

	upload_id = initiate_multipart_upload(
		glacier_client, vault_name, description, part_size_bytes)
	click.echo()
	click.echo("Initiated upload.\nUpload Id: {0}".format(upload_id))

	for part_offset in part_offsets:
		# Remove try/catch. We should not abort. 
		try:
			upload_part(archive, archive_size, part_offset, part_size_bytes, 
		                len(part_offsets), glacier_client, vault_name, upload_id)
		except Exception, e:
			abort_multipart_upload(glacier_client, vault_name, upload_id)
			raise e
	
	complete_multipart_upload(
		glacier_client, vault_name, 
		archive_size, checksum, 
		upload_id)

	click.secho("Upload to glacier completed!", fg='green')

	return response, err

def create_info_file(archive, archive_size, description, event, glacier_response):
	archive_paths = archive.split("/")
	archive_name = archive_paths[len(archive_paths) - 1]
	info_file_name = archive_name.split(".")[0] + "_info.json"

	with open(info_file_name, 'w') as f:
		f.write("{\n")
		f.write("\t\"archiveName\": \"{0}\",\n".format(archive_name))
		f.write("\t\"dateTime\": \"{0}\",\n".format(datetime.datetime.now()))
		f.write("\t\"description\": \"{0}\",\n".format(description))
		f.write("\t\"archive_size\": \"{0}b\",\n".format(archive_size))
		f.write("\t\"events\": \n\t[\n")
		if len(event) >= 1:
			f.write("\t\t\"{0}\"".format(event[0]))
		for i in range(1, len(event)):
			f.write(",\n\t\t\"{0}\"".format(event[i]))
		f.write("\n\t],\n")
		f.write("\t\"uploadDetails\":\n\t{\n")
		f.write("\t\t\"archiveId\": \"{0}\",\n".format(glacier_response['archiveId']))
		f.write("\t\t\"location\": \"{0}\",\n".format(glacier_response['location']))
		f.write("\t\t\"checksum\": \"{0}\"\n".format(glacier_response['checksum']))
		f.write("\t}\n}\n")

	return info_file_name

def upload_file_to_s3(file, bucket):
	bucket.upload_file(file, file)

def upload_archive(archive, archive_size, part_size_bytes, description,
	               event, glacier_client, vault_name, bucket):
	click.echo()
	checksum = get_treehash(archive)
	click.echo("Tree hash of file: {0}".format(checksum))

	response = err = None
	if part_size_bytes >= archive_size:
		response, err = upload_single_archive_to_glacier(
			archive, description, glacier_client, vault_name, checksum)
	else:
		response, err = upload_multipart_archive_to_glacier(
			archive, archive_size, part_size_bytes, 
			description, glacier_client, vault_name, checksum)

	if err:
		click.secho("Upload aborted!", fg='red')
		click.secho("Error occured uploading file!\n{0}".format(err), fg='red')
		exit(1)

	# info_file = create_info_file(archive, archive_size, description, event, response)
	# click.echo("Created info file: {0}".format(info_file))
	
	# upload_file_to_s3(info_file, bucket)
	# click.echo("Successfully uploaded info file to S3.")

@click.command()
@click.option(
	'-a', '--archive', 
	required=True, prompt=True,
	help='Path to archive to push to Glacier.')
@click.option(
	'-v', '--vault-name', 
	required=True, prompt=True,
	help='Name of Glacier vault.')
@click.option(
	'-s', '--s3-bucket',
	prompt=True,
	required=True,
	help='Name of S3 bucket to put info file with upload details.')
@click.option(
	'-d', '--description',
	prompt=True,
	required=True,
	help='Description of the archive.')
@click.option(
	'-e', '--event',
	multiple=True,
	required=True,
	help='Events covered by archive')
@click.option(
	'-s', '--part-size',
	default=24,
	help='Part size in megabytes')

def main(archive, vault_name, s3_bucket, description, 
	     event, part_size):
	click.echo("")

	click.secho("The upload process will now begin.", fg='green')
	click.echo("Glacier vault: {0}".format(vault_name))
	click.echo("S3 Bucket: {0}".format(s3_bucket))
	click.echo("Archive path: {0}".format(archive))

	glacier_client = boto3.client('glacier')
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(s3_bucket)

	verify_all_prereqs(archive, glacier_client, vault_name, bucket, s3, part_size)

	archive_size = get_size(archive)
	click.echo("Size of archive: {0} ({1} megabytes)".format(
		archive_size, (archive_size / 1024 / 1024)))

	part_size_bytes = part_size * 1024 * 1024
	click.echo("Selected part size in bytes: {0} ({1} megabytes)".format(
		part_size_bytes, part_size))
	click.echo("Number of parts: {0}".format(
		int(math.ceil(archive_size / float(part_size_bytes)))))

	click.echo("The archive will now be uploaded to glacier. Continue? [yn] ", nl=False)
	c = click.getchar()
	click.echo()
	if c.lower() == 'n':
		click.echo("Have a good day! Bye!")
		exit(0)

	upload_archive(archive, archive_size, part_size_bytes, description,
	               event, glacier_client, vault_name, bucket)

	




if __name__ == '__main__':
	main()