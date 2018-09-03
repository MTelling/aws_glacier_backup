import click
import os, math
import boto3
from botocore.client import ClientError


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

def verify_all_prereqs(archive, glacier_client, vault_name, bucket, s3):
	exit_if_not_archive_exists(archive)
	exit_if_not_vault_exists(glacier_client, vault_name)
	exit_if_not_bucket_exists(bucket, s3)


def get_size(archive):
	return os.path.getsize(archive)

def prepare_archive_for_multipart_upload(archive, full_size, part_size_bytes):
	return None

def upload_single_archive_to_glacier(archive, description, glacier_client, vault_name):
	click.echo("Archive will be uploaded as single file to glacier.")
	
	return None, "Err"

def upload_multipart_archive_to_glacier(archive, part_size_bytes, description, glacier_client, vault_name):
	click.echo("Archive will be uploaded as single file to glacier.")
	

	return None, "Err"

def create_info_file(archive, archive_size, description, event, glacier_response):
	archive_paths = archive.split("/")
	archive_name = archive_paths[len(archive_paths) - 1]
	info_file_name = archive_name.split(".")[0] + "_info.txt"

	with open(info_file_name, 'w') as f:
		f.write("Archive: {0}\n".format(archive_name))
		f.write("Description: {0}\n".format(description))
		f.write("Archive Size: {0}\n".format(archive_size))
		f.write("Events:\n")
		for e in event:
			f.write("\t- {0}\n".format(e))
		f.write("Upload:\n")
		f.write(glacier_response)

	return info_file_name

def upload_file_to_s3(file, bucket):
	bucket.upload_file(file, file)

def upload_archive(archive, archive_size, part_size_bytes, description,
	               event, glacier_client, vault_name, bucket):
	click.echo()

	if part_size_bytes >= archive_size:
		response, err = upload_single_archive_to_glacier(
			archive, description, glacier_client, vault_name)
	else:
		response, err = upload_multipart_archive_to_glacier(
			archive, part_size_bytes, description, glacier_client, vault_name)

	if err:
		click.secho("Upload aborted!", fg='red')
		click.secho("Error occured uploading file!\n{0}".format(err), fg='red')
		exit(1)

	info_file = create_info_file(archive, archive_size, description, event, response)
	click.echo("Created info file: {0}".format(info_file))
	
	upload_file_to_s3(info_file, bucket)
	click.echo("Successfully uploaded info file to S3.")

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

	verify_all_prereqs(archive, glacier_client, vault_name, bucket, s3)

	archive_size = get_size(archive)
	click.echo("Size of archive: {0} ({1} megabytes)".format(
		archive_size, (archive_size / 1024 / 1024)))

	part_size_bytes = part_size * 1024 * 1024.1
	click.echo("Selected part size in bytes: {0} ({1} megabytes)".format(
		part_size_bytes, part_size))
	click.echo("Number of parts: {0}".format(int(math.ceil(archive_size / part_size_bytes))))

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