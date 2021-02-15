from flask import Flask, jsonify, session, send_file, make_response, send_from_directory, Response
import boto3
from botocore.exceptions import ClientError
import yaml
import os
import json
import uuid
import shutil
import glob
from bson import ObjectId
import datetime
import subprocess

CONFIGFILE = './config.yaml'
TEMPDIR = '/home/katsuwo/work/SDR_TEMP'

app = Flask(__name__)
app.secret_key = 'secret'

class JSONEncoder(json.JSONEncoder):
	def default(self, o):
		if isinstance(o, ObjectId):
			return str(o)
		return json.JSONEncoder.default(self, o)

# ex
# /filelist/2020-02-10/120_5MHz
@app.route('/filelist/<string:date>', methods=['GET'])
@app.route('/filelist/<string:date>/<string:freq>', methods=['GET'])
def get_file_list(date=None, freq=None):
	config = read_configuration_file(CONFIGFILE)
	try:
		check_config(config)
		s3 = setup_S3_client(config)
	except ValueError as e:
		return e.args[0]
	except Exception as e:
		print(e)
		return Response(response=json.dumps({'message': 'something bad3.'}), status=500)

	try:
		bucket_name = config['S3_STORAGE']['S3_bucket_name']
		prefix = ""
		if date is not None:
			prefix = f"/{date}"
			if freq is not None:
				prefix= f"{prefix}/{freq}"
		if prefix == "":
			bucket = s3.list_objects_v2(Bucket=bucket_name)
		else:
			bucket = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
	except ClientError as e:
		return Response(response=json.dumps({'message': 'something bad4.'}), status=400)

	items = []
	if 'Contents' not in bucket:
		return JSONEncoder().encode({'Items': []})
	for content in bucket['Contents']:
		items.append(content['Key'])
	return JSONEncoder().encode({'Items': items})


@app.route('/clear', methods=['GET'])
@app.route('/clear/<string:uuid_>', methods=['GET'])
def clear_tmp_files(uuid_=None):
	if uuid_ is not None:
		temp_dir = os.path.join(TEMPDIR, uuid_)
		shutil.rmtree(temp_dir)
		print(f"delete {temp_dir}")
	else:
		for temp_dir in glob.glob(TEMPDIR + "/*"):
			shutil.rmtree(temp_dir)
			print(f"delete {temp_dir}")
	return Response(response=json.dumps({'message': 'success'}), status=200)


@app.route('/freqlist/<string:date>', methods=['GET'])
def get_freq_list(date=None):
	config = read_configuration_file(CONFIGFILE)
	try:
		check_config(config)
		s3 = setup_S3_client(config)
	except ValueError as e:
		return Response(response=json.dumps({'message': 'something bad1.'}), status=400)
	except Exception as e:
		print(e)
		return Response(response=json.dumps({'message': 'something bad2.'}), status=400)

	try:
		bucket_name = config['S3_STORAGE']['S3_bucket_name']
		if date is not None:
			bucket = s3.list_objects_v2(Bucket=bucket_name, Prefix=date)
			temp_list = []

			if 'Contents' not in bucket:
				return JSONEncoder().encode({'Items': []})
			for content in bucket['Contents']:
				print(content['Key'].split("/")[1])
				temp_list.append(content['Key'].split("/")[1])
			freq_list = list(set(temp_list))
	except ClientError as e:
		return Response(response=json.dumps({'message': 'something bad8.'}), status=400)

	return JSONEncoder().encode({'Items': freq_list})


# ex.
# /preparefiles/2010-10-06_23-59/60
# /preparefiles/2010-10-06_23-59/60/120_5MHz
@app.route('/preparefiles/<string:start_date_time>/<int:duration>', methods=['GET'])
@app.route('/preparefiles/<string:start_date_time>/<int:duration>/<string:freq>', methods=['GET'])
def prepare_files(start_date_time=None, duration=60, freq=None):

	config = read_configuration_file(CONFIGFILE)
	try:
		check_config(config)
		s3 = setup_S3_client(config)
	except ValueError as e:
		return e.args[0]
	except Exception as e:
		print(e)
		return "something bad6."

	uuid_ = str(uuid.uuid1())
	temp_dir = os.path.join(TEMPDIR, uuid_)

	if os.path.exists(temp_dir):
		shutil.rmtree(temp_dir)
	os.mkdir(temp_dir)

	temp_datetime = datetime.datetime.strptime(start_date_time , '%Y-%m-%d_%H-%M')
	start_date_string = temp_datetime.strftime('%Y-%m-%d')

	try:
		bucket_name = config['S3_STORAGE']['S3_bucket_name']
		prefix = f"/{start_date_string}"
		if freq is not None:
			prefix = f"{prefix}/{freq}"
		bucket = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
	except ClientError as e:
		return e.args[0]

	file_list = []
	if 'Contents' not in bucket:
		return JSONEncoder().encode({'Items': []})

	# make file list
	for delta in range(duration):
		delta = datetime.timedelta(minutes=delta)
		find_time = temp_datetime + delta
		find_datetime_string = datetime.datetime.strftime(find_time, '%Y_%m_%d__%H_%M_%S')

		for content in bucket['Contents']:
			if freq is not None:
				if freq in content['Key'] and find_datetime_string in content['Key']:
					file_list.append(content['Key'])
			else:
				if find_datetime_string in content['Key']:
					file_list.append(content['Key'])

	# download files
	temp_file_list = []
	for file in file_list:
		print(f"Downloading.{file}")
		temp_file = os.path.join(temp_dir, os.path.basename(file))
		s3.download_file(bucket_name, file, temp_file)
		temp_file_list.append(os.path.basename(file))
	return JSONEncoder().encode({'Items': temp_file_list, "uuid": uuid_})


@app.route('/getaudiofile/<uuid_>/<string:filename>', methods=['GET'])
def get_file(uuid_, filename):
	temp_dir = os.path.join(TEMPDIR, uuid_)
	full_path = os.path.join(temp_dir, filename)

	if os.path.exists(full_path.replace(".wav", ".ogg")):
		if filename.split('.')[1] == "wav":
			# ogg => wav
			cmdline = f"oggdec -Q -o {full_path} {full_path.replace('.wav', '.ogg')}".split(" ")
			subprocess.run(cmdline)
			return send_file(full_path, as_attachment=True, attachment_filename=filename, mimetype="audio/wav")
		return send_file(full_path, as_attachment=True, attachment_filename=filename, mimetype="audio/ogg")
	return "something bad7."

@app.route('/')
def hello_world():
	return 'Hello World!'


def read_configuration_file(file_name):
	with open(file_name, 'r') as yml:
		config = yaml.safe_load(yml)
	return config


def check_config(config):
	if 'S3_STORAGE' not in config:
		raise ValueError("S3_STORAGE Section not exists in config file.")
	if 'S3_access_key_id' not in config['S3_STORAGE']:
		raise ValueError("S3_access_key_id element not exists in S3_STORAGE section.")
	if 'S3_secret_access_key' not in config['S3_STORAGE']:
		raise ValueError("S3_secret_access_key element not exists in S3_STORAGE section.")
	if 'S3_bucket_name' not in config['S3_STORAGE']:
		raise ValueError("S3_bucket_name element not exists in S3_STORAGE section.")


def setup_S3_client(config):
	s3_endpoint_erl = config['S3_STORAGE']['S3_endpoint_url']
	os.environ['AWS_ACCESS_KEY_ID'] = config['S3_STORAGE']['S3_access_key_id']
	os.environ['AWS_SECRET_ACCESS_KEY'] = config['S3_STORAGE']['S3_secret_access_key']
	return boto3.client('s3', endpoint_url=s3_endpoint_erl, verify=False)


if __name__ == '__main__':
	app.run(host="192.168.10.51", port=5000)
