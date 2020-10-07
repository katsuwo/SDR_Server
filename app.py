from flask import Flask,jsonify,session
import boto3
from botocore.exceptions import ClientError
import yaml
import os
import json
import uuid
import shutil
from bson import ObjectId
import datetime

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
@app.route('/filelist/<date>', methods=['GET'])
@app.route('/filelist/<date>/<freq>', methods=['GET'])
def get_file_list(date=None, freq=None):
	config = read_configuration_file(CONFIGFILE)
	try:
		check_config(config)
		s3 = setup_S3_client(config)
	except ValueError as e:
		return e.args[0]
	except Exception as e:
		print(e)
		return "something bad."

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
		return e.args[0]

	if 'uuid' not in session:
		session['uuid'] = uuid.uuid1()

	items = []
	if 'Contents' not in bucket:
		return JSONEncoder().encode({'Items': []})
	for content in bucket['Contents']:
		items.append(content['Key'])
	return JSONEncoder().encode({'Items': items})

@app.route('/clear', methods=['GET'])
def clear_tmp_files():
	print(f"uuid is {str(session['uuid'])}")
	if 'uuid' in session:
		temp_dir = os.path.join(TEMPDIR, str(session['uuid']))
		shutil.rmtree(temp_dir)
		print(f"delete {temp_dir}")
		print(f"delete uuid:{str(session['uuid'])} from session dictionary.")
		del session['uuid']
		return "success."
	return "failed"

@app.route('/freqlist/<date>', methods=['GET'])
def get_freq_list(date=None):
	config = read_configuration_file(CONFIGFILE)
	try:
		check_config(config)
		s3 = setup_S3_client(config)
	except ValueError as e:
		return e.args[0]
	except Exception as e:
		print(e)
		return "something bad."

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
		return e.args[0]

	if 'uuid' not in session:
		session['uuid'] = uuid.uuid1()

	return JSONEncoder().encode({'Items': freq_list})


#ex.
#/preparefiles/2010-10-06_23-59
#/preparefiles/2010-10-06_23-59/120_5MHz
#/preparefiles/2010-10-06_23-59?duration=30
#/preparefiles/2010-10-06_23-59/120_5MHz?duration=30
@app.route('/preparefiles/<start_date_time>/', methods=['GET'])
@app.route('/preparefiles/<start_date_time>/<freq>', methods=['GET'])
def prepare_files(start_date_time=None, freq=None):

	dulation = 60

	config = read_configuration_file(CONFIGFILE)
	try:
		check_config(config)
		s3 = setup_S3_client(config)
	except ValueError as e:
		return e.args[0]
	except Exception as e:
		print(e)
		return "something bad."

	if 'uuid'not in session:
		session['uuid'] = uuid.uuid1()
	temp_dir = os.path.join(TEMPDIR, str(session['uuid']))

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
	for delta in range(dulation):
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
		temp_file_list.append(temp_file)
	return JSONEncoder().encode({'Items': temp_file_list})


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
	app.run()
