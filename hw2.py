import boto3
import csv

s3 = boto3.resource('s3', aws_access_key_id='AKIASKONREDYFY5F4645', aws_secret_access_key='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')
b_name = 'adacav4-datacont-cs1660-pgh'

try:
	s3.create_bucket(Bucket=b_name, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
	
	# Make bucket publicly readable
	bucket = s3.Bucket(b_name)
	bucket.Acl().put(ACL='public-read')
except:
	print "Bucket " + b_name + " already exists"

# Upload "test.jpg" to the bucket
s3.Object(b_name, 'test.jpg').put(Body=open('/Users/Adarsh/Desktop/test.jpg', 'rb'))

# Create DynamoDB table
dyndb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='AKIASKONREDYFY5F4645', aws_secret_access_key='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')

try:	# If table has not been defined yet, use:
	table = dyndb.create_table(
		TableName='DataTable',
		KeySchema=[
		{'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
		{'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
		],
		AttributeDefinitions=[
		{'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
		{'AttributeName': 'RowKey', 'AttributeType': 'S'}],
		ProvisionedThroughput={
		'ReadCapacityUnits': 5,
		'WriteCapacityUnits': 5})

except:  # If table was previously defined, use:
	print("Table already exists" + "\n" )
	table = dyndb.Table("DataTable")

# Wait for table to be created
print("Wating for table to be created...")
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print("Table creation confirmed!" + "\n")

# Read and upload the csv file to the bucket
with open('/Users/Adarsh/Desktop/master_data.csv', 'rb') as csvfile:
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	for item in csvf:
		print("\n")
		print(item)
		
		body = open(item[5], 'rb')
		s3.Object(b_name, item[2]).put(Body=body)
		md = s3.Object(b_name, item[2]).Acl().put(ACL='public-read')
		
		url = " https://s3-us-west-2.amazonaws.com/'datacont-adacav4'/" + item[2]
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'date': item[3], 'url': url}
		
		try:
			table.put_item(Item=metadata_item)
		except:
			print "Item is already in the table"

# Query an entry from the table and print it and the response
same_table = dyndb.Table("DataTable")
response = same_table.get_item(Key={'PartitionKey': 'experiment1', 'RowKey': '1'})
item = response['Item']
print("\n")
print(item)
print("\n")
print(response)