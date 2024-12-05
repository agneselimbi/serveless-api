import boto3
import json 
from decimal import Decimal
def create_db(resourcedb,clientdb):
    '''Creates a dynamodb table called Movies'''
    try:
        table = resourcedb.create_table(
                    TableName="Movies",
                    KeySchema=[
                        {
                            'AttributeName':'title' ,
                            'KeyType': 'HASH',
                        },
                        {
                            'AttributeName': 'releaseYear',
                            'KeyType':'RANGE',
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'title',
                            'AttributeType': 'S'
                            ,
                        },
                        {
                            'AttributeName': 'releaseYear',
                            'AttributeType':'N',
                        }                  
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
        waiter = clientdb.get_waiter('table_exists')
        waiter.wait(TableName="Movies")  

    except Exception as e:
        print(e)
    
def put_items_in_table(resourcedb,table,jsonfile):
    """Populates the table Movies with items"""
    moviestable=resourcedb.Table(table)
    with open(jsonfile,'r') as f:
        items = json.load(f,parse_float=Decimal)
    for item in items:
        title = item['title']
        releaseYear=item['releaseYear']
        imdbRatings=item['imdbRatings']
        director=item['director']
        genre=item['genre']
        print(title)
        moviestable.put_item(
            TableName=table,
            Item={
                    "title":title,
                    "releaseYear":releaseYear,
                    "imdbRatings":imdbRatings,
                    "director":director,
                    "genre":genre
                }
                    )

if __name__ =="__main__":
    boto3.setup_default_session(profile_name='serverless')
    db = boto3.resource('dynamodb')
    clientdb=boto3.client('dynamodb')
    table="Movies"
    jsonfile=r'movies.json'
    create_db(db,clientdb)  
    put_items_in_table(db,table,jsonfile)

    