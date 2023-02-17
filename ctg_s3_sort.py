import s3fs
import requests

API_URL = 'https://dev-api.clue.io/api/'
API_KEY = '1ce653c27e4ae77b060c0364be79db0b'
screens_url = API_URL + 'prism_screens/'
map_src_url = API_URL + 'v_ctg_build/'
fs = s3fs.S3FileSystem()
dir_path = 's3://ctg.clue.io/'
raw_path = 's3://data.clue.io/enspire/'

def get_data_from_db(endpoint_url, user_key, where=None, fields=None):
    request_url = make_request_url_filter(endpoint_url, where=where, fields=fields)
    response = requests.get(request_url, headers={'user_key': user_key})
    if response.ok:
        return response.json()
    else:
        response.raise_for_status()

def make_request_url_filter(endpoint_url, where=None, fields=None):
    clauses = []
    if where:
        where_clause = '"where":{'
        wheres = []
        for k, v in where.items():
            wheres.append('"{k}":"{v}"'.format(k=k, v=v))
        where_clause += ','.join(wheres) + '}'
        clauses.append(where_clause)

    if fields:
        fields_clause = '"fields":{'
        fields_list = []
        if type(fields) == dict:
            for k, v in fields.items():
                fields_list.append('"{k}":"{v}"'.format(k=k, v=v))
        elif type(fields) == list:
            for field in fields:
                fields_list.append('"{k}":"{v}"'.format(k=field, v="true"))
        fields_clause += ','.join(fields_list) + '}'
        clauses.append(fields_clause)

    if len(clauses) > 0:
        # print(endpoint_url.rstrip("/") + '?filter={' +  ','.join(clauses) + '}')
        return endpoint_url.rstrip("/") + '?filter={' + requests.utils.quote(','.join(clauses)) + '}'
    else:
        return endpoint_url

screens = get_data_from_db(
    endpoint_url=screens_url,
    user_key=API_KEY,
    fields=['name']
)

screens_list = []
for i in screens:
    screens_list.append(list(i.values())[0])

print('Available screens:')
print(screens_list)

for screen in screens_list:
    plates_list = []
    plates = get_data_from_db(
        endpoint_url=map_src_url,
        user_key=API_KEY,
        fields=['assay_plate_barcode'],
        where={'screen': screen}
    )
    for plate in plates:
        plates_list.append(list(plate.values())[0])
    plates_list_dedup = list(set(plates_list))

    for plate in plates_list_dedup:
        print('Looking for plate ' + plate + ' from screen ' + screen + '....')
        if fs.glob(raw_path + plate + '*'):
            if fs.glob(dir_path + screen + '/' + plate + '*'):
                print(plate + ' already exists')
            else:
                cp_plate = fs.glob(raw_path + plate + '*')[0]
                print('Found plate ' + plate)
                print('Copying to ' + dir_path + screen + '/' + plate + '.csv')
                fs.cp(cp_plate, dir_path + screen + '/' + plate + '.csv')
        else:
            print(screen + ': ' + plate + ' file not found.')