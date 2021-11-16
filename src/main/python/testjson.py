import json

from pyspark.sql.types import StructField, StructType

with open(f'../../../configuration/eng/schema/test.json') as f:
    table_meta = json.load(f)['columns']

schema = StructType.fromJson(table_meta)

print(schema)


# StructFields = []
# for cols in table_meta['columns']:
#     temp = []
#     name = cols['name']
#     temp.append(name)
#     type = cols['type']+"()"
#     temp.append(type)
#     nullable = cols['nullable']
#     temp.append(nullable)
#     StructFields.append(temp)

# print(StructFields[0])
# fields = StructField('{}','{}()','{}'.format(StructFields[0][0],StructFields[0][1], StructFields[0][2]))
# print(fields)
