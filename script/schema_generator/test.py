import json
import datetime


table_data = '''
{    
    "__meta__": {
        "tableName": "Test",
        "timeOfSchemaCreation": "%s",
        "typesOfChecks": ["count_check", "data_check"]
    },

    "columns": {
        "fields": [
            {
                "metadata": {},
                "ordinal": 1,
                "name": "name",
                "id_flag": 1,
                "type": "string",
                "nullable": false
            },
            {
                "metadata": {},
                "ordinal": 2,
                "name": "salary",
                "id_flag": 0,
                "type": "long",
                "nullable": false
            },
            {
                "metadata": {},
                "ordinal": 3,
                "name": "dept",
                "id_flag": 0,
                "type": "string",
                "nullable": false
            }
        ]
    }
}
''' % datetime.datetime.now()


data = json.loads(table_data)

with open("../../configuration/eng/schema/test.json", "w") as f:
    json.dump(data, f, indent=2)