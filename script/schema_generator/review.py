import json
import datetime


table_data = '''
{    
    "__meta__": {
        "tableName": "Review",
        "timeOfSchemaCreation": "%s",
        "typesOfChecks": ["count_check", "data_check"]
    },

    "columns": {
        "fields": [
            {
                "metadata": {},
                "ordinal": 1,
                "name": "business_id",
                "id_flag": 1,
                "type": "string",
                "nullable": false
            },
            {
                "metadata": {},
                "ordinal": 2,
                "name": "cool",
                "id_flag": 0,
                "type": "long",
                "nullable": true
            },
            {
                "metadata": {},
                "ordinal": 3,
                "name": "date",
                "id_flag": 0,
                "type": "timestamp",
                "nullable": false
            },
            {
                "metadata": {},
                "ordinal": 4,
                "name": "funny",
                "id_flag": 0,
                "type": "long",
                "nullable": true
            },
            {
                "metadata": {},
                "ordinal": 5,
                "name": "review_id",
                "id_flag": 1,
                "type": "string",
                "nullable": false
            },
            {
                "metadata": {},
                "ordinal": 6,
                "name": "stars",
                "id_flag": 0,
                "type": "double",
                "nullable": false
            },
            {
                "metadata": {},
                "ordinal": 7,
                "name": "text",
                "id_flag": 0,
                "type": "string",
                "nullable": true
            },
            {
                "metadata": {},
                "ordinal": 8,
                "name": "useful",
                "id_flag": 0,
                "type": "long",
                "nullable": true
            },
            {
                "metadata": {},
                "ordinal": 9,
                "name": "user_id",
                "id_flag": 1,
                "type": "string",
                "nullable": false
            }
        ]
    }
}
''' % datetime.datetime.now()


data = json.loads(table_data)

with open("../../configuration/eng/schema/review.json", "w") as f:
    json.dump(data, f, indent=2)