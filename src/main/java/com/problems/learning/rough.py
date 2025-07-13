main.py
import logging
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from decimal import Decimal
import json
import base64

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
templates = Jinja2Templates(directory="templates")

dynamodb = boto3.resource("dynamodb")
ALLOWED_TABLES = ['dynamo-accounts']

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)

async def get_table_schema(table_name):
    client = dynamodb.meta.client
    desc = client.describe_table(TableName=table_name)['Table']
    # key schema order: HASH then optional RANGE
    key_schema = desc['KeySchema']
    attr_defs = {ad['AttributeName']: ad['AttributeType'] for ad in desc['AttributeDefinitions']}
    table_keys = {
        'partitionKey': next(ks['AttributeName'] for ks in key_schema if ks['KeyType'] == 'HASH'),
        'sortKey': next((ks['AttributeName'] for ks in key_schema if ks['KeyType'] == 'RANGE'), None),
        'attrTypes': attr_defs
    }
    # indexes
    gsis = [idx['IndexName'] for idx in desc.get('GlobalSecondaryIndexes', [])]
    lsis = [idx['IndexName'] for idx in desc.get('LocalSecondaryIndexes', [])]
    index_map = {}
    for idx in desc.get('GlobalSecondaryIndexes', []) + desc.get('LocalSecondaryIndexes', []):
        index_name = idx['IndexName']
        idx_pk = next(ks['AttributeName'] for ks in idx['KeySchema'] if ks['KeyType'] == 'HASH')
        idx_sk = next((ks['AttributeName'] for ks in idx['KeySchema'] if ks['KeyType'] == 'RANGE'), None)
        index_map[index_name] = {'partitionKey': idx_pk}
        if idx_sk:
            index_map[index_name]['sortKey'] = idx_sk
    table_keys['indexMap'] = index_map
    return table_keys, ['no-index-query'] + gsis + lsis

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "tables": ALLOWED_TABLES})

@app.post("/get_indexes")
async def get_indexes(request: Request):
    body = await request.json()
    table = body.get('table')
    table_keys, indexes = await get_table_schema(table)
    return {"tableKeys": table_keys, "indexes": indexes}

@app.post("/query")
async def query_table(request: Request):
    data = await request.json()
    table_name = data.get("table")
    index = data.get("index")
    pk = data.get("partitionKey")
    pk_value = data.get("partitionValue")
    sk = data.get("sortKey")
    sk_value = data.get("sortValue")

    logger.info(f"Received query for table '{table_name}', index '{index}'")
    logger.info(f"Partition key: {pk}, Value: {pk_value}, Sort key: {sk}, Value: {sk_value}")

    if index and index != 'no-index-query' and not pk_value:
        logger.error(f"Missing required partition key value for index '{index}' on key '{pk}'")
        return JSONResponse(content={"error": f"Missing required partition key value for index '{index}' on key '{pk}'"}, status_code=400)

    table = dynamodb.Table(table_name)
    # retrieve types
    table_keys, _ = await get_table_schema(table_name)
    types = table_keys['attrTypes']

    def convert_value(value, atype):
        try:
            if atype == 'N':
                return Decimal(str(value))
            if atype == 'B':
                return base64.b64decode(value)
            return value  # treat as String
        except Exception as e:
            logger.error(f"Error converting value '{value}' for type '{atype}'", exc_info=True)
            raise

    try:
        expr = None
        if pk and pk_value is not None:
            expr = Key(pk).eq(convert_value(pk_value, types.get(pk)))
            if sk and sk_value is not None:
                expr = expr & Key(sk).eq(convert_value(sk_value, types.get(sk)))

        if expr:
            if index and index != 'no-index-query':
                response = table.query(IndexName=index, KeyConditionExpression=expr)
            else:
                response = table.query(KeyConditionExpression=expr)
        else:
            response = table.scan()

        items = response.get("Items", [])
        return JSONResponse(content=json.loads(json.dumps(items, cls=DecimalEncoder)))

    except ClientError as e:
        logger.error("DynamoDB ClientError during query", exc_info=True)
        return JSONResponse(content={"error": e.response['Error']['Message']}, status_code=400)
    except Exception as e:
        logger.error("Unhandled Exception during query", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=400)

@app.post("/update_item")
async def update_item(request: Request):
    data = await request.json()
    table_name = data.get("table")
    item = data.get("item")

    table = dynamodb.Table(table_name)
    def convert_types(obj):
        if isinstance(obj, float):
            return Decimal(str(obj))
        if isinstance(obj, dict):
            return {k: convert_types(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [convert_types(v) for v in obj]
        return obj

    try:
        item = convert_types(item)
        table.put_item(Item=item)
        return {"status": "success"}
    except ClientError as e:
        logger.error("DynamoDB ClientError during update_item", exc_info=True)
        return JSONResponse(content={"error": e.response['Error']['Message']}, status_code=400)
    except Exception as e:
        logger.error("Unhandled Exception during update_item", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=400)





index.html
<!DOCTYPE html>
<html>
<head>
    <title>DynamoDB Editor</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f9f9f9;
        }

        label {
            font-weight: bold;
            margin-right: 10px;
        }

        select, input {
            padding: 5px;
            margin-bottom: 10px;
            margin-right: 20px;
            border: 1px solid #ccc;
            border-radius: 4px;
        }

        button {
            padding: 7px 15px;
            background-color: #007BFF;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }

        button:hover {
            background-color: #0056b3;
        }

        textarea {
            font-family: monospace;
            background-color: #fff;
            border: 1px solid #ccc;
            padding: 10px;
            width: 95%;
            border-radius: 4px;
        }

        #results {
            margin-top: 30px;
        }

        hr {
            border-top: 1px solid #ccc;
            margin: 20px 0;
        }
    </style>
</head>
<body>
<h2>DynamoDB Table Viewer</h2>
<label>Select Table:</label>
<select id="tableSelect" onchange="onTableChange()">
    <option value="">--Select--</option>
    {% for t in tables %}<option value="{{ t }}">{{ t }}</option>{% endfor %}
</select>
<br><br>
<label>Select Mode:</label>
<select id="indexSelect" onchange="onIndexChange()"></select>
<div id="keyInputs"></div>
<button onclick="query()">Query</button>
<div id="results"></div>
<script>
    let tableKeys = {};
    async function onTableChange() {
        const table = document.getElementById('tableSelect').value;
        if (!table) return;
        const res = await fetch('/get_indexes', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({table})});
        const data = await res.json(); tableKeys = data.tableKeys;
        const idx = document.getElementById('indexSelect'); idx.innerHTML = '';
        data.indexes.forEach(i=>idx.innerHTML+=`<option value="${i}">${i}</option>`);
        onIndexChange();
    }
    function onIndexChange() {
        const mode = document.getElementById('indexSelect').value;
        const div = document.getElementById('keyInputs');
        div.innerHTML = '';

        // Debug logs for index mode and tableKeys
        console.log("Selected index mode:", mode);
        console.log("tableKeys object:", tableKeys);

        if (mode === 'no-index-query') {
            div.innerHTML += `<label>${tableKeys.partitionKey}:</label><input id="pkVal" />`;
            if (tableKeys.sortKey) {
                div.innerHTML += `<label>${tableKeys.sortKey}:</label><input id="skVal" />`;
            }
        } else {
            if (tableKeys.indexMap) {
                const indexKey = tableKeys.indexMap[mode];
                if (indexKey) {
                    console.log("Index key found:", indexKey);
                    div.innerHTML += `<label>${indexKey.partitionKey}:</label><input id="pkVal" />`;
                    if (indexKey.sortKey) {
                        div.innerHTML += `<label>${indexKey.sortKey}:</label><input id="skVal" />`;
                    }
                } else {
                    console.error("No indexKey found for mode:", mode);
                    // Additional fallback debug log
                    console.error("Current tableKeys.indexMap:", tableKeys.indexMap);
                }
            } else {
                console.error("tableKeys.indexMap is undefined");
            }
        }
    }
    async function query() {
        const table = document.getElementById('tableSelect').value;
        const mode = document.getElementById('indexSelect').value;

        const pkInput = document.getElementById('pkVal');
        if (!pkInput || !pkInput.value.trim()) {
            pkInput.style.border = "2px solid red";
            alert("Partition key value is required.");
            return;
        } else {
            pkInput.style.border = "";
        }

        const skInput = document.getElementById('skVal');
        const pkVal = pkInput.value.trim();
        const skVal = skInput?.value.trim();

        let pk, sk;
        if (mode === 'no-index-query') {
            pk = tableKeys.partitionKey;
            sk = tableKeys.sortKey;
        } else {
            pk = tableKeys.indexMap?.[mode]?.partitionKey;
            sk = tableKeys.indexMap?.[mode]?.sortKey;
        }

        const body = {table, index: mode, partitionKey: pk, partitionValue: pkVal};
        if (sk && skVal) Object.assign(body, {sortKey: sk, sortValue: skVal});

        const res = await fetch('/query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });

        const out = document.getElementById('results');
        out.innerHTML = '<h3>Results</h3>';

        if (!res.ok) {
            const errorText = await res.text();
            out.innerHTML += `<pre style="color: red;">Error: ${res.status} ${errorText}</pre>`;
            return;
        }

        const items = await res.json();
        if (!Array.isArray(items)) {
            out.innerHTML += `<pre style="color: red;">Error: Expected array but got ${typeof items}</pre>`;
            console.error("Unexpected response:", items);
            return;
        }

        items.forEach((it, i) => {
            out.innerHTML += `
                <textarea id="edit_${i}" rows="10" cols="80" data-original='${JSON.stringify(it)}'>${JSON.stringify(it, null, 2)}</textarea>
                <br>
                <button onclick="updateItem('${table}', ${i})">Save</button>
                <hr>`;
        });
    }

    async function updateItem(table, i) {
        const textarea = document.getElementById(`edit_${i}`);
        const newItem = JSON.parse(textarea.value);
        const original = JSON.parse(textarea.dataset.original);

        let changes = [];
        for (const key in newItem) {
            if (JSON.stringify(newItem[key]) !== JSON.stringify(original[key])) {
                changes.push(`<li><b>${key}</b>: <span style="color:red;">${JSON.stringify(original[key])} ➝ ${JSON.stringify(newItem[key])}</span></li>`);
            }
        }

        if (changes.length === 0) {
            alert("No changes detected.");
            return;
        }

        const confirmHtml = `
            <p>The following changes were detected:</p>
            <ul>${changes.join('')}</ul>
            <p>Do you want to save these changes?</p>
        `;

        const confirmDiv = document.createElement('div');
        confirmDiv.innerHTML = confirmHtml;
        confirmDiv.style.background = "#fff";
        confirmDiv.style.border = "1px solid #ccc";
        confirmDiv.style.padding = "20px";
        confirmDiv.style.position = "fixed";
        confirmDiv.style.top = "50%";
        confirmDiv.style.left = "50%";
        confirmDiv.style.transform = "translate(-50%, -50%)";
        confirmDiv.style.zIndex = 9999;
        confirmDiv.style.boxShadow = "0 0 10px rgba(0,0,0,0.2)";
        confirmDiv.innerHTML += `
            <button onclick="this.parentElement.remove(); submitUpdate('${table}', ${i})">Confirm</button>
            <button onclick="this.parentElement.remove()">Cancel</button>
        `;
        document.body.appendChild(confirmDiv);
    }

    async function submitUpdate(table, i) {
        const textarea = document.getElementById(`edit_${i}`);
        const item = JSON.parse(textarea.value);
        const res = await fetch('/update_item', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({table, item})
        });
        const r = await res.json();
        alert(r.status === 'success' ? 'Updated!' : 'Error: ' + r.error);
    }
</script>
</body>
</html>
