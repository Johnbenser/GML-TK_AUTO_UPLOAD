import requests

headers = {
    "Authorization": "Bearer ak_2qp2h31Vm2LB3AX3XH5Hb3Ve9ZJ1Y",
    "Content-Type": "application/json"
}

data = {
    "model": "LongCat-Flash-Lite",
    "messages": [
        {"role": "user", "content": "Say hi"}
    ]
}

url = "https://api.longcat.chat/openai/v1/chat/completions"

try:
    response = requests.post(url, headers=headers, json=data, timeout=120)
    res_json = response.json()
    print("Keys:", res_json.keys())
    if 'choices' in res_json:
        print("Content exists:", 'content' in res_json['choices'][0]['message'])
        print("Content length:", len(res_json['choices'][0]['message']['content']))
except Exception as e:
    print(f"Error: {e}")
