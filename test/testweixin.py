import requests
import json
# 构建请求数据
data = {
    "msgtype": "text",
    "text": {
        "content": '密码密密麻麻sssbbb'
    }
}

url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=be0e8741-7bf5-4222-a0d1-df88ac7748fb'
# 发送请求
headers = {'Content-Type': 'application/json'}
response = requests.post(url,headers=headers, data=json.dumps(data))

