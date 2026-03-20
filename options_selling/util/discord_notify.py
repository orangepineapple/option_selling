import requests

def send_notif(message):
    
    data = {
       "message" : message
    }

    response = requests.post('https://n8n.n8ndomain.uk/webhook/3e2f2286-161f-4d42-b2e5-ece3920f10b5', json=data)
    if response.status_code == 200:
        return True
    else:
        return False
