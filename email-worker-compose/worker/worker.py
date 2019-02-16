import redis
import json
import os
from time import sleep
from random import randint


## para escalar: docker-compose up -d --scale worker=3

if __name__ == '__main__':
    redis_host = os.getenv('REDIS_HOST', 'queue')
    r = redis.Redis(host=redis_host, port=6379, db=0)
    print('\n')
    print('Aguardando mensagens ...')
    print('\n')
    while True:
        mensagem = json.loads(r.blpop('sender')[1])
        # Simulando envio de e-mail...
        print('\n')
        print('Mandando a mensagem:', mensagem['assunto'])
        sleep(randint(15, 45))
        print('\n')
        print('Mensagem', mensagem['assunto'], 'enviada')
        print('\n')