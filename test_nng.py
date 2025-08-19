import pynng
import json
address = "ipc:///tmp/hwdiscovery.ipc"
rep = pynng.Rep0(listen=address, recv_timeout=15000, send_timeout=15000 )
while True:
    try:
        print('Waiting for request...')  # waits for a request from the engine
        question = rep.recv()
        print(question)  # prints b'4'
        answer = {'action': 'hw_discovery', 'value': 'OK'}  # guaranteed to be random
        print(f'now sending: {answer}')
        rep.send(json.dumps(answer).encode())
        print('sent')
    except pynng.exceptions.Timeout:
        pass

