from http.server import BaseHTTPRequestHandler, HTTPServer
from time import sleep
import logging
import random
import json


class S(BaseHTTPRequestHandler):
    def _set_response(self, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def response(self, response_dict: dict):
        self.wfile.write(json.dumps(response_dict).encode('utf-8'))

    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        if str(self.path) == '/airflow/base-task':
            work_rand = random.randrange(0, 10)
            success = True if work_rand else False  # 0에서 9까지 나오므로 1/10 확률로 실패
            if success:
                self._set_response(200)
                response = {
                    'status': 'success',
                    'msg': 'hi',
                    'next_task_number': (work_rand % 3) + 1
                }
                self.response(response)

            else:
                # self._set_response(500) # 500으로 해보니까 아예 airflow 내부에서도 실패로 처리
                # self._set_response(404) # 200번대가 아니면 애초에 그냥 False가 됨. airflow가 아니라 airflow에서 사용하고 있는 requests에서 raise함
                self._set_response(404)
                response = {
                    'status': 'failed',
                    'msg': 'bye'
                }
                self.response(response)

        elif str(self.path) == '/airflow/dummy-task1':
            self._set_response(200)
            response = airflow_dummy_task1()
            self.response(response)

        elif str(self.path) == '/airflow/dummy-task2':
            self._set_response(200)
            response = airflow_dummy_task2()
            self.response(response)

        elif str(self.path) == '/airflow/dummy-task3':
            self._set_response(200)
            response = airflow_dummy_task3()
            self.response(response)

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself
        logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
                     str(self.path), str(self.headers), post_data.decode('utf-8'))

        self._set_response()
        self.wfile.write("POST request for {}".format(self.path).encode('utf-8'))


def airflow_dummy_task1():
    count = random.randrange(1, 10)  # 최대 9초가 걸리는 dummy task
    while count:
        count -= 1
        sleep(1)
    return {'msg': 'dummy_task1', 'status': 'success'}


def airflow_dummy_task2():
    count = random.randrange(1, 10)  # 최대 9초가 걸리는 dummy task
    while count:
        count -= 1
        sleep(1)
    return {'msg': 'dummy_task2', 'status': 'success'}


def airflow_dummy_task3():
    count = random.randrange(1, 10)  # 최대 9초가 걸리는 dummy task
    while count:
        count -= 1
        sleep(1)
    return {'msg': 'dummy_task3', 'status': 'success'}


def run(server_class=HTTPServer, handler_class=S, port=8000):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting httpd...\n')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...\n')


if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
