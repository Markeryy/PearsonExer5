import sys
import random
import socket
import json
import time
from multiprocessing import Process, Queue
import subprocess


ENCODING_FORMAT = "utf-8"
DISCONNECT_MESSAGE = "!DISCONNECT"
HEADER_SIZE = 2048
t = 2

if len(sys.argv) < 4:
  print("Usage: python exer5core.py n p s")
  sys.exit(1)

try:
  n = int(sys.argv[1])  
  p = int(sys.argv[2])
  s = int(sys.argv[3])
except:
  print("Invalid argument")

with open("ip.in", "r") as file:
  conf = file.readlines()
  IP_ADDRESS_MASTER = conf[0].strip()

  print("\n[CONFIGURATIONS]")
  print(f"{n} x {n} matrix")
  print(f"master IP = {IP_ADDRESS_MASTER}")
  print("\n")

def pearson_cor(x_matrix, y, m, n):
  print("Running PEARSON")
  results = []
  # iterate x_matrix columns
  for i in range(n):
    x = []
    # print(f"m={m}, n={n}")

    if (n>1):
      for j in range(n):
        x.append(x_matrix[i][j])
    else:
      x.append(x_matrix[i])

    sum_x = sum(x)  # num
    sum_y = sum(y)  # num

    x_squared = [num**2 for num in x]  # list
    y_squared = [num**2 for num in y]  # list

    x_squared_sum = sum(x_squared)  # num
    y_squared_sum = sum(y_squared)  # num

    xy = []
    i = 0
    for i in range(n):
      xy.append(x[i] * y[i])

    sum_xy = sum(xy)

    numerator = (n * sum_xy) - (sum_x * sum_y)
    denominator = (((n * x_squared_sum) - (sum_x ** 2)) * ((n * y_squared_sum) - (sum_y ** 2))) ** 0.5
    # print(f"denominator {denominator}")

    try:
      r = numerator/denominator
      results.append(r)
    except:
      results.append(0)

  return results

def send_submatrix(submatrix, port, vector_y, result_queue):
  server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  server.bind((IP_ADDRESS_MASTER, port))
  server.listen(1)

  print(f"[SERVER] Waiting for connection on port {port}")
  # subprocess.run(["start", "cmd", "/k", f"python exer5core.py 5 {port} 1"], shell=True)
  conn, addr = server.accept()
  # Run the command/script
  print(f"[SERVER] Connected to {addr}")
  print(f"[SERVER] Sending submatrix")

  # 1. send submatrix
  serialized_submatrix = json.dumps(submatrix)
  conn.send(f"{len(serialized_submatrix):<{HEADER_SIZE}}".encode(ENCODING_FORMAT))
  conn.send(serialized_submatrix.encode(ENCODING_FORMAT))

  # 2. receive ack from client
  response = conn.recv(HEADER_SIZE).decode(ENCODING_FORMAT)

  # 3. send vector y
  serialized_vector_y = json.dumps(vector_y)
  conn.send(f"{len(serialized_vector_y):<{HEADER_SIZE}}".encode(ENCODING_FORMAT))
  conn.send(serialized_vector_y.encode(ENCODING_FORMAT))

  # 4. receive ack from client
  response = conn.recv(HEADER_SIZE).decode(ENCODING_FORMAT)
  print(response)

  # 5. receive pearson result from client
  msg_length = int(conn.recv(HEADER_SIZE).decode(ENCODING_FORMAT))
  serialized_results = conn.recv(msg_length).decode(ENCODING_FORMAT)
  results = json.loads(serialized_results)

  # save all results in the queue
  result_queue.put(results)

  conn.close()
  server.close()

def master_process(n, t, IP_ADDRESS_MASTER):
  start_port = 5001
  with open("slaves.in", "w") as file:
    for _ in range(n):
      file.write(f"{start_port}\n")
      start_port += 1

  print(f"[GENERATING MATRIX]")
  matrix_n = []
  for i in range(n):
    matrix_n.append([])
    for j in range(n):
      matrix_n[i].append(random.randint(1, 100))
  print(f"[MATRIX CREATED!]")

  print(f"[GENERATING VECTOR Y]")
  vector_y = []
  for i in range(n):
    vector_y.append(random.randint(1, 100))
  print(f"[VECTOR Y CREATED]")

  column_per_submatrix = n // t

  print(f"[DIVIDING MATRIX]")
  submatrices = []
  start_row = 0
  for i in range(t):
    end_row = start_row + column_per_submatrix
    if i == t - 1:
      end_row = n
    submatrices.append(matrix_n[start_row:end_row])
    start_row = end_row

  print("[SETTING UP SERVER]")

  processes = []
  result_queue = Queue()
  result = []
  for i in range(t):
    process = Process(target=send_submatrix, args=(submatrices[i], 5001 + i, vector_y, result_queue))
    processes.append(process)

  print("[RUNNING ALL PROCESSES]")
  server_time_before = time.time()

  for process in processes:
    process.start()

  for process in processes:
    process.join()

  while not result_queue.empty():
    current_result = result_queue.get()
    result.extend(current_result)
    print(f"[SERVER] Server received result from client")
  
  print("[SERVER] All results received")
  #print(result)

  server_time_after = time.time()
  time_elapsed = server_time_after - server_time_before
  time_elapsed = round(time_elapsed, 4)

  print(f"[SERVER TIME] {time_elapsed}")

  with open("server.txt", "w") as file:
    file.write(f"{time_elapsed}\n")

def slave_process(p, IP_ADDRESS_MASTER,):
  client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  client.connect((IP_ADDRESS_MASTER, p))

  # 1. load submatrix
  msg_length = int(client.recv(HEADER_SIZE).decode(ENCODING_FORMAT))
  serialized_submatrix = client.recv(msg_length).decode(ENCODING_FORMAT)
  submatrix = json.loads(serialized_submatrix)
  # print(f"[CLIENT] Submatrix received {submatrix}")
  print(f"[CLIENT] Submatrix received")

  # 2. send ack to server
  client.send("[CLIENT ACKNOWLEDGEMENT] Submatrix received".encode(ENCODING_FORMAT))

  # 3. receive vector y
  msg_length = int(client.recv(HEADER_SIZE).decode(ENCODING_FORMAT))
  serialized_vector_y = client.recv(msg_length).decode(ENCODING_FORMAT)
  vector_y = json.loads(serialized_vector_y)
  # print(f"[CLIENT] Vector y received {vector_y}")
  print(f"[CLIENT] Vector y received")

  # 4. send ack to server
  client.send("[CLIENT ACKNOWLEDGEMENT] Vector y received".encode(ENCODING_FORMAT))

  time_before = time.time()

  # get pearson
  results = pearson_cor(submatrix, vector_y, n, n//t)
  # print(f"[PEARSON RESULT] {results}")

  time_after = time.time()
  time_elapsed = time_after - time_before
  time_elapsed = round(time_elapsed, 4)

  # prepare for sending to server
  for i in range(len(results)):
    results[i] = results[i].real

  # 5. send results back to server
  serialized_results = json.dumps(results)
  client.send(f"{len(serialized_results):<{HEADER_SIZE}}".encode(ENCODING_FORMAT))
  client.send(serialized_results.encode(ENCODING_FORMAT))

  print(f"[CLIENT TIME] {time_elapsed}")

  client.close()

if __name__ == '__main__':
  if s == 0:
    master_process(n, t, IP_ADDRESS_MASTER)
  elif s == 1:
    slave_process(p, IP_ADDRESS_MASTER)
