import socket
import json
from cryptography.fernet import Fernet
from confluent_kafka import Producer, Consumer, KafkaError
import logging
import networkx as nx
import random 
import pandas as pd
from ucimlrepo import fetch_ucirepo
import yaml
import time
class FCLoader:
    def __init__(self, dataset_id, fraction):
        self.dataset_id = dataset_id
        self.fraction = fraction
        self.dataset = fetch_ucirepo(id=self.dataset_id)

    def load_data(self):
        # Load dataset from provided data
        df = self.dataset.data.features
        # Sample a subset of the dataset randomly based on the fraction
        sampled_df = df.sample(frac=self.fraction, random_state=42)
        return sampled_df

    def construct_formal_context(self, df, selected_attributes):
        # Extract unique values for each selected attribute to form the attribute list
        attribute_values = {attr: set(df[attr]) for attr in selected_attributes}

        # Flatten and sort the attribute list for consistency
        attribute_list = sorted([f"{attr}={val}" for attr, values in attribute_values.items() for val in values])

        # Initialize the binary matrix
        matrix = [[False for _ in attribute_list] for _ in range(len(df))]

        # Populate the binary matrix
        for i, row in df.iterrows():
            for j, attribute in enumerate(attribute_list):
                attr, val = attribute.split('=')
                if row[attr] == val:
                    matrix[i][j] = True

        # Define objects as indices of the DataFrame entries
        objects = list(df.index)

        return objects, attribute_list, matrix


def faster_algorithm(obj, attr, aMat):
        dictBC = {}
        def generate_lattice_dict(bCList):
            lattice_dict = {}
            for i, concept in enumerate(bCList, start=1):
                extent, intent = concept
                intent_str = [str(attr) for attr in intent]
                extent_str = [str(obj) for obj in extent]
                concept_dict = {"Intent": intent_str, "Extent": extent_str}
                lattice_dict[f"Concept {i}"] = concept_dict
            return json.dumps(lattice_dict)
        def get_bipartite_cliques(aMat):
            cList = []
            aLen = len(aMat)
            bLen = len(aMat[0])

            for x in range(0, aLen):
                tmpList = []
                tmpObj = [obj[x]]

                for y in range(0, bLen):
                    if aMat[x][y] == 1:
                        tmpList.append(attr[y])

                tmp = tmpObj, tmpList
                dictBC[obj[x]] = tmpList
                cList.append(tmp)

            for x in range(0, bLen):
                tmpList = []
                tmpAttr = [attr[x]]

                for y in range(0, aLen):
                    if aMat[y][x] == 1:
                        tmpList.append(obj[y])

                tmp = tmpList, tmpAttr
                dictBC[attr[x]] = tmpList
                cList.append(tmp)

            return cList

        def condense_list(inputlist):
            clist = []
            to_skip = []

            for x in range(0, len(inputlist)):
                if x in to_skip:
                    continue
                matched = 0
                for y in range(x+1, len(inputlist)):
                    if y in to_skip:
                        continue
                    if set(inputlist[x][0]) == set(inputlist[y][0]):
                        tmp_tuple = inputlist[x][0], list(set(inputlist[x][1]).union(set(inputlist[y][1])))
                        clist.append(tmp_tuple)
                        to_skip.append(y)
                        matched = 1
                        break
                    elif set(inputlist[x][1]) == set(inputlist[y][1]):
                        tmp_tuple = list(set(inputlist[x][0]).union(set(inputlist[y][0]))), inputlist[x][1]
                        clist.append(tmp_tuple)
                        to_skip.append(y)
                        matched = 1
                        break
                if matched == 0:
                    clist.append(inputlist[x])

            return clist

        def generate_lattice(bCList):
            G = nx.Graph()
            for concept in bCList:
                logging.info("concept : %s",concept)
                extent, intent = concept
                node_name = "(" + ", ".join(str(m) for m in extent) + "), (" + ", ".join(str(m) for m in intent) + ")"
                G.add_node(node_name)

            for i in range(len(bCList)):
                for j in range(i + 1, len(bCList)):
                    if set(bCList[i][0]).issubset(set(bCList[j][0])) or set(bCList[j][0]).issubset(set(bCList[i][0])):
                        node_name1 = "(" + ", ".join(str(m) for m in bCList[i][0]) + "), (" + ", ".join(str(m) for m in bCList[i][1]) + ")"
                        node_name2 = "(" + ", ".join(str(m) for m in bCList[j][0]) + "), (" + ", ".join(str(m) for m in bCList[j][1]) + ")"
                        G.add_edge(node_name1, node_name2)

            pos = nx.spring_layout(G)



        bCliques = get_bipartite_cliques(aMat)
        bCliquesStore = bCliques

        bCListSize = len(bCliques)
        bCListSizeCondensed = -1

        while bCListSize != bCListSizeCondensed:
            bCListSize = len(bCliques)
            bCliques = condense_list(bCliques)
            bCListSizeCondensed = len(bCliques)

        supremum_attrs = [at for at in attr if all(aMat[obj.index(o)][attr.index(at)] for o in obj)]
        supremum = (tuple(obj), tuple(supremum_attrs))
        infimum_objs = [o for o in obj if all(aMat[obj.index(o)][attr.index(at)] for at in attr)]
        infimum = (tuple(infimum_objs), tuple(attr))
        if supremum not in bCliques:
            bCliques.append(supremum)
        if infimum not in bCliques:
            bCliques.append(infimum)
        bCliques = list(set(tuple(tuple(x) for x in sub) for sub in bCliques))

        bCliques.sort(key=lambda x: len(x[0]), reverse=True)

        conceptDict = {}
        for x in range(len(bCliques)):
            obj_str = "".join(str(m) for m in sorted(bCliques[x][0]))
            attr_str = "".join(str(m) for m in sorted(bCliques[x][1]))
            conceptDict[obj_str] = set(bCliques[x][1])
            conceptDict[attr_str] = set(bCliques[x][0])

        generate_lattice(bCliques)
       
        return bCliques


def randomized_response(original_value, epsilon):
    random.seed(42)
    p = 1 / (1 + 1 / (epsilon + 1e-10))
    if original_value:
        return random.random() < p
    else:
        return random.random() >= p


def apply_aldp_to_context(objects, attributes, matrix, context_sensitivity):
    try:
        noisy_matrix = []
        for row in matrix:
            noisy_row = [adaptive_ldp(cell, context_sensitivity) for cell in row]
            noisy_matrix.append(noisy_row)
        return objects, attributes, noisy_matrix
    except Exception as e:
            logging.error('Error adaptive_ldp message: %s', e)


def adaptive_ldp(original_value, context_info):
    epsilon = 1 + 9 * context_info
    return randomized_response(original_value, epsilon)

class LocalServer:
    def __init__(self, objects, attributes, matrix):
        self.objects = objects
        self.attributes = attributes
        self.matrix = matrix
        self.received_globallattices = []

class EncryptedLocalServer(LocalServer):
    def __init__(self, objects, attributes, matrix, context):
        super(EncryptedLocalServer, self).__init__(objects, attributes, matrix)
        self.context = context
        self.noisy_objects, self.noisy_attributes, self.noisy_matrix = self.process_data()

    def process_data(self):
        try:
            noisy_objects, noisy_attributes, noisy_matrix = apply_aldp_to_context(
            self.objects, self.attributes, self.matrix, self.context
            )
            return noisy_objects, noisy_attributes, noisy_matrix
        except Exception as e:
            logging.error('Error process data message: %s', e)

    def create_perturbated_lattice(self):
        lattice = faster_algorithm(
            self.noisy_objects, self.noisy_attributes, self.noisy_matrix
        )
        return lattice

    def encrypt_data(self, data, key):
        cipher_suite = Fernet(key)
        encrypted_data = cipher_suite.encrypt(data)
        return encrypted_data

    def send_encrypted_data(self, global_server, key):
        encrypted_lattice = self.encrypt_data(str(self.create_perturbated_lattice()).encode(), key)
        global_server.receive_encrypted_data(encrypted_lattice)
    def receive_globallattice(self, encrypted_globallattice, key):
        cipher_suite = Fernet(key)
        decrypted_data = cipher_suite.decrypt(encrypted_globallattice).decode()
        self.received_globallattices.append(eval(decrypted_data))
        return decrypted_data
    
class ALMActor:
    def __init__(self, kafka_servers, actor_id=None):
        self.kafka_servers = kafka_servers
        self.config_file = "config.yml"
        self.load_config()
        self.actor_id = actor_id if actor_id else socket.gethostname()
        self.data=f"No Data from {self.actor_id}"
        self.key = None  # Initialize key as None
        self.context_sensitivity=0.8 
        self.encryptedlattice=None
        self.cipher_suite = None
        self.startTime=None
        self.endTime=None
        self.RunTime=None
        self.runtime_data=None
        self.producer = Producer({'bootstrap.servers': kafka_servers})
        self.consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'alm_group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([self.actor_id])
    def load_config(self):
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)
        self.num_clients = config['clients']['num_clients']
        self.dataset_id = config['dataset']['id']
        self.fraction = config['fraction']
        self.privacy_budget = config['privacy_budget']
        self.context_sensitivity= config['privacy_budget']
    def set_key(self, key):
        self.key = key.encode('utf-8')  # Convert the key to bytes
        self.cipher_suite = Fernet(self.key)
    def handle_message(self, message):
        try:
            
            message = json.loads(message.value().decode('utf-8'))
            logging.info('message handling message: %s', message)
            if 'key' in message:
                #  # Set the received key
                self.set_key(message['key']) 
                self.context_sensitivity = float(message['context_sensitivity'])
                logging.info('execute_task message')
                #self.execute_task()
                self.startTime = time.time()
                self.build_lattice()
                self.endTime= time.time()
                self.RunTime = self.endTime -  self.startTime
                # Prepare data to send to Kafka
                self.runtime_data = {
                    "Actor": self.actor_id,
                    "StartTime":  self.startTime,
                    "EndTime": self.endTime,
                    "Runtime": self.RunTime
                }
                logging.info("runtime :%s",self.runtime_data)
        except Exception as e:
            logging.error('Error handling message: %s', e)
        
       
    def decrypt_key(self, encrypted_key):
        try:
            return Fernet(self.key).decrypt(encrypted_key.encode('utf-8'))
        except Exception as e:
            logging.error("Error encrypting and sending result: %s", e)

    def build_lattice(self):
        try:
 
            logging.info("dataset_id: %s", self.dataset_id)
            loader = FCLoader(self.dataset_id, self.fraction)
            df = loader.load_data()
            selected_attributes = df.columns
            logging.info("selected_attributes: %s ", df.columns)
            objects, attributes, matrix = loader.construct_formal_context(loader.dataset.data.features, selected_attributes)
            # The objects, attributes, and matrix variables now contain the formal context.
             
            logging.info("Objects: %s ", objects)
            logging.info("Attributes: %s", attributes)
            logging.info("Matrix: %s ", matrix)
            logging.info("Begin Lattice Buidding: %s")
            local_server = EncryptedLocalServer(objects, attributes, matrix, self.privacy_budget)
            lattice=local_server.create_perturbated_lattice()
            logging.info("End Lattice Buidding: %s")
            logging.info("lattice: %s ", lattice)
            self.encryptedlattice = local_server.encrypt_data(str(lattice).encode(), self.key)
            logging.info("Begin Encrypting and sending result")
            self.encrypt_and_send_result()
            logging.info("End Encrypting and sending result")
        except Exception as e:
            logging.error("Error encrypting and sending result: %s", e)
  

    def execute_task(self):
        # Example task: Calculate PI (dummy calculation)
        pi_value = 3.14159
        self.data = str(pi_value)
        self.save_result(pi_value)
         
        self.send_pi_result('AGM', pi_value)
        logging.info("Begin Encrypting and sending result")
        self.encrypt_and_send_result()
        logging.info("End Encrypting and sending result")

    def save_result(self, result):
        # Example: Save task result as JSON
        result_data = {'actor_id': self.actor_id, 'result': result}
        with open(f"{self.actor_id}_result.json", "w") as f:
            json.dump(result_data, f)
        logging.info("Saved result as %s_result.json", self.actor_id)
        
    def encrypt_and_send_result2(self):
        if not self.key:
            logging.error("Key is not initialized. Cannot encrypt message.")
            return
        
        encrypted_data = self.cipher_suite.encrypt(self.data.decode('utf-8'))
        self.producer.produce('AGM', value=encrypted_data,callback=self.delivery_report)
        self.producer.flush()
        logging.info("Sent encrypted result to AGM")

    def encrypt_and_send_result(self):
        try:
            self.producer.produce('AGM', value=json.dumps({'result': {self.actor_id:self.encryptedlattice.decode('utf-8')}}).encode('utf-8'),callback=self.delivery_report)
            self.producer.flush()
            self.producer.produce('AGM', value=json.dumps({'stats': {self.actor_id:self.runtime_data.decode('utf-8')}}).encode('utf-8'),callback=self.delivery_report) 
            self.producer.flush()
            logging.info("Sent encrypted result to AGM")
            logging.info("Sent stats result to AGM %s",{'stats': {self.actor_id:self.runtime_data.decode('utf-8')}}).encode('utf-8')
        except Exception as e:
            logging.error("Error encrypting and sending result: %s", e)

    def send_pi_result(self, topic, pi_value):
        message = json.dumps({'pi_result': pi_value})
        self.producer.produce(topic, value=message, callback=self.delivery_report)
        self.producer.flush()  # Make sure the message is sent

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error("Consumer error: %s", msg.error())
                        continue
                logging.info("Received message: %s", msg.value().decode('utf-8'))
                self.handle_message(msg)
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt: Stopping ALMActor")

if __name__ == "__main__":
    #kafka_servers = 'PLAINTEXT://172.23.0.3:9092,PLAINTEXT://localhost:29092,PLAINTEXT://localhost:29092,PLAINTEXT://kafka-1:9092,PLAINTEXT://kafka-2:9093,PLAINTEXT://kafka-3:9094'
    kafka_servers = 'PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19093,PLAINTEXT://kafka-3:19094'

    logging.basicConfig(level=logging.INFO)
    alm_actor = ALMActor(kafka_servers)
    # Example usage

    logging.info("actor_id: %s", alm_actor.actor_id)
    alm_actor.run()  # Start ALMActor
    logging.info("key: %s", alm_actor.key)

