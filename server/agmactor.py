import json
import logging
import socket
from cryptography.fernet import Fernet
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import networkx as nx
import random
import yaml 
import ast
import json
class GlobalServer:
    def __init__(self):
        self.decrypted_lattices = []

class EncryptedGlobalServer(GlobalServer):
    def __init__(self):
        super().__init__()
        self.objects = []
        self.attributes = []
    def receive_encrypted_lattice_from_local(self, encrypted_data, secret_key):
        cipher_suite = Fernet(secret_key)
        decrypted_data = cipher_suite.decrypt(encrypted_data).decode()
        self.decrypted_lattices.append(eval(decrypted_data))
        return decrypted_data
    def num_concepts(self):
        if self.decrypted_lattices:
            num_concepts = len(self.decrypted_lattices[-1])
            return num_concepts
        else:
            return 0
    def get_infimum_concept(self):
        if self.objects is not None and self.attributes is not None and self.decrypted_lattices:
            infimum_objs = None
            infimum_attrs = set()
            max_properties = 0
            for lattice in self.decrypted_lattices:
                for concept in lattice:
                    num_properties = len(concept[0]) + len(concept[1])
                    if num_properties > max_properties:
                        max_properties = num_properties
                        infimum_objs, infimum_attrs = concept[0], set(concept[1])
            for lattice in self.decrypted_lattices:
                for concept in lattice:
                    infimum_objs = list(set(infimum_objs) & set(concept[0]))
                    infimum_attrs = infimum_attrs.union(set(concept[1]))

            return infimum_objs, list(infimum_attrs)
        else:
            return None




    def get_supremum_concept(self):
            if self.objects is not None and self.attributes is not None and self.decrypted_lattices:
                supremum_objs = set()
                supremum_attrs = None

                max_objects = 0
                for lattice in self.decrypted_lattices:
                    for concept in lattice:
                        num_objects = len(concept[0])
                        if num_objects > max_objects:
                            max_objects = num_objects
                            supremum_objs, supremum_attrs = set(concept[0]), concept[1]

                for lattice in self.decrypted_lattices:
                    for concept in lattice:
                        supremum_objs = supremum_objs.union(set(concept[0]))
                        supremum_attrs = list(set(supremum_attrs) & set(concept[1]))

                return list(supremum_objs), supremum_attrs
            else:
                return None

    
    def union_decrypted_lattices(self):
        flattened_list = [item for sublist in self.decrypted_lattices for item in sublist]

        infimum_concept = self.get_infimum_concept()
        supremum_concept = self.get_supremum_concept()
        other_concepts = flattened_list
        unique_concepts = []

        unique_concepts.extend([infimum_concept, supremum_concept])

        for concept in other_concepts:
            if concept not in unique_concepts:
                unique_concepts.append(concept)
                logging.info("Concept:%s",concept)
              

        empty_concepts = [concept for concept in unique_concepts if len(concept[0]) == 0 and len(concept[1]) > 0]
        if len(empty_concepts) > 1:
            unique_concepts = [concept for concept in unique_concepts if concept not in empty_concepts[1:]]

        sorted_other_concepts = sorted(unique_concepts, key=lambda x: len(x[0]), reverse=True)

        formatted_result = [
            (list(concept[0]), list(concept[1])) if concept is not None else (list(self.objects), []) for concept in sorted_other_concepts
        ]

        return formatted_result

    def generate_lattice_dict(self, bCList):
        lattice_dict = {}
        for i, concept in enumerate(bCList, start=1):
            extent, intent = concept
            intent_str = [str(attr) for attr in intent]
            extent_str = [str(obj) for obj in extent]
            concept_dict = {"Intent": intent_str, "Extent": extent_str}
            lattice_dict[f"Concept {i}"] = concept_dict
        return json.dumps(lattice_dict)

    def generate_gllattice(self, bCList):
            G = nx.Graph()
            for concept in bCList:
                extent, intent = concept
                node_name = "(" + ", ".join(str(m) for m in extent) + "), (" + ", ".join(str(m) for m in intent) + ")"
                G.add_node(node_name)


            for i in range(len(bCList)):
                for j in range(i + 1, len(bCList)):
                    if set(bCList[i][0]).issubset(set(bCList[j][0])) or set(bCList[j][0]).issubset(set(bCList[i][0])):
                        node_name1 = "(" + ", ".join(str(m) for m in bCList[i][0]) + "), (" + ", ".join(str(m) for m in bCList[i][1]) + ")"
                        node_name2 = "(" + ", ".join(str(m) for m in bCList[j][0]) + "), (" + ", ".join(str(m) for m in bCList[j][1]) + ")"
                        G.add_edge(node_name1, node_name2)

            #pos = nx.spring_layout(G)

            #nx.draw(G, pos, with_labels=True)
            #plt.show()


    def encrypt_globallattice(self, data, key):
        cipher_suite = Fernet(key)
        encryptgloballattice = cipher_suite.encrypt(data)
        return encryptgloballattice
    def send_encrypt_globallattice(self, local_server, key):
        encrypted_glattice = self.encrypt_globallattice(str(self.union_decrypted_lattices()).encode(), key)
        local_server.receive_globallattice(encrypted_glattice)
        
class AGMActor:
    def __init__(self, kafka_servers,context_sensitivity):
        self.config_file = "config.yml"
        self.load_config()
        self.global_server = EncryptedGlobalServer()
        self.kafka_servers = kafka_servers
        self.key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.key)
        self.fca_lattice = None
        self.global_lattice=None
        self.context_sensitivity=context_sensitivity
        self.producer = Producer({'bootstrap.servers': kafka_servers})
        self.consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'agm_group',
            'auto.offset.reset': 'earliest',
            'client.id': socket.gethostname()
        })
        self.consumer.subscribe(['AGM'])
        self.results = {}
    def receive_encrypted_lattice_from_local(self, encrypted_data, secret_key):
        cipher_suite = Fernet(secret_key)
         
        self.fca_lattice= eval(cipher_suite.decrypt(encrypted_data).decode())
        return self.fca_lattice
    def load_config(self):
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)
        self.num_clients = config['clients']['num_clients']
        #self.dataset_id = config['dataset']['id']
        #self.fraction = config['fraction']
        self.privacy_budget = config['privacy_budget']    
    def config_topics(self):
        client = AdminClient({'bootstrap.servers': self.kafka_servers, 'debug': 'broker,admin'})
        new_topics = [NewTopic(topic, num_partitions=3, replication_factor=3) for topic in ['AGM','stats','fca_actor','alm_actor1', 'alm_actor2', 'alm_actor3', 'alm_actor4', 'alm_actor5']]
        fs = client.create_topics(new_topics, request_timeout=15)
        for topic, f in fs.items():
            try:
                f.result()
                logging.info("Topic %s created", topic)
            except Exception as e:
                logging.error("Failed to create topic %s: %s", topic, e)

    def initiate_federation(self):
        alm_actors = ['alm_actor1', 'alm_actor2', 'alm_actor3', 'alm_actor4', 'alm_actor5']
        participants = random.sample(alm_actors, self.num_clients)
        participants.append('fca_actor')
        encrypted_key = self.key.decode('utf-8')
        logging.info("Encrypted key: %s", encrypted_key)
        for participant in participants:
            self.producer.produce(participant, value=json.dumps({'key': encrypted_key,'context_sensitivity':self.context_sensitivity}).encode('utf-8'))
            logging.info("Sent key to %s", participant)
 
        

 
    def compute_f1_score(self):
        try:
            if self.fca_lattice is not None and self.global_lattice is not None:
                true_positives = 0
                false_positives = 0
                false_negatives = 0
                
                fca_lattice_list = self.fca_lattice
                global_lattice_list = self.global_lattice

                logging.info("fca lattice: %s", fca_lattice_list)
                logging.info("global lattice: %s", global_lattice_list)

                for fca_concept in global_lattice_list:
                    fca_intent, fca_extent = fca_concept
                    for global_concept in global_lattice_list:
                        global_intent, global_extent = global_concept
                        if set(fca_intent).issubset(set(global_intent)) \
                            and set(fca_extent).issubset(set(global_extent)) \
                            and set(global_intent).issubset(set(fca_intent)) \
                            and set(global_extent).issubset(set(fca_extent)):
                            true_positives += 1
                            break
                    else:
                        false_negatives += 1
                
                for global_concept in global_lattice_list:
                    global_intent, global_extent = global_concept
                    for fca_concept in fca_lattice_list:
                        fca_intent, fca_extent = fca_concept
                        if set(global_intent).issubset(set(fca_intent)) \
                            and set(global_extent).issubset(set(fca_extent)) \
                            and set(fca_intent).issubset(set(global_intent)) \
                            and set(fca_extent).issubset(set(global_extent)):
                            break
                    else:
                        false_positives += 1

                precision = true_positives / (true_positives + false_positives)
                recall = true_positives / (true_positives + false_negatives)

                # Calculate F1-score
                if precision + recall == 0:
                    f1_score = 0
                else:
                    f1_score = 2 * (precision * recall) / (precision + recall)

                return f1_score
            else:
                return None
        except Exception as e:
            logging.error("Error computing F1-mesure: %s", e)
    def compute_f1_score3(self):
        try:
            if self.fca_lattice is not None and self.global_lattice is not None:
                true_positives = 0
                false_positives = 0
                false_negatives = 0
                
                fca_lattice_list = self.fca_lattice
                global_lattice_list = self.global_lattice

                for fca_concept in fca_lattice_list:
                    fca_intent, fca_extent = fca_concept
                    matched = False
                    for global_concept in global_lattice_list:
                        global_intent, global_extent = global_concept
                        # Check if the FCA concept is a subset of any global concept
                        if set(fca_intent).issubset(set(global_intent)) and \
                                set(fca_extent).issubset(set(global_extent)):
                            true_positives += 1
                            matched = True
                            break
                    
                    if not matched:
                        false_negatives += 1
                
                for global_concept in global_lattice_list:
                    global_intent, global_extent = global_concept
                    matched = False
                    for fca_concept in fca_lattice_list:
                        fca_intent, fca_extent = fca_concept
                        # Check if any global concept is a subset of the FCA concept
                        if set(global_intent).issubset(set(fca_intent)) and \
                                set(global_extent).issubset(set(fca_extent)):
                            matched = True
                            break
                    
                    if not matched:
                        false_positives += 1

                precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) != 0 else 0
                recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) != 0 else 0

                # Calculate F1-score
                f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) != 0 else 0

                return f1_score
            else:
                return None
        except Exception as e:
            logging.error("Error computing F1-mesure: %s", e)



    def compute_f1_score2(self):
        try:
            if self.fca_lattice is not None and self.global_lattice is not None:
                true_positives = 0
                false_positives = 0
                false_negatives = 0
                 
                for concept in  ast.literal_eval(self.fca_lattice):
                    logging.info("Concept %s:",concept)
                    if concept in  (self.global_lattice):
                        true_positives += 1
                    else:
                        false_negatives += 1
                
                for concept in self.global_lattice:
                    if concept not in self.fca_lattice:
                        false_positives += 1
                        
                precision = true_positives / (true_positives + false_positives)
                recall = true_positives / (true_positives + false_negatives)
                
                # Calculate F1-score
                if precision + recall == 0:
                    f1_score = 0
                else:
                    f1_score = 2 * (precision * recall) / (precision + recall)
                
                return f1_score
            else:
                return None
        except Exception as e:
            logging.error("Error computing F1-mesure :%s",e)

    def handle_quality(self):
            # Example usage
            
            self.quality = self.compute_f1_score3()
            print("F1-score:", self.quality)
            logging.info("F1-mesure: % s",self.quality)

    def handle_message(self, message):
        try:
            message = json.loads(message.value().decode('utf-8'))
            #logging.info("Received message: %s", message)
            if 'result' in message:
                result_dict = message['result']
                for recipient, encrypted_data in result_dict.items():
                    self.global_server.receive_encrypted_lattice_from_local(encrypted_data,self.key)
                    self.global_lattice=self.global_server.union_decrypted_lattices()
                    
                    #logging.info("Decrypted data for recipient %s: %s", recipient, lattice)
                    decrypted_data = self.decrypt_message(encrypted_data)
                    if decrypted_data is not None:
                        #logging.info("Decrypted data for recipient %s: %s", recipient, decrypted_data)
                        self.results[recipient] = decrypted_data
                    else:
                        logging.error("Error decrypting data for recipient %s", recipient)
            elif 'fca-central' in message:
                result_dict = message['fca-central']
                for recipient, encrypted_data in result_dict.items():
                    self.fca_lattice = self.receive_encrypted_lattice_from_local(encrypted_data,self.key)   
                    logging.info("Decrypted data for recipient %s: %s", recipient, type(self.fca_lattice))
                    decrypted_data = self.receive_encrypted_lattice_from_local(encrypted_data,self.key) 
                    if decrypted_data is not None:
                        logging.info("Decrypted data for recipient %s: %s", recipient, decrypted_data)
                        self.results[recipient] = decrypted_data
                    else:
                        logging.error("Error decrypting data for recipient %s", recipient)
            self.handle_quality()      
        except Exception as e:
            logging.error("Error handling message: %s", e)

    def decrypt_message(self, encrypted_data):
        try:
            decrypted_data = self.cipher_suite.decrypt(encrypted_data.encode('utf-8'))
            return decrypted_data.decode('utf-8')
        except Exception as e:
            logging.error("Error decrypting message: %s", e)
            return None

    def compute_global_average(self):
        results = [float(result) for result in self.results.values()]
        if results:
            global_avg = sum(results) / len(results)
            self.save_json_result(global_avg)
        else:
            logging.warning("No results to compute global average.")

    def save_json_result(self, data):
        with open("global_average.json", "w") as f:
            json.dump({'global_average': data}, f)
        logging.info("Global average result saved as JSON.")

    def run(self):
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
            self.handle_message(msg)

if __name__ == "__main__":
    kafka_servers = 'PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19093,PLAINTEXT://kafka-3:19094'
    logging.basicConfig(level=logging.INFO)
    context_sensitivity=0.8
    agm_actor = AGMActor(kafka_servers,context_sensitivity)
    agm_actor.config_topics()
    agm_actor.initiate_federation()
    agm_actor.run()
