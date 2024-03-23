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
from collections import Counter
import time
from logactor import LoggerActor
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
        try:

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
        except Exception as e:
            logging.error("error computing infimum :%s",e)



    def get_supremum_concept(self):
        try:
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
        except Exception as e:
            logging.error("error computing suprremum :%s",e)
    def union_decrypted_lattices(self):
        """
            Combines decrypted lattices using supremum and infimum calculations,
            respecting lattice theory principles.

            Returns:
                A list of unique concept pairs (intent, extent) representing the combined lattice.
        """
        # try:
        # Use a set for efficient membership checking
        flattened_list = [item for sublist in self.decrypted_lattices for item in sublist]
         
        unique_concepts = []
        formatted_result =[]
        logging.info("global lattice:%s",flattened_list)
        # Infimum and supremum (can be pre-calculated if known)
        infimum_concept = self.get_infimum_concept()
        supremum_concept = self.get_supremum_concept()
        
        unique_concepts.append(infimum_concept)
        unique_concepts.append(supremum_concept)

        for concept in flattened_list:
            logging.info("concept :%s", concept)
            for other_concept in unique_concepts.copy():  # Iterate over a copy to avoid modification issues
                if concept != other_concept:
                    supremum = self.calculate_supremum(concept, other_concept)
                    infimum = self.calculate_infimum(concept, other_concept)

                # Option 1: Convert extents (concept[1]) to tuples before adding to set (preferred)
                if supremum not in unique_concepts and supremum != infimum:
                    unique_concepts.append((concept[0], tuple(supremum[1])))

                if infimum not in unique_concepts and infimum != supremum:
                    unique_concepts.append((concept[0], tuple(infimum[1])))

                # Option 2: Modify unique_concepts to hold entire concept tuples (if separate intent/extent not needed)
                # unique_concepts = set(self.decrypted_lattices)  # concepts is a list of concept tuples

        # Convert set of concepts to formatted list
        formatted_result =(unique_concepts)
        print(f"global lattice ssss:{formatted_result}")
        return formatted_result
    # except Exception as e:
    #     logging.error("error lattice aggregator :%s", e)

 
 
    def calculate_supremum(self, concept1, concept2):
        """
        Calculates the supremum (least upper bound) of two concepts.

        Args:
            concept1: A concept represented as a tuple (intent, extent).
            concept2: A concept represented as a tuple (intent, extent).

        Returns:
            The supremum concept (intent, extent).
        """
        intent_sup = set(concept1[0]) | set(concept2[0])  # Union of intents
        extent_sup = [obj for obj in self.objects if obj in concept1[1] or obj in concept2[1]]  # Intersection of extents
    
        return (list(intent_sup), extent_sup)

    def calculate_infimum(self, concept1, concept2):
        """
        Calculates the infimum (greatest lower bound) of two concepts.

        Args:
            concept1: A concept represented as a tuple (intent, extent).
            concept2: A concept represented as a tuple (intent, extent).

        Returns:
            The infimum concept (intent, extent).
        """
        intent_inf = list(set(concept1[0]) & set(concept2[0]))  # Intersection of intents
        extent_inf = [obj for obj in concept1[1] if obj in concept2[1]]  # Intersection of extents
        return (intent_inf, extent_inf)
    
    def compute_f1_score4(self):
      """
      Computes the F1 score between a generated lattice and a ground truth FCA lattice.

      Args:
        generated_lattice: A list of lists representing the generated lattice structure.
        fac_lattice: A list of lists representing the ground truth FCA lattice structure.

      Returns:
        A tuple containing precision, recall, and F1 score (all floats between 0 and 1).
      """
      # Count true positives, false positives, false negatives
      tp = fp = fn = 0
      for concept in self.global_lattice:
        # Check if concept exists in fac_lattice
        if concept in  self.fca_lattice:
          tp += 1  # True positive: concept is in both lattices
        else:
          fp += 1  # False positive: concept only in generated lattice

      # Count elements missing from generated lattice
      for concept in  self.fca_lattice:
        if concept not in  self.global_lattice:
          fn += 1  # False negative: concept only in fac_lattice

      # Calculate precision, recall, and F1 score (handle division by zero)
      precision = tp / (tp + fp) if tp + fp > 0 else 0
      recall = tp / (tp + fn) if tp + fn > 0 else 0
      f1_score = 2 * (precision * recall) / (precision + recall) if precision + recall > 0 else 0
      quality={"precision":precision,"recall":recall,"f1_score":f1_score}
      return quality

    def union_decrypted_lattices_old(self):
        flattened_list = [item for sublist in self.decrypted_lattices for item in sublist]
        logging.info("Lattice Received:%s",flattened_list)
        infimum_concept = self.get_infimum_concept()
        supremum_concept = self.get_supremum_concept()
        other_concepts = flattened_list
        unique_concepts = []

        unique_concepts.extend([infimum_concept, supremum_concept])
        logging.info("Global Lattice SUP/Inf:%s",unique_concepts)
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
        self.config_file = "/data/config.yml"
        self.dataset_id =  None
        self.fraction = None
        self.privacy_budget = None
        self.startTime=None
        self.endTime=None
        self.RunTime=None
        self.runtime_data=None
        self.quality_data={}
        self.load_config()
        self.actor_id = socket.gethostname()
        self.logactor= LoggerActor(self.actor_id)
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
        self.dataset_id = config['dataset']['id']
        self.fraction = config['fraction']
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
 
        

    def compute_f1_score4(self):
      """
      Computes the F1 score between a generated lattice and a ground truth FCA lattice.

      Args:
        generated_lattice: A list of lists representing the generated lattice structure.
        fac_lattice: A list of lists representing the ground truth FCA lattice structure.

      Returns:
        A tuple containing precision, recall, and F1 score (all floats between 0 and 1).
      """
      # Count true positives, false positives, false negatives
      tp = fp = fn = 0
      for concept in self.global_lattice:
        # Check if concept exists in fac_lattice
        if concept in  self.fca_lattice:
          tp += 1  # True positive: concept is in both lattices
        else:
          fp += 1  # False positive: concept only in generated lattice

      # Count elements missing from generated lattice
      for concept in  self.fca_lattice:
        if concept not in  self.global_lattice:
          fn += 1  # False negative: concept only in fac_lattice

      # Calculate precision, recall, and F1 score (handle division by zero)
      precision = tp / (tp + fp) if tp + fp > 0 else 0
      recall = tp / (tp + fn) if tp + fn > 0 else 0
      f1_score = 2 * (precision * recall) / (precision + recall) if precision + recall > 0 else 0
      quality={"precision":precision,"recall":recall,"f1_score":f1_score}
      return quality


     
    def compute_f1_score(self):
        try:
            logging.info("fca lattice: %s", self.fca_lattice)
            logging.info("global lattice: %s", self.global_lattice)
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
            quality={}
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
                quality["precision"]=precision
                quality["recall"]=recall
                # Calculate F1-score
                f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) != 0 else 0
                quality["f1_score"]=f1_score
                return quality
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
            try:
                    
                logging.info(f"{len (self.global_server.decrypted_lattices) } from: % s",self.num_clients)
                if self.global_lattice is not None:
                    self.quality = self.compute_f1_score4()
                    logging.info("F1-score:%s", self.quality)
                    self.quality_data = {
                    "Dataset_id": self.dataset_id,
                    "Fraction": self.fraction,
                    "Privacy_budget": self.privacy_budget,
                    "Participant": self.num_clients,
                        "F1-score": self.quality["f1_score"],
                        "Precision": self.quality["precision"],
                        "Recall": self.quality["recall"]
                    }

                    self.logactor.log_stats( self.quality_data,keyspace='data_quality') 
                    logging.info("F1-mesure: % s",self.quality)
            except Exception as e:
                logging.error("Error quality : %s", e)
    def handle_message(self, message):
        try:
            message = json.loads(message.value().decode('utf-8'))
            #logging.info("Received message: %s", message)
            if 'result' in message:
                result_dict = message['result']
                try:
                    for recipient, encrypted_data in result_dict.items():
                        self.startTime = time.time()
                        self.global_server.receive_encrypted_lattice_from_local(encrypted_data,self.key)
                        self.global_lattice=self.global_server.union_decrypted_lattices()
                        
                        #logging.info("Decrypted data for recipient %s: %s", recipient, lattice)
                        decrypted_data = self.decrypt_message(encrypted_data)
                        if decrypted_data is not None:
                            #logging.info("Decrypted data for recipient %s: %s", recipient, decrypted_data)
                            self.results[recipient] = decrypted_data
                        else:
                            logging.error("Error decrypting data for recipient %s", recipient)
                        self.endTime = time.time()
                        self.RunTime = self.endTime-self.startTime
                        # Prepare data to send to Kafka
                        self.runtime_data = {
                            "Actor": self.actor_id,
                            "StartTime":  self.startTime,
                            "EndTime": self.endTime,
                            "Runtime": self.RunTime
                        }
                        self.logactor.log_stats(self.runtime_data)  
                        logging.info("runtime :%s",self.runtime_data)
                except Exception as e:
                    logging.error("runtime error :%s",e)
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
            # if 'stats' in message:
            #     try:
            #         result_dict = message['stats']
            #         logging.info("%s",result_dict)
            #         for recipient, stats_data in result_dict.items():
            #             if stats_data is not None:
            #                 task_id = str(stats_data['Actor'])
            #                 start_time = float(stats_data['StartTime'])
            #                 end_time = float(stats_data['EndTime'])
            #                 runtime = float(stats_data['Runtime'])
            #                 logging.info("%s",{'Actor': task_id, 'StartTime': start_time, 'EndTime': end_time, 'Runtime': runtime})
            #     except Exception as e:
            #             logging.error("Error stats data for recipient %s", e)
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
