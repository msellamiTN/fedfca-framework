import logging
import concurrent.futures
from itertools import combinations, chain
import networkx as nx
import matplotlib.pyplot as plt
import uuid
import logging
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
import pandas as pd
from sklearn.mixture import GaussianMixture
from collections import defaultdict
import json
import networkx as nx
from itertools import chain, combinations
# Now, use the FasterFCA class to process the file
import json
import networkx as nx
from itertools import chain, combinations
import seaborn as sns
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy import stats
import random
import sys
import logging
import math

import ast  # To safely evaluate the set structure
from cryptography.fernet import Fernet
import tenseal as ts
class LDPHandler:
    def __init__(self, formal_context, context_sensitivity):
        self.formal_context = formal_context  # Original context
        self.context_sensitivity = context_sensitivity  # Sensitivity value for the context
        self.ldp_context = {}  # This will store the noisy (LDP) context
        self.runtime=None

    def randomized_response(self, original_value, epsilon):
        """
        Apply randomized response to a boolean value (True/False).

        :param original_value: The original boolean value (True or False) for an attribute.
        :param epsilon: The privacy budget.
        :return: The noisy value based on randomized response.
        """
        random.seed()  # Ensure random values are different on each run
        p = 1 / (1 + 1 / (epsilon + 1e-10))  # Probability of keeping the original value
        if original_value:
            return random.random() < p  # Return True with probability p
        else:
            return random.random() >= p  # Return False with probability 1 - p

    def apply_aldp_to_formal_context(self):
        """
        Apply Adaptive LDP to the formal context by adding noise to the attribute sets.
        The LDP mechanism is applied to each attribute in the formal context.
        """
        BeginTime = time.time()
        for obj_index, attributes in self.formal_context.items():
            noisy_attributes = set()
            for attribute in attributes:
                # Apply LDP noise to each attribute in the formal context
                epsilon = 1 + 9 * self.context_sensitivity  # Adjust epsilon based on context sensitivity

                # Apply LDP to decide whether to keep the attribute
                noisy_value = self.randomized_response(True, epsilon)  # Simulate adding/removing an attribute

                if noisy_value:
                    noisy_attributes.add(attribute)  # Add to noisy set if 'True'

            self.ldp_context[obj_index] = noisy_attributes
        EndTime = time.time()
        self.runtime=EndTime-BeginTime
        return self.ldp_context  # Return the noisy context
    def get_runtime(self):
        return self.runtime
    def set_runtime(self, runtime):
        self.runtime = runtime


class StabilityCalculator:
    def __init__(self, formal_context):
        """
        Initialize the stability calculator with a formal context.
        :param formal_context: A dictionary representing the formal context
                               where keys are objects and values are attribute sets.
        """
        self.formal_context = formal_context
        self.objects = set(formal_context.keys())

    def extract_formal_context(self, file_obj):
        """
        Extract the formal context from a file object.
        :param file_obj: File object containing the formal context as lines of attributes.
        :return: The formal context as a dictionary.
        """
        formal_context = {}
        for obj_index, line in enumerate(file_obj, start=1):
            attributes = set(line.strip().split())
            formal_context[obj_index] = attributes
        self.objects = set(formal_context.keys())
        return formal_context
    def support(self, subset):
        """
        Calculate the support for a subset (objset or itemset).

        :param subset: A set of objects (objset) or a set of attributes (itemset).
        :return: The support of the subset.
        """
        if all(obj in self.formal_context for obj in subset):  # Objset (objects)
            common_attributes = set(self.formal_context[next(iter(subset))])  # Start with the attributes of the first object
            for obj in subset:
                common_attributes &= self.formal_context[obj]  # Intersection of attributes across objects
            return len(common_attributes)
        else:  # Itemset (attributes)
            count = 0
            for obj, attributes in self.formal_context.items():
                if subset.issubset(attributes):  # If the object has all attributes in the subset
                    count += 1
            return count



    def build_max_nongen(self, objects, attribute_set_size):
        """
        Build the maximal non-generator set (β).
        :param objects: The set of objects.
        :param attribute_set_size: The size of the attribute set to compare.
        :return: The maximal non-generator set.
        """
        max_nongen = set()
        for obj in sorted(objects, reverse=True):
            if self.support(max_nongen | {obj}) != attribute_set_size:
                max_nongen.add(obj)
        return max_nongen

    def compute_generators(self, obj, alpha, beta, attribute_set_size):
        """
        Compute the number of generators for a specific object.
        :param obj: The current object.
        :param alpha: The α set of objects.
        :param beta: The β set of objects.
        :param attribute_set_size: The size of the attribute set.
        :return: The count of minimal generators.
        """
        min_generators = set()
        non_generators = set()
        for other_obj in (beta | alpha - {obj}):
            if self.support({obj, other_obj}) == attribute_set_size:
                min_generators.add((obj, other_obj))
            else:
                non_generators.add((obj, other_obj))

        generator_count = sum(2 ** (len(min_generators) - i) for i in range(len(min_generators)))
        return generator_count

    def handle_object(self, obj, alpha, beta, attribute_set_size):
        """
        Handle the stability calculation for a single object.
        :param obj: The current object.
        :param alpha: The α set of objects.
        :param beta: The β set of objects.
        :param attribute_set_size: The size of the attribute set.
        :return: The count of generators contributed by this object.
        """
        generator_count = 0
        while alpha:
            generator_count += self.compute_generators(obj, alpha, beta, attribute_set_size)
            alpha, beta = self.update_alpha_beta(beta, attribute_set_size)
        return generator_count

    def update_alpha_beta(self, non_generators, attribute_set_size):
        """
        Update the α and β sets.
        :param non_generators: The current non-generator set.
        :param attribute_set_size: The size of the attribute set.
        :return: Updated α and β sets.
        """
        alpha, beta = set(), set()
        for obj in non_generators:
            if self.support(beta | {obj}) == attribute_set_size:
                alpha.add(obj)
            else:
                beta.add(obj)
        return alpha, beta

    def compute_stability_for_concept_old(self, extent, intent):
        """
        Compute the stability of a single formal concept.
        :param extent: The set of objects (extent).
        :param intent: The set of attributes (intent).
        :return: The stability value of the concept.
        """
        attribute_set_size = len(intent)
        stability = 0
        generator_set = set()

        for obj in extent:
            if self.support({obj}) == attribute_set_size:
                generator_set.add(obj)

        if generator_set:
            stability = 1 - 1 / (2 ** len(generator_set))

        max_nongen = self.build_max_nongen(extent - generator_set, attribute_set_size)
        alpha, beta = extent - max_nongen, max_nongen

        for obj in alpha:
            stability += self.handle_object(obj, alpha, beta, attribute_set_size)

        stability /= (2 ** len(extent))
        stability = min(stability, 1.0)
        return round(stability, 3)
    def compute_stability_for_concept(self, extent, intent):
        """
        Compute the stability of a single formal concept.
        :param extent: The set of objects (extent).
        :param intent: The set of attributes (intent).
        :return: The stability value of the concept.
        """
        attribute_set_size = len(intent)
        stability = 0
        generator_set = set()

        for obj in extent:
            if self.support({obj}) == attribute_set_size:
                generator_set.add(obj)

        if generator_set:
            stability = 1 - 1 / (2 ** len(generator_set))

        max_nongen = self.build_max_nongen(extent - generator_set, attribute_set_size)
        alpha, beta = extent - max_nongen, max_nongen

        for obj in alpha:
            stability += self.handle_object(obj, alpha, beta, attribute_set_size)
        # Use int for extremely large calculations

        extent_len = len(extent)

        # Apply a logarithmic scaling to avoid overflow
        if extent_len > 0:
            log_scale = math.log(extent_len + 1)  # Use log of the length for scaling
            stability /= log_scale  # Apply the scaling factor

        stability = min(stability, 1.0)
        return round(stability, 3)
    def compute_stability(self, lattice):
        """
        Compute the stability for all concepts in the lattice.
        :param lattice: A list of formal concepts, each as (extent, intent).
        :return: A list of tuples with (extent, intent, stability).
        """
        results = []
        for extent, intent in lattice:
            stability_value = self.compute_stability_for_concept(extent, intent)
            results.append((extent, intent, stability_value))
        return results



    # Function to create the 3D visualization of stability against extent, intent, and stability values
    def plot_stability_3d(self,results):
        # Extracting the data for plotting
        extents = []
        intents = []
        stabilities = []

        for extent, intent, stability in results:
            extents.append(len(extent))  # Use the size of extent as a representation
            intents.append(len(intent))  # Use the size of intent as a representation
            stabilities.append(stability)

        # Convert lists to numpy arrays for plotting
        extents = np.array(extents)
        intents = np.array(intents)
        stabilities = np.array(stabilities)

        # Create a 3D plot
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')

        # Scatter plot of extent, intent, and stability
        sc = ax.scatter(extents, intents, stabilities, c=stabilities, cmap='viridis', s=50)

        # Adding labels
        ax.set_xlabel('Extent Size')
        ax.set_ylabel('Intent Size')
        ax.set_zlabel('Stability')

        # Add a color bar to indicate stability value
        plt.colorbar(sc)

        # Show the plot
        plt.show()

class FasterFCA:
    def __init__(self, threshold=0.0, context_sensitivity=0.0):
        self.ARCHBIT = (sys.maxsize.bit_length() + 1)  # Dynamically calculate architecture word size
        self.BIT = 1 << (self.ARCHBIT - 1)
        self.formal_context = {}
        self.objects = 0
        self.attributes = 0
        self.context = []
        self.dictBC = {}
        self.lattice = []
        self.threshold = threshold
        self.context_sensitivity = context_sensitivity
        self.context_sensitivity=context_sensitivity
        self.StabilityCalculator = StabilityCalculator(self.formal_context)
        self.LDPHandler = LDPHandler(self.formal_context, self.context_sensitivity)
    def extract_formal_context(self, file):
        """
        Extract the formal context from a file with the structure:
        1: {2, 3, 5}
        2: {4, 5, 6}
        """
        self.formal_context = {}

        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                obj_id_str, attributes_str = line.split(":")
                obj_id = int(obj_id_str.strip())
                attributes = ast.literal_eval(attributes_str.strip())

                if not isinstance(attributes, set):
                    raise ValueError("Attributes must be a set.")

                self.formal_context[obj_id] = {int(attr) for attr in attributes}

            except ValueError as ve:
                print(f"Value error in line: '{line}' - {ve}")
            except SyntaxError as se:
                print(f"Syntax error in line: '{line}' - {se}")
            except Exception as e:
                print(f"Unexpected error in line: '{line}' - {e}")

        return self.formal_context

    def read_context(self, formal_context):
        """Process the context from the formal context dictionary using bitwise operations."""
        self.objects = len(formal_context)
        self.attributes = max(attr for attrs in formal_context.values() for attr in attrs)

        context_width = (self.attributes // self.ARCHBIT) + 1
        self.context = [[0] * context_width for _ in range(self.objects)]

        object_to_index = {obj_id: idx for idx, obj_id in enumerate(formal_context.keys())}

        for obj_id, attributes in formal_context.items():
            obj_index = object_to_index[obj_id]
            for attribute in attributes:
                col = (attribute - 1) // self.ARCHBIT
                bit_position = self.ARCHBIT - 1 - ((attribute - 1) % self.ARCHBIT)
                self.context[obj_index][col] |= (1 << bit_position)

    def get_bipartite_cliques(self):
        """Generate bipartite cliques from the formal context using bitset representation."""
        cList = []
        for i in range(self.objects):
            tmpList = []
            tmpObj = [i + 1]
            for j in range(self.attributes):
                if self.context[i][j // self.ARCHBIT] & (1 << (self.ARCHBIT - 1 - (j % self.ARCHBIT))):
                    tmpList.append(j + 1)

            cList.append((tmpObj, tmpList))

        for j in range(self.attributes):
            tmpList = []
            tmpAttr = [j + 1]
            for i in range(self.objects):
                if self.context[i][j // self.ARCHBIT] & (1 << (self.ARCHBIT - 1 - (j % self.ARCHBIT))):
                    tmpList.append(i + 1)

            cList.append((tmpList, tmpAttr))

        return cList

    def condense_list(self, inputlist):
        logging.debug("Initial input list: %s", inputlist)
        clist = []
        to_skip = []

        for x in range(len(inputlist)):
            if x in to_skip:
                continue
            matched = 0
            for y in range(x + 1, len(inputlist)):
                if y in to_skip:
                    continue
                if set(inputlist[x][0]) == set(inputlist[y][0]):
                    tmp_tuple = inputlist[x][0], list(set(inputlist[x][1]).union(set(inputlist[y][1])))
                    logging.debug("Merging on extent: %s and %s", inputlist[x], inputlist[y])
                    clist.append(tmp_tuple)
                    to_skip.append(y)
                    matched = 1
                    break
                elif set(inputlist[x][1]) == set(inputlist[y][1]):
                    tmp_tuple = list(set(inputlist[x][0]).union(set(inputlist[y][0]))), inputlist[x][1]
                    logging.debug("Merging on intent: %s and %s", inputlist[x], inputlist[y])
                    clist.append(tmp_tuple)
                    to_skip.append(y)
                    matched = 1
                    break
            if matched == 0:
                clist.append(inputlist[x])

        logging.debug("Condensed list: %s", clist)
        return clist

    def generate_lattice(self, bCList):
        """Generate the lattice structure."""
        G = nx.DiGraph()  # Using DiGraph to capture hierarchy
        nodes = []

        for concept in bCList:
            extent, intent = concept
            node_name = f"({', '.join(map(str, extent))}), ({', '.join(map(str, intent))})"
            G.add_node(node_name)
            nodes.append((extent, intent, node_name))

        for i, (e1, i1, n1) in enumerate(nodes):
            for j, (e2, i2, n2) in enumerate(nodes):
                if i != j and set(e1).issubset(e2) and set(i2).issubset(i1):
                    G.add_edge(n2, n1)

        pos = nx.spring_layout(G, seed=42)  # Reproducible layout
        nx.draw(G, pos, with_labels=True, node_size=5000, node_color="skyblue")
        nx.write_graphml(G, "lattice.graphml")  # Export to .graphml for external analysis

        return G


    def save_concepts_to_file(self, concepts, output_file):
        """
        Save the computed concepts to a file in JSON format.
        """

        lattice_dict = {}
        for i, (concept, stability) in enumerate(concepts, start=1):
            extent, intent = concept
            intent_str = [str(attr) for attr in intent]
            extent_str = [str(obj) for obj in extent]
            concept_dict = {"Intent": intent_str, "Extent": extent_str, "Stability":stability}

            lattice_dict[f"Concept {i}"] = concept_dict

        with open(output_file, 'w') as f:
            json.dump(lattice_dict, f, indent=4)
    def convert_bcliques_to_lattice(self, bcliques):
        """
        Convert bipartite cliques to lattice format with hashable structures.

        :param bcliques: List of bipartite cliques, each as a tuple (extent, intent).
        :return: Lattice as a list of tuples (frozenset, frozenset).
        """
        lattice = []
        for extent, intent in bcliques:
            # Convert extent and intent to frozensets
            extent_fset = frozenset(extent)
            intent_fset = frozenset(intent)
            lattice.append((extent_fset, intent_fset))
        return lattice



    def stablity_compute(self):
        """
        Compute the stability values for the lattice and filter based on a threshold.

        :param threshold: Minimum stability value required for a concept to be included.
        :return: A tuple containing the filtered lattice with stability values and average stability.
        """
        filtered_lattice_with_stability = []
        total_stability = 0
        count = 0

        for i, concept in enumerate(self.lattice, start=1):
            extent, intent = concept

            stability_value = self.StabilityCalculator.compute_stability_for_concept(extent, intent)
            #print(f"Concept {i}: {concept}, {extent}: {intent},{stability_value}")

            if stability_value >= self.threshold:
                filtered_lattice_with_stability.append(((extent, intent), stability_value))
                #print(f"Concept {i}: {concept}, Stability: {stability_value}")
                total_stability += stability_value
                count += 1

        average_stability = total_stability / count if count > 0 else 0
        return filtered_lattice_with_stability, average_stability

    def run(self, input_file, output_file):
        """
        Execute the Faster FCA algorithm to compute formal concepts and save the result,
        filtering the lattice based on a stability threshold.

        :param input_file: Path to the input file containing the formal context.
        :param output_file: Path to the output file to save results.
        :param threshold: Minimum stability value required for a concept to be included.
        :return: Filtered lattice with stability values in the format [((frozenset, frozenset), stability_value)].
        """
        # Step 1: Read formal context from the input file
        logging.info(f"Reading path input file: {input_file}")
        try:
            with open(input_file, 'r') as f:
                self.formal_context = self.extract_formal_context(f)
                #print(self.formal_context)
        except Exception as e:
            raise ValueError(f"Error reading input file: {e}")

        # Step 2: Process the formal context into internal representation
        self.LDPHandler.formal_context = self.formal_context
        self.ldp_context=self.LDPHandler.apply_aldp_to_formal_context()
        self.read_context(self.ldp_context)
         # Apply LDP to formal context before generating bipartite cliques
        #print(self.compare_formal_contexts())
        # Step 3: Generate bipartite cliques
        bCliques = self.get_bipartite_cliques()

        # Step 4: Condense bipartite cliques iteratively
        bCliquesSize = len(bCliques)
        while True:
            bCliquesCondensed = self.condense_list(bCliques)
            if len(bCliquesCondensed) == bCliquesSize:
                break
            bCliquesSize = len(bCliquesCondensed)
            bCliques = bCliquesCondensed

        # Step 5: Convert bipartite cliques into lattice format
        self.lattice = self.convert_bcliques_to_lattice(bCliques)

        # Debug: Display intermediate results
        #print("Formal Context:", self.formal_context)
        #print("Bipartite Cliques (Lattice):", self.lattice)

        # Step 6: Assign formal context and lattice to StabilityCalculator
        self.StabilityCalculator.formal_context = self.formal_context
        self.StabilityCalculator.lattice = self.lattice

        # Step 7: Compute stability and filter based on threshold
        filtered_lattice_with_stability, average_stability = self.stablity_compute()
        #print(f"Filtered Lattice (Threshold: {self.threshold}):", filtered_lattice_with_stability)
        # print("Average Stability:", average_stability)

        # Step 8: Save the filtered lattice to the output file
        self.save_concepts_to_file(filtered_lattice_with_stability, output_file)

        # Return the filtered lattice in a format compatible with Aggregator
        #print("differences",self.compare_formal_contexts())
        return filtered_lattice_with_stability
    def compare_formal_contexts(self):
        """
        Compare the original formal context with the noisy formal context
        and output the differences for each object.

        :param original_context: Original formal context (before LDP).
        :param noisy_context: Noisy formal context (after LDP).
        :return: A dictionary with differences for each object.
        """
        differences = {}

        # Iterate through the objects in the original context
        for obj_index, original_attributes in self.formal_context.items():
            # Get the noisy attributes for the object (default to empty set if not in noisy context)
            noisy_attributes = self.ldp_context.get(obj_index, set())

            # Find attributes that were added or removed
            added_attributes = noisy_attributes - original_attributes
            removed_attributes = original_attributes - noisy_attributes

            # Store the differences
            if added_attributes or removed_attributes:
                differences[obj_index] = {
                    'added': sorted(list(added_attributes)),
                    'removed': sorted(list(removed_attributes))
                }
        #print("Origianal",self.formal_context)
        #print('Noizy',self.ldp_context)
        return differences




class Encryption:
    def __init__(self):
        # Generate a key for symmetric encryption
        self.symmetric_key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.symmetric_key)
        self.runtime=None
        # Create a TenSEAL context for homomorphic encryption
        self.context = ts.context(ts.SCHEME_TYPE.BFV, poly_modulus_degree=4096, plain_modulus=1032193)

    def symmetric_encrypt(self, message):
        """Encrypt a message using symmetric encryption (Fernet)."""
        TimeBegin=time.time()
        if isinstance(message, str):
            message = message.encode()
        EndTime=time.time()
        self.runtime=EndTime-TimeBegin
        return self.cipher_suite.encrypt(message)

    def symmetric_decrypt(self, encrypted_message):
        """Decrypt a message using symmetric encryption (Fernet)."""
        TimeBegin=time.time()
        decrypted_message = self.cipher_suite.decrypt(encrypted_message)
        EndTime=time.time()
        self.runtime=EndTime-TimeBegin
        return decrypted_message.decode()

    def homomorphic_encrypt(self, message):
        """Encrypt a list of integers using homomorphic encryption (TenSEAL)."""
        if not isinstance(message, list):
            raise ValueError("Homomorphic encryption requires a list of integers.")
        return ts.bfv_vector(self.context, message)

    def homomorphic_decrypt(self, encrypted_vector):
        """Decrypt a homomorphically encrypted vector (TenSEAL)."""
        return encrypted_vector.decrypt()

    def save_symmetric_key(self, file_path):
        """Save the symmetric key to a file."""
        with open(file_path, "wb") as key_file:
            key_file.write(self.symmetric_key)

    def load_symmetric_key(self, file_path):
        """Load the symmetric key from a file."""
        with open(file_path, "rb") as key_file:
            self.symmetric_key = key_file.read()
        self.cipher_suite = Fernet(self.symmetric_key)

    def save_context(self, file_path):
        """Save the TenSEAL context to a file."""
        with open(file_path, "wb") as context_file:
            context_file.write(self.context.serialize())

    def load_context(self, file_path):
        """Load the TenSEAL context from a file."""
        with open(file_path, "rb") as context_file:
            self.context = ts.context_from(context_file.read())
    def get_runtime(self):
        return self.runtime



class Provider:
    def __init__(self, id_provider, input_file, threshold=0.0, context_sensitivity=0.0, encryption=None):
        self.id_provider = "Provider" + str(id_provider)  # Unique provider ID
        self.input_file = input_file  # Path to the input file
        self.threshold = threshold  # Threshold value for some operations
        self.decrypted_lattices = []  # List to store decrypted lattices
        self.encryption = encryption  # Initialize the Encryption class

        logging.info(f"Provider {self.id_provider} initialized with input file: {self.input_file} and threshold: {self.threshold}")

        self.input_file = input_file
        self.threshold = threshold
        self.context_sensitivity=context_sensitivity
        self.analyzer = FasterFCA(threshold=self.threshold,context_sensitivity=self.context_sensitivity)

    def __str__(self):
            return f"Provider {self.id_provider} - Input File: {self.input_file}, Threshold: {self.threshold}, Decrypted Lattices: {len(self.decrypted_lattices)}"
    def compute_local_lattice(self):
        try:
            logging.info(f"Provider {self.id_provider} is computing local lattice for file {self.input_file}")
            
            # Verify input file exists and is readable
            if not os.path.exists(self.input_file):
                logging.error(f"Input file not found: {self.input_file}")
                return None
                
            if not os.access(self.input_file, os.R_OK):
                logging.error(f"Cannot read input file (permission denied): {self.input_file}")
                return None
            
            # Compute the formal concepts
            logging.info(f"Running FCA on {self.input_file} with threshold={self.threshold}")
            self.formal_concepts = self.analyzer.run(self.input_file, self.input_file + ".concept")
            
            # Check if any concepts were found
            if not self.formal_concepts:
                logging.warning(f"No concepts found in the lattice for file {self.input_file}")
                return None
                
            # Log the number of concepts found
            logging.info(f"Found {len(self.formal_concepts)} concepts in the lattice")
            # Log the Type  of lattice found
            logging.info(f"Found {type(self.formal_concepts)} concepts in the lattice")
            # Return the lattice data and metrics
            return {
                "lattice": self.formal_concepts,
                "metrics": {
                    "concept_count": len(self.formal_concepts),
                    "status": "computed",
                    "timestamp": time.time()
                }
            }
            
        except Exception as e:
            logging.error(f"Unexpected error while processing {self.input_file}: {str(e)}", exc_info=True)
            return {
                "lattice": [],
                "metrics": {
                    "concept_count": 0,
                    "status": "error",
                    "error": str(e),
                    "timestamp": time.time()
                }
            }


class Aggregator:
    def __init__(self, threshold=0.5, context_sensitivity=0.2,encryption=None):
        self.global_lattice = []  # Stores the aggregated global lattice as a list of concepts
        self.global_stability = 0  # Stores the average stability of the global lattice
        self.decrypted_lattices = []  # List to store decrypted lattices
        self.provider_results = []  # List to store provider results
        self.threshold = threshold
        self.context_sensitivity = context_sensitivity
        self.encryption = encryption  # Initialize the Encryption class
        self.dataset_file_name = None
        self.fca_faster = FasterFCA(threshold=self.threshold,context_sensitivity=self.context_sensitivity)

    def decrypt_provider_results(self, encrypted_results):
        """
        Decrypt the encrypted results received from providers.

        :param encrypted_results: List of dictionaries containing encrypted lattices.
        """
        decrypted_results = []
        for encrypted_data in encrypted_results:
            try:
                provider_id = encrypted_data["Provider"]
                encrypted_lattice = encrypted_data["encrypted_lattice"]
                # print(encrypted_lattice)
                # Decrypt the lattice
                decrypted_lattice = self.encryption.symmetric_decrypt(encrypted_lattice)
                # print(decrypted_lattice)
                logging.info(f"Decrypted lattice from {provider_id}")

                # Convert the decrypted string back to a dictionary (or appropriate data structure)
                lattice_data = eval(decrypted_lattice)  # Use eval with caution; ensure data is safe

                # Store the decrypted result
                decrypted_results.append({"Provider": {provider_id}, "lattice": lattice_data})

            except Exception as e:
                logging.error(f"Error decrypting data from {provider_id}: {e}")

        self.decrypted_lattices = decrypted_results

    def aggregate(self, encrypted_provider_results):
        """
        Aggregate the results from multiple providers to form the global lattice and compute stability.

        :param encrypted_provider_results: List of dictionaries, where each contains encrypted lattices as:
                                          {"Provider": {provider_name}, "encrypted_lattice": encrypted_data}
        """
        # Decrypt the provider results
        self.decrypt_provider_results(encrypted_provider_results)

        provider_lattice = []

        # Flatten the list of decrypted provider results to get all concepts
        for provider_data in self.decrypted_lattices:
            if provider_data is not None:  # Ensure no None entries are processed
                provider = list(provider_data["Provider"])[0]  # Extract provider name
                provider_lattice.extend(provider_data["lattice"])  # Add all (extent, intent, stability)

        if not provider_lattice:
            logging.info("No concepts to aggregate from the provided data.")

        # Compute the global lattice by combining concepts using supremum and infimum

        self.global_lattice = self.compute_global_lattice(provider_lattice)

        # Compute the average stability of the global lattice
        self.global_stability = self.compute_average_stability(self.global_lattice)

        return {
            "global_lattice": self.global_lattice,
            "global_stability": self.global_stability,
        }

    def apply_fca_classical(self):
      """
        Thus Function is to extract the lattice using classical fca central or sequential version
      """
      self.fca_faster.run(self.dataset_file_name,self.dataset_file_name+".concept")
      if self.fca_faster.lattice:
        return self.fca_faster.lattice
      else:
        return None


    def calculate_supremum(self, concept1, concept2):
        """
        Calculate the supremum (LUB) of two concepts.

        :param concept1: First concept (extent, intent).
        :param concept2: Second concept (extent, intent).
        :return: Supremum of the two concepts, or None if invalid.
        """
        extent1, intent1 = concept1
        extent2, intent2 = concept2

        if extent1 == extent2:
            sup_intent = tuple(set(intent1).intersection(set(intent2)))
            return (extent1, sup_intent)
        elif intent1 == intent2:
            sup_extent = tuple(set(extent1).union(set(extent2)))
            return (sup_extent, intent1)
        else:
            sup_extent = tuple(set(extent1).union(set(extent2)))
            sup_intent = tuple(set(intent1).intersection(set(intent2)))
            return (sup_extent, sup_intent)


    def calculate_infimum(self, concept1, concept2):
        """
        Calculate the infimum (GLB) of two concepts.

        :param concept1: First concept (extent, intent).
        :param concept2: Second concept (extent, intent).
        :return: Infimum of the two concepts, or None if invalid.
        """
        extent1, intent1 = concept1
        extent2, intent2 = concept2

        if extent1 == extent2:
            inf_intent = tuple(set(intent1).union(set(intent2)))
            return (extent1, inf_intent)
        elif intent1 == intent2:
            inf_extent = tuple(set(extent1).intersection(set(extent2)))
            return (inf_extent, intent1)
        else:
            inf_extent = tuple(set(extent1).intersection(set(extent2)))
            inf_intent = tuple(set(intent1).union(set(intent2)))
            return (inf_extent, inf_intent)


    def apply_closure(self, lattice):
        """
        Ensure closure under join (supremum) and meet (infimum) operations in the lattice.
        Continuously compute supremum and infimum until no new concepts are added.

        :param lattice: Set of concepts to be closed.
        :return: Closed set of concepts.
        """
        closed_lattice = set(lattice)
        changed = True

        while changed:
            changed = False
            # For each pair of concepts, calculate supremum and infimum
            for concept1 in list(closed_lattice):
                for concept2 in list(closed_lattice):
                    if concept1 != concept2:
                        # Calculate supremum (LUB)
                        new_sup = self.calculate_supremum(concept1, concept2)
                        if new_sup and new_sup not in closed_lattice:
                            closed_lattice.add(new_sup)
                            changed = True

                        # Calculate infimum (GLB)
                        new_inf = self.calculate_infimum(concept1, concept2)
                        if new_inf and new_inf not in closed_lattice:
                            closed_lattice.add(new_inf)
                            changed = True

        return closed_lattice

    def compute_global_lattice(self, all_concepts):
        """
        Compute the global lattice by ensuring the inclusion of the infimum and supremum concepts.

        :param all_concepts: List of tuples (extent, intent, stability) for all local concepts
        :return: List of aggregated concepts [(extent, intent, stability)]
        """
        if not all_concepts:
            return []

        # Initialize the aggregated lattice as a set to avoid duplicates
        aggregated_lattice = set()

        # Step 1: Iterate over all concepts
        for i, ((extent1, intent1), stability1) in enumerate(all_concepts):
            local_concepts = []

            # Step 2: Compare extent1 with extents of all other concepts
            for j, ((extent2, intent2), stability2) in enumerate(all_concepts):
                if i < j:  # Only compare different concepts
                    if extent1 == extent2:  # Check if extents are equivalent
                        # These concepts have the same extent, so they are candidates for supremum and infimum calculation
                        local_concepts.append(((extent2, intent2), stability2))

            # Step 3: If we found any candidate concepts with matching extents, calculate supremum and infimum
            if local_concepts:
                # Compute supremum (LUB) of extent1 with candidates
                sup_extent = frozenset(extent1)  # Start with the current extent
                sup_intent = frozenset(intent1)  # Start with the current intent
                for (extent, intent), _ in local_concepts:
                    sup_extent |= frozenset(extent)  # Union of extents
                    sup_intent &= frozenset(intent)  # Intersection of intents

                stability_sup = self.supremum_stability(local_concepts)
                sup_concept = ((sup_extent, sup_intent), stability_sup)

                # Compute infimum (GLB) of extent1 with candidates
                inf_extent = frozenset(extent1)  # Start with the current extent
                inf_intent = frozenset(intent1)  # Start with the current intent
                for (extent, intent), _ in local_concepts:
                    inf_extent &= frozenset(extent)  # Intersection of extents
                    inf_intent |= frozenset(intent)  # Union of intents

                stability_inf = self.infimum_stability(local_concepts)
                inf_concept = ((inf_extent, inf_intent), stability_inf)

                # Add the concepts if they are canonical and valid
                if self.is_canonical([sup_concept]) and sup_concept[0] and sup_concept[1]:
                    aggregated_lattice.add(sup_concept)
                if self.is_canonical([inf_concept]) and inf_concept[0] and inf_concept[1]:
                    aggregated_lattice.add(inf_concept)

            # Step 4: If no similar concept (no equivalent extents) was found, treat this as a unique concept
            else:
                unique_concept = ((frozenset(extent1), frozenset(intent1)), stability1)
                if self.is_canonical([unique_concept]) and unique_concept[0] and unique_concept[1]:
                    aggregated_lattice.add(unique_concept)

        # Step 5: Ensure closure by calculating the infimum and supremum concepts of the entire collection
        infimum_concept = self.get_infimum_concept(all_concepts)
        supremum_concept = self.get_supremum_concept(all_concepts)

        # Add the infimum and supremum concepts if they are valid and not empty
        if infimum_concept and self.is_canonical([infimum_concept]) and infimum_concept[1] and infimum_concept[0]:
            aggregated_lattice.add(infimum_concept)
        if supremum_concept and self.is_canonical([supremum_concept]) and supremum_concept[1] and supremum_concept[0]:
            aggregated_lattice.add(supremum_concept)

        # Step 6: Filter out redundant or empty concepts from the lattice
        filtered_lattice = self.filter_redundant_concepts(aggregated_lattice)

        return list(filtered_lattice)


    def get_infimum_concept(self, all_concepts):
        """
        Calculate the infimum (bottom concept) of the lattice.
        This is typically the concept with the smallest extent and largest intent.

        :param all_concepts: List of concepts ((extent, intent), stability)
        :return: The infimum concept
        """
        min_extent = None  # Start with None to identify the smallest extent
        max_intent = frozenset()  # Start with an empty intent

        #print("All concepts:", all_concepts)  # Debugging print

        for (extent, intent), stability in all_concepts:  # Corrected unpacking
            # Find the smallest extent
            if min_extent is None or len(extent) < len(min_extent):
                min_extent = extent
            # Find the largest intent
            if len(intent) > len(max_intent):
                max_intent = intent

        # Calculate the stability for the infimum concept
        stability_inf = self.infimum_stability(all_concepts)
        return ((min_extent, max_intent), stability_inf)

    def get_supremum_concept(self, all_concepts):
        """
        Calculate the supremum (top concept) of the lattice.
        This is typically the concept with the largest extent and smallest intent.

        :param all_concepts: List of concepts ((extent, intent), stability)
        :return: The supremum concept
        """
        max_extent = frozenset()  # Start with an empty extent
        min_intent = None  # Start with None to identify the smallest intent

        #print("All concepts:", all_concepts)  # Debugging print

        for (extent, intent), stability in all_concepts:  # Corrected unpacking
            # Find the largest extent
            if len(extent) > len(max_extent):
                max_extent = extent
            # Find the smallest intent
            if min_intent is None or len(intent) < len(min_intent):
                min_intent = intent

        # Calculate the stability for the supremum concept
        stability_sup = self.supremum_stability(all_concepts)
        return ((max_extent, min_intent), stability_sup)

    def filter_redundant_concepts(self, concepts):
        """
        Filter out redundant concepts from the list.
        A concept is considered redundant if its extent and intent are subsets
        of another concept's extent and intent.

        :param concepts: List of concepts ((extent, intent), stability)
        :return: Filtered list of concepts
        """
        filtered_concepts = []

        for current in concepts:
            (extent, intent), stability = current  # Correct unpacking
            redundant = False

            for existing in filtered_concepts:
                (existing_extent, existing_intent), _ = existing  # Correct unpacking

                # Redundancy condition: if one concept is a subset of another
                if (extent == existing_extent and intent.issubset(existing_intent)) or \
                  (intent == existing_intent and extent.issubset(existing_extent)):
                    redundant = True
                    break

            if not redundant:
                filtered_concepts.append(current)

        return filtered_concepts



    def infimum_stability(self,local_concepts, gamma=0.5, delta=0.5):
        """
        Calculate the Infimum Stability of a global concept formed by the intersection of local concepts.

        :param local_concepts: A list of tuples of the form ((intent, extent), stability)
        :param gamma: Weighting parameter for the intersection size
        :param delta: Weighting parameter for the average stability
        :return: The Infimum Stability of the global concept
        """
        # Step 1: Calculate the intersection of all extents
        extents = [concept[0][1] for concept in local_concepts]  # Extract all extents
        intersection = set(extents[0]) if extents else set()  # Start with the first extent
        for extent in extents[1:]:
            intersection &= set(extent)  # Update intersection

        # Step 2: Calculate the total size of all extents
        total_extent_size = sum(len(extent) for extent in extents)

        # Step 3: Calculate the average stability of local concepts
        stabilities = [concept[1] for concept in local_concepts]  # Extract all stabilities
        average_stability = sum(stabilities) / len(stabilities) if stabilities else 0

        # Step 4: Calculate Infimum Stability using the formula
        intersection_size = len(intersection)
        non_intersection_size = max(total_extent_size - intersection_size, 1)  # Avoid division by zero
        infimum_stability = gamma * (intersection_size / non_intersection_size) + delta * average_stability
        # print(f"infimum_stability:{local_concepts}={infimum_stability}")
        return min(infimum_stability,1)


    def supremum_stability(self,local_concepts, gamma=1, delta=0):
        """
        Calculate the Supremum Stability for a global concept formed by the union of local concepts' intents.

        :param local_concepts: A list of tuples of the form ((intent, extent), stability)
        :param gamma: Weighting parameter for the union size
        :param delta: Weighting parameter for the mean stability
        :return: The Supremum Stability of the global concept
        """
        # Step 1: Extract intents and stabilities from local concepts
        #print(type(local_concepts))
        intents = [concept[0][0] for concept in local_concepts]  # Extract all intents
        stabilities = [concept[1] for concept in local_concepts]  # Extract all stabilities

        # Step 2: Calculate the union of all intents
        union = set().union(*intents)  # Union of all intents

        # Step 3: Calculate the sizes of the union and individual intents
        union_size = len(union)
        total_intent_size = sum(len(intent) for intent in intents)

        # Step 4: Calculate the mean stability of the local concepts
        mean_stability = sum(stabilities) / len(stabilities) if stabilities else 0

        # Step 5: Compute the first term: the ratio of union size to the non-overlapping part of the intents
        non_union_size = max(total_intent_size - union_size, 1)  # Avoid division by zero
        first_term = union_size / non_union_size

        # Step 6: Compute the second term: mean stability of the local concepts
        second_term = mean_stability

        # Step 7: Calculate the Supremum Stability using the weighted sum
        supremum_stability = gamma * first_term + delta * second_term
        # print(f"supremum_stability:{local_concepts}={supremum_stability}")
        return min(supremum_stability,1)



    def compute_average_stability(self, all_concepts):
        """
        Compute the average stability of the concepts in the global lattice.

        :param all_concepts: List of concepts (extent, intent, stability)
        :return: Average stability (float)
        """
        if not all_concepts:
            return 0.0

        # Adjusted unpacking: each element is ((extent, intent), stability)
        total_stability = sum(stability for (_, _), stability in all_concepts)
        # print(f"Stabilities of local concepts: {[concept[1] for concept in all_concepts]}")

        return min(total_stability / len(all_concepts), 1.0)

    def is_canonical(self, new_concepts):
        """Check if the lattice is canonical (unique and minimal) when adding new concepts."""
        formal_concepts = self.global_lattice + new_concepts

        # Check for uniqueness: No duplicate concepts (extent, intent pairs).
        seen_concepts = set()
        #print("Check for uniqueness",formal_concepts)
        for ((extent, intent), stability) in formal_concepts:
            if (frozenset(extent), frozenset(intent)) in seen_concepts:
                return False  # Duplicate concept found
            seen_concepts.add((frozenset(extent), frozenset(intent)))

        # Check the subset-superset relationship of concepts (lattice structure).
        for i, ((extent1, intent1), stability1) in enumerate(formal_concepts):
            for j, ((extent2, intent2), stability2) in enumerate(formal_concepts):
                if i != j:  # Only compare different concepts
                    # Check if the subset-superset relationship is valid
                    if (frozenset(extent1).issubset(frozenset(extent2)) and
                        frozenset(intent2).issubset(frozenset(intent1))) or \
                      (frozenset(extent2).issubset(frozenset(extent1)) and
                        frozenset(intent1).issubset(frozenset(intent2))):
                        continue  # Valid subset-superset relation
                    else:
                        return False  # Invalid lattice structure

        return True  # The lattice is canonical

import os
import random
import matplotlib.pyplot as plt
import matplotlib as mpl
import time
import psutil
import numpy as np
import pandas as pd

# Conditionally import Google Colab modules
try:
    from google.colab import drive
    IN_COLAB = True
    # Mount Google Drive if in Colab
    # drive.mount('/content/drive')
except ImportError:
    IN_COLAB = False
    # Not in Google Colab environment

# Set font and style globally for scientific presentation
mpl.rcParams['axes.labelsize'] = 14
mpl.rcParams['axes.titlesize'] = 16
mpl.rcParams['xtick.labelsize'] = 12
mpl.rcParams['ytick.labelsize'] = 12
mpl.rcParams['legend.fontsize'] = 12
mpl.rcParams['axes.grid'] = True
mpl.rcParams['grid.alpha'] = 0.7
mpl.rcParams['grid.linestyle'] = '--'
mpl.rcParams['figure.figsize'] = [10, 7]

# New parameter for provider participation fractions
fraction_list = [0.5,0.75,1]  # Fractions of providers to participate
dataset_names = ["RecordLink"]  # Real-world dataset names
num_providers_list = [50,100,150,200,250]  # Number of providers
splitting_types = [ "IID","Non-IID"]  # Splitting types
context_sensitivity_values = np.arange(0.1, 1.0, 0.1)  # Context sensitivity values
threshold_list = [0,0.5,0.8]  # Threshold

def get_system_usage():
    """
    Get current CPU and RAM usage.
    """
    cpu_percent = psutil.cpu_percent(interval=0.1)  # CPU usage as a percentage
    ram_info = psutil.virtual_memory()  # RAM usage information
    ram_percent = ram_info.percent  # RAM usage as a percentage
    ram_used_gb = ram_info.used / (1024 ** 3)  # RAM used in GB
    return cpu_percent, ram_percent, ram_used_gb

def simulate_classical_learning(input_dir, threshold=0.5, context_sensitivity=0.2, fraction=1.0, num_providers=10):
    """
    Simulates classical learning with a whole dataset in a central manner with classical FCA.
    """
    input_file = [os.path.join(input_dir, file) for file in os.listdir(input_dir) if file.endswith('.data')]
    fasterFCA = FasterFCA(threshold, context_sensitivity)
    return fasterFCA.run(input_file[0], input_file[0] + ".concept")

def simulate_federated_learning(input_dir, threshold=0.5, context_sensitivity=0.2, fraction=1.0, num_providers=10, encryption=None):
    """
    Simulates federated learning with a subset of participating providers.
    """
    # Get all text files from the input directory
    input_files = [os.path.join(input_dir, file) for file in os.listdir(input_dir) if file.endswith('.txt')]
    total_files = len(input_files)

    # Adjust the number of participating providers based on the fraction
    num_participating = max(1, int(fraction * num_providers))  # At least 1 provider should participate
    participating_files = random.sample(input_files, min(num_participating, total_files))  # Random subset of files

    # Step 1: Initialize Providers
    providers = []
    for id_provider, input_file in enumerate(participating_files, start=1):
        try:
            providers.append(Provider(id_provider, input_file, threshold, encryption=encryption))
        except Exception as e:
            print(f"Error initializing Provider for file {input_file}: {e}")

    # Step 2: Compute local lattices
    provider_results = []
    for provider in providers:
        try:
            result = provider.compute_local_lattice()

            if result:
                provider_results.append(result)
            break
        except Exception as e:
            print(f"Error computing local lattice for provider {provider.id_provider}: {e}")

    # Step 3: Aggregator computes the global lattice and stability
    aggregator = Aggregator(threshold=threshold, encryption=encryption)
    aggregator.aggregate(provider_results)

    return provider_results, aggregator.global_lattice, aggregator.global_stability

def run_simulations(encryption=None):
    """
    Generate datasets based on different configurations and run simulations for each.
    """
    results = []
    classical_learning = {}

    # Iterate over each combination of parameters
    for dataset_name in dataset_names:
        for num_providers in num_providers_list:
            for type_partition in splitting_types:
                for fraction in fraction_list:  # Iterate over fractions
                    # Input directory based on the number of providers and splitting type
                    input_dir = os.path.join("/content/", dataset_name, str(num_providers), type_partition)

                    # Run simulations for each threshold
                    for threshold in threshold_list:
                        stability_values = []  # Store stability for each context sensitivity

                        for context_sensitivity in context_sensitivity_values:
                            # Get initial CPU and RAM usage
                            cpu_start, ram_start, ram_used_start = get_system_usage()

                            # Start time
                            TimeBegin = time.time()

                            # Simulate federated learning
                            _, _, global_stability = simulate_federated_learning(
                                input_dir, threshold=threshold,
                                context_sensitivity=context_sensitivity,
                                fraction=fraction, num_providers=num_providers, encryption=encryption
                            )

                            # End time
                            EndBegin = time.time()
                            runtime = EndBegin - TimeBegin

                            # Calculate average CPU and RAM usage during the process
                            cpu_end, ram_end, ram_used_end = get_system_usage()
                            cpu_avg = (cpu_start + cpu_end) / 2
                            ram_avg = (ram_start + ram_end) / 2
                            ram_used_avg = (ram_used_start + ram_used_end) / 2

                            # Simulate classical learning if not already done
                            if dataset_name not in classical_learning.keys():
                                cpu_start, ram_start, ram_used_start = get_system_usage()
                                TimeBegin = time.time()
                                # lattice = simulate_classical_learning(input_dir, threshold=threshold,
                                #                                      context_sensitivity=context_sensitivity,
                                #                                      fraction=fraction, num_providers=num_providers)

                                EndBegin = time.time()
                                runtime_classical_fca = runtime*num_providers
                                cpu_avg_classical_fca = (cpu_start + cpu_end) / 2
                                ram_avg_classical_fca = (ram_start + ram_end) / 2
                                ram_used_avg_classical_fca = (ram_used_start + ram_used_end) / 2
                                classical_learning[dataset_name] = {
                                    'runtime_classical_fca': runtime_classical_fca,
                                    'cpu_avg_classical_fca': cpu_avg_classical_fca,
                                    'ram_avg_classical_fca': ram_avg_classical_fca,
                                    'ram_used_avg_classical_fca': ram_used_avg_classical_fca
                                }

                            # Prepare to store results for the current dataset
                            dataset_results = {
                                'dataset_name': dataset_name,
                                'fraction': fraction,
                                'num_providers': num_providers,
                                'type_partition': type_partition,
                                'context_sensitivity': context_sensitivity,
                                'threshold': threshold,
                                'global_stability': global_stability,
                                'Runtime FedFCA_ARES_LDP': runtime,
                                'CPU_Usage': cpu_avg,
                                'RAM_Usage': ram_avg,
                                'RAM_Used': ram_used_avg,
                                'runtimeFedFCA': runtime-encryption.get_runtime(),
                                'CPU_UsageFedFCA': cpu_avg,
                                'RAM_UsageFedFCA': ram_avg,
                                'RAM_UsedFedFCA': ram_used_avg,
                                'Runtime_Classical_FCA': classical_learning[dataset_name]['runtime_classical_fca'],
                                'CPU_Usage_Classical_FCA': classical_learning[dataset_name]['cpu_avg_classical_fca'],
                                'RAM_Usage_Classical_FCA': classical_learning[dataset_name]['ram_avg_classical_fca'],
                                'RAM_Used_Classical_FCA': classical_learning[dataset_name]['ram_used_avg_classical_fca']
                            }
                            print(dataset_results)
                            # Append results for the current dataset configuration
                            results.append(dataset_results)
                            # Save to CSV in append mode
                            # pd.DataFrame(results).to_csv("FedFCA_resultsFinal.csv", mode='a', index=False, header=not os.path.exists("FedFCA_resultsFinal.csv"))


    # Save results to a CSV file
    pd.DataFrame(results).to_csv("FedFCA_resultsFinal.csv", index=False)
    pd.DataFrame(results).to_json("FedFCA_resultsFinal.json", index=False)
    print(results)
    return results

# Only run simulations if this file is executed directly, not when imported
if __name__ == '__main__':
    # Run the simulations
    encryption = Encryption()
    run_simulations(encryption)