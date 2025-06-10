import logging
import concurrent.futures
from itertools import combinations, chain
import networkx as nx
import matplotlib.pyplot as plt # Keep for StabilityCalculator if plotting is used
import uuid
import numpy as np
import seaborn as sns # Keep for StabilityCalculator if plotting is used
# from sklearn.decomposition import PCA # Not used in the provided FedFCA.core.py snippet
# from sklearn.mixture import GaussianMixture # Not used
from collections import defaultdict
import json
import random
import sys
import math
import ast
import time # Ensure time is imported
import os # Ensure os is imported
import psutil # Ensure psutil is imported
import pandas as pd # Ensure pandas is imported

from cryptography.fernet import Fernet
import tenseal as ts # If HE is to be developed further

# --- Configuration ---
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper for JSON Serialization ---
def json_serializer_helper(obj):
    """Helper function to serialize objects not natively supported by JSON, like frozenset."""
    if isinstance(obj, frozenset):
        return sorted(list(obj)) # Convert frozenset to sorted list
    if isinstance(obj, set):
        return sorted(list(obj)) # Convert set to sorted list
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

class LDPHandler:
    """
    Handles Local Differential Privacy (LDP) mechanisms for formal contexts.
    Applies noise to attribute sets to protect individual data points.
    """
    def __init__(self, formal_context, context_sensitivity):
        """
        Initializes the LDPHandler.

        Args:
            formal_context (dict): The original formal context {object_id: {attributes}}.
            context_sensitivity (float): A parameter influencing the privacy budget (epsilon).
                                         Value typically between 0 and 1.
        """
        self.formal_context = formal_context
        self.context_sensitivity = context_sensitivity
        self.ldp_context = {}  # Stores the noisy (LDP) context
        self.runtime = 0.0
        # It's generally better to seed random once at the start of the script for reproducibility during testing,
        # or not at all for production to ensure different noise each time.
        # random.seed(42) # Example: for reproducible tests

    def randomized_response(self, original_value_present, epsilon):
        """
        Applies randomized response to a boolean value (presence of an attribute).

        Note: This is one specific implementation of randomized response.
        The probability 'p' determines how likely the original value is kept.
        Standard RR for binary attributes often uses p = exp(epsilon) / (exp(epsilon) + 1).

        Args:
            original_value_present (bool): True if the attribute is originally present, False otherwise.
            epsilon (float): The privacy budget. Higher epsilon means less privacy, more utility.

        Returns:
            bool: The noisy value (True or False).
        """
        if epsilon <= 0: # Epsilon must be positive
            return original_value_present

        # Probability of reporting truthfully
        # This specific formula for p might need to be justified based on the privacy model.
        # A common formulation is p = math.exp(epsilon) / (math.exp(epsilon) + 1)
        # The current one: p = 1 / (1 + 1 / (epsilon + 1e-10)) simplifies to epsilon / (epsilon + 1)
        # if epsilon is small, or (epsilon+1e-10)/(epsilon+1e-10+1)
        # Let's use a more standard p for clarity, or ensure the current one is theoretically sound.
        # For attribute perturbation (keeping or flipping):
        # Prob of keeping original: e^epsilon / (e^epsilon + 1)
        # Prob of flipping: 1 / (e^epsilon + 1)

        prob_true_if_true = math.exp(epsilon) / (math.exp(epsilon) + 1)
        prob_true_if_false = 1 / (math.exp(epsilon) + 1)

        if original_value_present:
            return random.random() < prob_true_if_true
        else:
            return random.random() < prob_true_if_false

    def apply_aldp_to_formal_context(self):
        """
        Applies Adaptive LDP to the formal context by adding noise to attribute sets.
        This implementation perturbs the presence of each attribute for each object.

        The privacy budget 'epsilon' is derived from `self.context_sensitivity`.
        A higher `context_sensitivity` leads to a higher epsilon (less noise).
        """
        start_time = time.time()
        self.ldp_context = {} # Reset LDP context

        # Determine the universe of all possible attributes for consistent perturbation
        all_possible_attributes = set()
        if not self.formal_context:
            logging.warning("LDPHandler: Formal context is empty. No LDP will be applied.")
            self.runtime = time.time() - start_time
            return self.ldp_context

        for attributes in self.formal_context.values():
            all_possible_attributes.update(attributes)

        if not all_possible_attributes:
            logging.warning("LDPHandler: No attributes found in the formal context.")
            # Still process objects, they will just have empty attribute sets
            for obj_id in self.formal_context:
                 self.ldp_context[obj_id] = set()
            self.runtime = time.time() - start_time
            return self.ldp_context


        # Epsilon mapping: Higher sensitivity -> higher epsilon (less noise)
        # This mapping (1 to 10) should be justified.
        epsilon = 1.0 + 9.0 * self.context_sensitivity
        if epsilon <=0:
            logging.warning(f"LDPHandler: Epsilon is {epsilon}, LDP will effectively not flip bits or keep original. Check context_sensitivity.")


        for obj_id, original_attributes in self.formal_context.items():
            noisy_attributes = set()
            for attribute in all_possible_attributes:
                is_present_original = attribute in original_attributes
                # Apply randomized response for each attribute in the universe
                if self.randomized_response(is_present_original, epsilon):
                    noisy_attributes.add(attribute)
            self.ldp_context[obj_id] = noisy_attributes

        self.runtime = time.time() - start_time
        logging.info(f"LDP applied. Epsilon: {epsilon:.2f}, Runtime: {self.runtime:.4f}s")
        return self.ldp_context

    def get_runtime(self):
        """Returns the runtime of the last LDP application."""
        return self.runtime

class StabilityCalculator:
    """
    Calculates the stability of formal concepts.
    The stability measure used here needs to be clearly defined and referenced.
    """
    def __init__(self, formal_context):
        """
        Initializes the StabilityCalculator.

        Args:
            formal_context (dict): The formal context {object_id: {attributes}}
                                   on which stability will be calculated.
        """
        self.formal_context = formal_context if formal_context else {}
        self.objects = set(self.formal_context.keys())
        if not self.formal_context:
            logging.warning("StabilityCalculator initialized with an empty formal context.")

    def _get_intent(self, extent_set):
        """Helper to get the common attributes (intent) for a given set of objects (extent)."""
        if not extent_set or not self.formal_context:
            return frozenset()
        
        # Ensure all objects in extent_set are in the formal_context
        valid_extent_objects = [obj for obj in extent_set if obj in self.formal_context]
        if not valid_extent_objects:
            return frozenset()

        # Start with attributes of the first valid object
        common_attributes = set(self.formal_context[valid_extent_objects[0]])
        for obj in valid_extent_objects[1:]:
            common_attributes.intersection_update(self.formal_context[obj])
        return frozenset(common_attributes)

    def support(self, subset_objects=None, subset_attributes=None):
        """
        Calculates support. This version is split for clarity.
        """
        if subset_objects is not None: # Calculate support for an extent (set of objects)
            if not self.formal_context or not subset_objects:
                return 0
            # Support of an extent is the size of its intent
            return len(self._get_intent(subset_objects))

        if subset_attributes is not None: # Calculate support for an intent (set of attributes)
            if not self.formal_context or not subset_attributes:
                return 0
            # Support of an intent is the size of its extent
            count = 0
            for obj_attributes in self.formal_context.values():
                if subset_attributes.issubset(obj_attributes):
                    count += 1
            return count
        return 0


    def build_max_nongen(self, objects_to_consider, attribute_set_size_of_intent):
        """
        Builds the maximal non-generator set (β).
        This is part of a specific stability calculation algorithm.
        """
        max_nongen = set()
        # Iterate objects in a defined order (e.g., sorted reverse by ID)
        for obj in sorted(list(objects_to_consider), reverse=True):
            # Check if adding 'obj' to 'max_nongen' changes the support (intent size)
            # The support here refers to the size of the common attributes for the set {max_nongen U {obj}}
            if self.support(subset_objects=(max_nongen | {obj})) != attribute_set_size_of_intent:
                max_nongen.add(obj)
        return max_nongen

    def compute_generators(self, obj, alpha, beta, attribute_set_size_of_intent):
        """
        Computes the number of generators for a specific object.
        Part of a specific stability algorithm.
        """
        min_generators_pairs = set() # Store pairs that form minimal generators with obj

        # Consider objects from beta and alpha (excluding obj itself)
        for other_obj in (beta | alpha - {obj}):
            # Check if {obj, other_obj} has the same intent size as the concept's intent
            if self.support(subset_objects={obj, other_obj}) == attribute_set_size_of_intent:
                min_generators_pairs.add(other_obj) # other_obj helps form a minimal generator with obj

        # The formula sum(2**(len(min_generators_pairs) - i)) for i in range(len(min_generators_pairs))
        # simplifies to 2**len(min_generators_pairs) - 1 if len > 0, or 0 if len == 0.
        # This counts subsets of min_generators_pairs to combine with obj.
        if not min_generators_pairs:
            return 0
        return (1 << len(min_generators_pairs)) -1 # 2^k - 1 (contribution from obj with subsets of its min_generator_partners)


    def handle_object(self, obj, current_alpha, current_beta, attribute_set_size_of_intent):
        """
        Handles stability calculation for a single object within the context of alpha/beta sets.
        """
        generator_count_for_obj = 0
        # This loop structure seems to refine alpha/beta iteratively for 'obj'.
        # The exact algorithm being implemented here needs citation for full understanding.
        temp_alpha = set(current_alpha) # Work with copies
        temp_beta = set(current_beta)

        while obj in temp_alpha: # Process obj as long as it's in the alpha set
            # Contribution of obj with current alpha/beta partitioning
            generator_count_for_obj += self.compute_generators(obj, temp_alpha, temp_beta, attribute_set_size_of_intent)

            # Update alpha and beta based on obj's removal or processing
            # This part of the algorithm is complex and specific.
            # Assuming 'update_alpha_beta' is meant to re-partition remaining elements
            # after considering 'obj' or elements related to 'obj'.
            # The original 'update_alpha_beta' took 'non_generators'.
            # This needs to be consistent with the algorithm being implemented.
            # For now, let's assume we remove obj from alpha and re-evaluate.
            temp_alpha.remove(obj)
            if not temp_alpha: # No more elements in alpha to process with obj
                break
            # Re-partitioning based on remaining elements (this is a guess at the intent)
            # temp_alpha, temp_beta = self.update_alpha_beta_internal(temp_alpha | temp_beta, attribute_set_size_of_intent)

        return generator_count_for_obj

    def update_alpha_beta_internal(self, objects_to_partition, attribute_set_size_of_intent):
        """
        Internal helper to partition a set of objects into new alpha and beta sets.
        An object goes to alpha if adding it to current beta doesn't change intent size, else to beta.
        """
        new_alpha, new_beta = set(), set()
        # Process objects in a defined order for consistency
        for obj_to_place in sorted(list(objects_to_partition)):
            if self.support(subset_objects=(new_beta | {obj_to_place})) == attribute_set_size_of_intent:
                new_alpha.add(obj_to_place)
            else:
                new_beta.add(obj_to_place)
        return new_alpha, new_beta

    def compute_stability_for_concept(self, extent, intent):
        """
        Computes the stability of a single formal concept.
        WARNING: The stability formula implemented here is complex and non-standard.
        It appears to be a variation of counting generators.
        The use of log scaling in the previous version was removed as its basis was unclear.
        This version attempts to follow the structure of the original `compute_stability_for_concept_old`.
        A clear citation or derivation for this stability measure is crucial.

        Args:
            extent (set or frozenset): The set of objects in the concept.
            intent (set or frozenset): The set of attributes in the concept.

        Returns:
            float: The stability value (0 to 1).
        """
        if not self.formal_context or not extent:
            return 0.0

        attribute_set_size_of_intent = len(intent)
        if attribute_set_size_of_intent == 0 and len(extent) == len(self.objects): # Top concept
             return 1.0 # Or a defined value for top/bottom
        if attribute_set_size_of_intent == 0 and not extent: # Empty concept
             return 0.0

        # Objects in the extent whose individual intent matches the concept's intent
        # These are "single object generators" for the intent.
        single_object_generators = {obj for obj in extent if self.support(subset_objects={obj}) == attribute_set_size_of_intent}

        # Initial stability contribution from single object generators
        # If k single object generators, they form 2^k - 1 non-empty subsets, each generating the intent.
        stability_contribution = 0
        if single_object_generators:
            stability_contribution = (1 << len(single_object_generators)) -1 # 2^k - 1

        # Objects in extent not in single_object_generators
        remaining_extent_objects = extent - single_object_generators

        # Build maximal non-generator set from remaining_extent_objects
        # These are objects that, even when added to beta, don't preserve the intent size.
        beta = self.build_max_nongen(remaining_extent_objects, attribute_set_size_of_intent)
        alpha = remaining_extent_objects - beta # Alpha contains objects that *do* preserve intent size when added to beta

        # Further stability contributions from objects in alpha
        # This part iterates through alpha, and for each obj_in_alpha, it seems to count
        # how many ways 'obj_in_alpha' can combine with subsets of other objects (from alpha and beta)
        # to form generators for the concept's intent.
        for obj_in_alpha in sorted(list(alpha)): # Process in defined order
            # The 'handle_object' logic is intricate.
            # It seems to recursively/iteratively find generator contributions.
            # The original 'handle_object' modified alpha/beta, which is complex.
            # Let's simplify to compute_generators for obj_in_alpha with current alpha/beta.
            # This might be an oversimplification of the original intent.
            stability_contribution += self.compute_generators(obj_in_alpha, alpha - {obj_in_alpha}, beta, attribute_set_size_of_intent)

        # Normalization: The sum of contributions is divided by 2^|Extent|.
        # This represents the ratio of subsets of Extent that generate the Intent.
        if not extent:
            return 0.0

        # Denominator can be very large.
        denominator = 1 << len(extent) # 2**len(extent)
        if denominator == 0: return 0.0 # Should not happen if extent is not empty

        stability = stability_contribution / denominator
        return min(round(stability, 3), 1.0) # Ensure stability is capped at 1.0

    def compute_stability_for_lattice(self, lattice_concepts):
        """
        Computes stability for all concepts in a given lattice.

        Args:
            lattice_concepts (list): A list of concepts, where each concept is ((extent, intent), stability_placeholder).
                                     The stability_placeholder will be ignored and recalculated.

        Returns:
            list: A list of tuples: ((extent, intent), computed_stability).
        """
        if not self.formal_context:
            logging.warning("StabilityCalculator: Cannot compute stability, formal context is not set or empty.")
            return [((c[0][0], c[0][1]), 0.0) for c in lattice_concepts]

        results_with_stability = []
        for concept_tuple in lattice_concepts:
            (extent, intent) = concept_tuple[0] # Concept is ((extent, intent), old_stability)
            # Ensure extent and intent are sets for internal calculations
            stability_value = self.compute_stability_for_concept(set(extent), set(intent))
            results_with_stability.append(((extent, intent), stability_value))
        return results_with_stability

    def plot_stability_3d(self, results_with_stability):
        """Plots stability in 3D against extent size and intent size."""
        if not results_with_stability:
            logging.warning("No stability results to plot.")
            return

        extents_sizes = [len(concept[0][0]) for concept in results_with_stability]
        intents_sizes = [len(concept[0][1]) for concept in results_with_stability]
        stabilities = [concept[1] for concept in results_with_stability]

        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        sc = ax.scatter(extents_sizes, intents_sizes, stabilities, c=stabilities, cmap='viridis', s=50)
        ax.set_xlabel('Extent Size')
        ax.set_ylabel('Intent Size')
        ax.set_zlabel('Stability')
        plt.colorbar(sc, label='Stability')
        plt.title('Concept Stability Distribution')
        plt.show()


class FasterFCA:
    """
    Implements the FasterFCA algorithm for computing formal concepts.
    Includes LDP integration and stability calculation.
    """
    def __init__(self, threshold=0.0, context_sensitivity=0.0, privacy_preserving=False):
        """
        Initializes the FasterFCA algorithm.

        Args:
            threshold (float): Minimum stability threshold for concepts.
            context_sensitivity (float): Parameter for LDP (if enabled).
            privacy_preserving (bool): If True, LDP will be applied to the context.
        """
        # Determine architecture word size for bitset operations
        self.ARCHBIT = (sys.maxsize.bit_length() + 1)
        self.BIT = 1 << (self.ARCHBIT - 1) # Most significant bit

        self.formal_context_original = {} # Stores the original context read from file
        self.formal_context_processed = {} # Context used for computation (original or LDP-perturbed)
        self.objects_count = 0
        self.attributes_max_id = 0 # Max attribute ID encountered
        self.context_bitset = [] # Bitset representation of the formal context
        self.lattice_concepts = [] # List of computed concepts [(extent, intent), stability]

        self.threshold = threshold
        self.context_sensitivity = context_sensitivity
        self.privacy_preserving = privacy_preserving

        self.ldp_handler = None # Will be initialized if privacy_preserving is True
        self.stability_calculator = None # Will be initialized with the original context

        self.fca_runtime = 0.0
        self.ldp_runtime = 0.0
        self.stability_runtime = 0.0


    def extract_formal_context(self, file_path):
        """
        Extracts the formal context from a file.
        File format: object_id: {attribute1, attribute2, ...} per line.

        Args:
            file_path (str): Path to the context file.
        """
        self.formal_context_original = {}
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    try:
                        obj_id_str, attributes_str = line.split(":", 1)
                        obj_id = int(obj_id_str.strip())
                        # Safely evaluate the attribute string as a set
                        attributes_eval = ast.literal_eval(attributes_str.strip())
                        if not isinstance(attributes_eval, set):
                            raise ValueError("Attributes part must be a set.")
                        self.formal_context_original[obj_id] = {int(attr) for attr in attributes_eval}
                    except (ValueError, SyntaxError) as e:
                        logging.error(f"Skipping malformed line in context file '{file_path}': '{line}' - Error: {e}")
        except FileNotFoundError:
            logging.error(f"Context file not found: {file_path}")
            raise
        except Exception as e:
            logging.error(f"Error reading context file '{file_path}': {e}")
            raise

        if not self.formal_context_original:
            logging.warning(f"Formal context from '{file_path}' is empty.")


    def _prepare_context_for_computation(self):
        """Prepares the formal context for FCA, applying LDP if enabled."""
        if self.privacy_preserving:
            if not self.formal_context_original:
                 logging.warning("Original formal context is empty, LDP will result in an empty processed context.")
                 self.formal_context_processed = {}
                 self.ldp_runtime = 0
                 return

            self.ldp_handler = LDPHandler(self.formal_context_original, self.context_sensitivity)
            self.formal_context_processed = self.ldp_handler.apply_aldp_to_formal_context()
            self.ldp_runtime = self.ldp_handler.get_runtime()
            logging.info("LDP applied to formal context.")
        else:
            self.formal_context_processed = self.formal_context_original
            self.ldp_runtime = 0.0
            logging.info("Using original formal context (LDP not applied).")

        if not self.formal_context_processed:
            self.objects_count = 0
            self.attributes_max_id = 0
            self.context_bitset = []
            logging.warning("Processed formal context is empty. FCA will result in an empty lattice.")
            return

        self.objects_count = len(self.formal_context_processed)
        if self.objects_count == 0:
            self.attributes_max_id = 0
        else:
            all_attrs = set().union(*self.formal_context_processed.values()) if self.formal_context_processed else set()
            self.attributes_max_id = max(all_attrs) if all_attrs else 0

        # Create bitset representation
        # Map object IDs to 0-based indices for bitset rows
        self.obj_id_to_idx_map = {obj_id: i for i, obj_id in enumerate(self.formal_context_processed.keys())}
        # Store the reverse mapping if needed later for reconstructing extents with original IDs
        self.idx_to_obj_id_map = {i: obj_id for obj_id, i in self.obj_id_to_idx_map.items()}


        context_width_bits = self.attributes_max_id
        num_bitset_words = (context_width_bits + self.ARCHBIT -1) // self.ARCHBIT # ceiling division

        self.context_bitset = [[0] * num_bitset_words for _ in range(self.objects_count)]

        for obj_id, attributes in self.formal_context_processed.items():
            obj_idx = self.obj_id_to_idx_map[obj_id]
            for attr_id in attributes:
                if 1 <= attr_id <= self.attributes_max_id: # Attributes are 1-indexed
                    # Adjust for 0-indexed bit positions
                    word_index = (attr_id - 1) // self.ARCHBIT
                    bit_in_word_pos = (attr_id - 1) % self.ARCHBIT
                    # Set the bit (from right, so shift from MSB)
                    self.context_bitset[obj_idx][word_index] |= (1 << (self.ARCHBIT - 1 - bit_in_word_pos))


    def _get_bipartite_cliques(self):
        """Generates initial bipartite cliques from the bitset context."""
        if not self.formal_context_processed or self.objects_count == 0 or self.attributes_max_id == 0:
            return []

        cliques = []
        # Cliques from objects: ( {obj_original_id}, {attributes_of_obj} )
        for original_obj_id, attributes in self.formal_context_processed.items():
            cliques.append( (frozenset([original_obj_id]), frozenset(attributes)) )

        # Cliques from attributes: ( {objects_with_attr}, {attr_id} )
        for attr_id in range(1, self.attributes_max_id + 1):
            objects_with_attr = set()
            # Find which bitset word and bit position this attribute corresponds to
            word_index = (attr_id - 1) // self.ARCHBIT
            bit_in_word_pos = (attr_id - 1) % self.ARCHBIT
            mask = (1 << (self.ARCHBIT - 1 - bit_in_word_pos))

            for obj_idx in range(self.objects_count):
                if self.context_bitset[obj_idx][word_index] & mask:
                    objects_with_attr.add(self.idx_to_obj_id_map[obj_idx]) # Use original object ID

            if objects_with_attr: # Only add if attribute is present in some object
                cliques.append( (frozenset(objects_with_attr), frozenset([attr_id])) )
        return cliques

    def _condense_clique_list(self, current_cliques):
        """Condenses a list of cliques by merging based on shared extents or intents."""
        if not current_cliques:
            return []

        condensed_list = []
        processed_indices = [False] * len(current_cliques)

        for i in range(len(current_cliques)):
            if processed_indices[i]:
                continue

            extent1, intent1 = current_cliques[i]
            merged_extent, merged_intent = set(extent1), set(intent1) # Start with copies
            processed_indices[i] = True

            # Try to merge with subsequent cliques
            for j in range(i + 1, len(current_cliques)):
                if processed_indices[j]:
                    continue

                extent2, intent2 = current_cliques[j]
                merged_this_iter = False
                if merged_extent == extent2: # Same extent, union intents
                    merged_intent.update(intent2)
                    processed_indices[j] = True
                    merged_this_iter = True
                elif merged_intent == intent2: # Same intent, union extents
                    merged_extent.update(extent2)
                    processed_indices[j] = True
                    merged_this_iter = True
                # Add other merging strategies if the algorithm requires, e.g., subset relations

            condensed_list.append((frozenset(merged_extent), frozenset(merged_intent)))
        return condensed_list


    def save_concepts_to_json(self, concepts_with_stability, output_file_path):
        """Saves computed concepts with stability to a JSON file."""
        lattice_output_dict = {}
        for i, ((extent, intent), stability) in enumerate(concepts_with_stability, start=1):
            concept_dict = {
                "Extent": sorted(list(extent)), # Store as sorted list for consistent output
                "Intent": sorted(list(intent)),
                "Stability": stability
            }
            lattice_output_dict[f"Concept_{i}"] = concept_dict
        try:
            with open(output_file_path, 'w') as f:
                json.dump(lattice_output_dict, f, indent=4)
            logging.info(f"Computed concepts saved to {output_file_path}")
        except IOError as e:
            logging.error(f"Error saving concepts to file {output_file_path}: {e}")


    def run(self, input_file_path, output_concepts_file_path):
        """
        Executes the FasterFCA algorithm.

        Args:
            input_file_path (str): Path to the input formal context file.
            output_concepts_file_path (str): Path to save the computed concepts (JSON).

        Returns:
            list: List of computed concepts, each as ((extent, intent), stability).
                  Returns empty list if errors occur or context is empty.
        """
        total_start_time = time.time()

        try:
            self.extract_formal_context(input_file_path)
        except Exception: # Catch errors from file reading
            return [] # Return empty if context extraction fails

        if not self.formal_context_original:
            logging.warning("FCA run aborted: Original formal context is empty.")
            self.fca_runtime = time.time() - total_start_time
            return []

        self._prepare_context_for_computation()

        if not self.formal_context_processed or self.objects_count == 0:
            logging.warning("FCA run aborted: Processed formal context is empty (possibly after LDP).")
            self.fca_runtime = time.time() - total_start_time
            return []

        fca_algo_start_time = time.time()
        # Step 1: Generate initial bipartite cliques
        # The original FasterFCA might have a more direct way to generate concepts,
        # e.g., NextClosure algorithm. This implementation uses iterative condensation.
        current_concepts_tuples = self._get_bipartite_cliques()

        # Step 2: Iteratively condense the list of concept tuples
        # This step aims to find maximal rectangles / formal concepts.
        # The exact mechanism of "condense_list" needs to be faithful to the FasterFCA paper.
        # A simple pairwise merge might not be sufficient for full FCA.
        # For a more standard FCA, one would use an algorithm like NextClosure, InClose, etc.
        # This iterative condensation is a heuristic or a part of a specific algorithm.
        previous_size = -1
        while len(current_concepts_tuples) != previous_size:
            previous_size = len(current_concepts_tuples)
            current_concepts_tuples = self._condense_clique_list(current_concepts_tuples)
            # Deduplicate after each condensation
            current_concepts_tuples = list(set(current_concepts_tuples))


        # Ensure concepts are valid (extent' is intent, intent' is extent)
        # This is a crucial step for correctness if the condensation is heuristic.
        # For now, we assume condensation yields valid pre-concepts.
        # A full FCA algorithm would derive concepts that are their own double prime.

        # Convert to list of ((extent, intent), 0.0) for stability calculation
        # The stability calculator expects the original (pre-LDP) context.
        self.stability_calculator = StabilityCalculator(self.formal_context_original)

        # The `current_concepts_tuples` are (extent, intent) pairs.
        # We need to format them for `compute_stability_for_lattice`
        concepts_for_stability_calc = [((ext, itt), 0.0) for ext, itt in current_concepts_tuples]

        stability_start_time = time.time()
        concepts_with_stability = self.stability_calculator.compute_stability_for_lattice(concepts_for_stability_calc)
        self.stability_runtime = time.time() - stability_start_time

        # Filter concepts by stability threshold
        self.lattice_concepts = [
            concept_stability_pair for concept_stability_pair in concepts_with_stability
            if concept_stability_pair[1] >= self.threshold
        ]
        self.fca_runtime = time.time() - fca_algo_start_time


        # Save concepts to file
        self.save_concepts_to_json(self.lattice_concepts, output_concepts_file_path)

        logging.info(f"FasterFCA run complete. Concepts: {len(self.lattice_concepts)}, "
                     f"LDP Runtime: {self.ldp_runtime:.4f}s, "
                     f"FCA Algo Runtime: {self.fca_runtime:.4f}s, "
                     f"Stability Runtime: {self.stability_runtime:.4f}s")

        return self.lattice_concepts

    def get_runtimes(self):
        return {
            "ldp_runtime": self.ldp_runtime,
            "fca_algorithm_runtime": self.fca_runtime,
            "stability_calculation_runtime": self.stability_runtime,
            "total_fca_pipeline_runtime": self.ldp_runtime + self.fca_runtime + self.stability_runtime
        }

class Encryption:
    """
    Handles encryption and decryption operations.
    Uses Fernet for symmetric encryption of lattice data.
    Includes stubs for Homomorphic Encryption (HE) using TenSEAL.
    """
    def __init__(self):
        self.symmetric_key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.symmetric_key)
        self.total_symmetric_encryption_runtime = 0.0
        self.total_symmetric_decryption_runtime = 0.0

        # HE context (BFV scheme) - Further development needed for practical lattice HE
        try:
            self.he_context = ts.context(ts.SCHEME_TYPE.BFV, poly_modulus_degree=4096, plain_modulus=1032193)
            self.he_context.generate_galois_keys() # Needed for some operations like vector sum
            self.he_context.generate_relin_keys() # Needed for multiplications
            logging.info("TenSEAL (HE) context initialized.")
        except Exception as e:
            self.he_context = None
            logging.warning(f"Failed to initialize TenSEAL context: {e}. HE features will be unavailable.")

    def symmetric_encrypt(self, data_bytes):
        """Encrypts data bytes using Fernet symmetric encryption."""
        start_time = time.time()
        encrypted_data = self.cipher_suite.encrypt(data_bytes)
        self.total_symmetric_encryption_runtime += (time.time() - start_time)
        return encrypted_data

    def symmetric_decrypt(self, encrypted_data_bytes):
        """Decrypts data bytes using Fernet symmetric encryption."""
        start_time = time.time()
        decrypted_data = self.cipher_suite.decrypt(encrypted_data_bytes)
        self.total_symmetric_decryption_runtime += (time.time() - start_time)
        return decrypted_data

    def homomorphic_encrypt_vector(self, int_vector):
        """Encrypts a list of integers using BFV homomorphic encryption."""
        if not self.he_context:
            raise RuntimeError("Homomorphic encryption context is not available.")
        if not all(isinstance(x, int) for x in int_vector):
            raise ValueError("Homomorphic encryption (BFV vector) requires a list of integers.")
        return ts.bfv_vector(self.he_context, int_vector)

    def homomorphic_decrypt_vector(self, encrypted_bfv_vector):
        """Decrypts a BFV homomorphically encrypted vector."""
        if not self.he_context:
            raise RuntimeError("Homomorphic encryption context is not available.")
        return encrypted_bfv_vector.decrypt()

    # --- Key/Context Management (Example) ---
    def export_symmetric_key(self):
        """Exports the Fernet symmetric key as bytes."""
        return self.symmetric_key

    def import_symmetric_key(self, key_bytes):
        """Imports a Fernet symmetric key."""
        self.symmetric_key = key_bytes
        self.cipher_suite = Fernet(self.symmetric_key)

    def get_encryption_runtimes(self):
        return {
            "symmetric_encryption_total_runtime": self.total_symmetric_encryption_runtime,
            "symmetric_decryption_total_runtime": self.total_symmetric_decryption_runtime,
        }

class Provider:
    """
    Represents a data provider in the federated learning setup.
    Computes its local concept lattice from its data file.
    """
    def __init__(self, provider_id, input_file_path, threshold=0.0, context_sensitivity=0.0,
                 privacy_preserving=False, encryption_service=None):
        """
        Initializes a Provider.

        Args:
            provider_id (str or int): Unique identifier for the provider.
            input_file_path (str): Path to the provider's local data file.
            threshold (float): Stability threshold for concept filtering.
            context_sensitivity (float): LDP context sensitivity.
            privacy_preserving (bool): Whether to apply LDP.
            encryption_service (Encryption): Instance of the Encryption class for encrypting results.
        """
        self.id_provider = f"Provider_{provider_id}"
        self.input_file_path = input_file_path
        self.encryption_service = encryption_service
        self.local_lattice_with_stability = [] # Stores ((extent, intent), stability)

        self.fca_analyzer = FasterFCA(threshold=threshold,
                                      context_sensitivity=context_sensitivity,
                                      privacy_preserving=privacy_preserving)
        logging.info(f"{self.id_provider} initialized. Data: '{input_file_path}', Threshold: {threshold}, LDP: {privacy_preserving}")

    def compute_local_lattice(self):
        """
        Computes the local concept lattice from the provider's data.
        The result (lattice) is stored internally and also returned, potentially encrypted.

        Returns:
            dict: A dictionary containing provider ID and the (optionally encrypted) lattice.
                  Format: {"Provider": self.id_provider, "lattice_payload": payload}
                  Returns None if an error occurs.
        """
        try:
            # Define a unique output file path for this provider's concepts (optional, for debugging)
            output_concept_file = f"{self.input_file_path}_concepts_{self.id_provider}.json"
            self.local_lattice_with_stability = self.fca_analyzer.run(self.input_file_path, output_concept_file)

            if not self.local_lattice_with_stability:
                logging.warning(f"{self.id_provider}: No concepts computed or all filtered out.")
                # return {"Provider": self.id_provider, "lattice_payload": None, "error": "No concepts"}


            # Serialize the lattice to JSON string
            # The lattice is a list of [((extent_fset, intent_fset), stability_float)]
            lattice_json_str = json.dumps(self.local_lattice_with_stability, default=json_serializer_helper)
            payload = lattice_json_str.encode('utf-8') # Encode to bytes

            if self.encryption_service:
                payload = self.encryption_service.symmetric_encrypt(payload)
                logging.info(f"{self.id_provider}: Local lattice computed and encrypted.")
            else:
                logging.info(f"{self.id_provider}: Local lattice computed (unencrypted).")

            return {"Provider": self.id_provider, "lattice_payload": payload}

        except Exception as e:
            logging.error(f"{self.id_provider}: Error computing local lattice: {e}", exc_info=True)
            return None # Or: {"Provider": self.id_provider, "lattice_payload": None, "error": str(e)}

    def get_fca_runtimes(self):
        """Returns the runtimes from the FasterFCA instance."""
        return self.fca_analyzer.get_runtimes()


class Aggregator:
    """
    Aggregates encrypted local lattices from multiple providers.
    Decrypts lattices and computes a global concept lattice and its stability.
    """
    def __init__(self, threshold=0.5, encryption_service=None):
        """
        Initializes the Aggregator.

        Args:
            threshold (float): Threshold used for filtering or other aggregation logic (if any).
                               Note: `FasterFCA` already filters by threshold locally.
            encryption_service (Encryption): Instance of the Encryption class for decryption.
        """
        self.global_lattice = []  # Stores aggregated global lattice [((extent, intent), stability)]
        self.global_stability = 0.0
        self.decrypted_lattices_cache = [] # Stores successfully decrypted lattices from providers
        self.encryption_service = encryption_service
        self.aggregation_threshold = threshold # May be used differently than provider's threshold
        self.dataset_file_name_for_classical = None # For classical FCA comparison
        self.aggregation_runtimes = {}


    def set_dataset_for_classical_comparison(self, file_path):
        self.dataset_file_name_for_classical = file_path

    def _decrypt_and_parse_lattice(self, provider_id, lattice_payload):
        """Helper to decrypt and parse a single provider's lattice payload."""
        decrypted_bytes = lattice_payload
        if self.encryption_service:
            try:
                decrypted_bytes = self.encryption_service.symmetric_decrypt(lattice_payload)
            except Exception as e:
                logging.error(f"Aggregator: Failed to decrypt lattice from {provider_id}: {e}")
                return None
        try:
            # Deserialize JSON string to Python list structure
            # The structure is: [ [ [list_extent, list_intent], stability_float], ... ]
            # We need to convert extents/intents back to frozensets for set operations.
            lattice_list_form = json.loads(decrypted_bytes.decode('utf-8'))
            
            parsed_lattice = []
            for concept_data in lattice_list_form:
                # concept_data is like [[list_extent, list_intent], stability_float]
                concept_tuple, stability = concept_data
                extent_list, intent_list = concept_tuple
                parsed_lattice.append(((frozenset(extent_list), frozenset(intent_list)), stability))
            return parsed_lattice
        except json.JSONDecodeError as e:
            logging.error(f"Aggregator: Failed to parse JSON lattice from {provider_id}: {e}")
        except Exception as e:
            logging.error(f"Aggregator: Unexpected error parsing lattice from {provider_id}: {e}")
        return None


    def aggregate_provider_lattices(self, provider_results_list):
        """
        Aggregates lattices from provider results.

        Args:
            provider_results_list (list): List of dictionaries, each from a Provider:
                                          {"Provider": provider_id, "lattice_payload": encrypted_bytes_or_bytes}
        """
        start_time = time.time()
        self.decrypted_lattices_cache = []
        all_concepts_from_providers = [] # List of all ((extent, intent), stability) tuples

        for result in provider_results_list:
            if result and "Provider" in result and "lattice_payload" in result and result["lattice_payload"] is not None:
                provider_id = result["Provider"]
                lattice_payload = result["lattice_payload"]
                
                decrypted_lattice = self._decrypt_and_parse_lattice(provider_id, lattice_payload)
                if decrypted_lattice:
                    self.decrypted_lattices_cache.append({"Provider": provider_id, "lattice": decrypted_lattice})
                    all_concepts_from_providers.extend(decrypted_lattice)
                    logging.info(f"Aggregator: Successfully processed lattice from {provider_id}, {len(decrypted_lattice)} concepts.")
                else:
                    logging.warning(f"Aggregator: Could not process lattice from {provider_id}.")
            else:
                logging.warning(f"Aggregator: Invalid or empty result received: {result}")


        if not all_concepts_from_providers:
            logging.warning("Aggregator: No valid concepts from any provider to aggregate.")
            self.global_lattice = []
            self.global_stability = 0.0
            self.aggregation_runtimes['aggregation_logic'] = time.time() - start_time
            return {"global_lattice": self.global_lattice, "global_stability": self.global_stability}

        # --- Global Lattice Computation Logic ---
        # This is a placeholder for a robust lattice merging algorithm.
        # A simple approach: take the union of all concepts and then try to find a "closure"
        # or use a more sophisticated lattice merging technique.
        # For now, let's use a simplified approach:
        # 1. Collect all unique concepts.
        # 2. (Optional) Apply some form of closure or reduction if needed.
        # 3. Calculate average stability for the resulting set.

        # Using a set to store unique concepts ( ( (extent_fset, intent_fset), stability ) )
        # However, stability might differ for the same (extent, intent) from different views.
        # We need to decide how to handle this. Average stability? Max?
        # Let's group by (extent, intent) and average stability.
        concept_stability_map = defaultdict(list)
        for (concept_ci, stability_ci) in all_concepts_from_providers:
            concept_stability_map[concept_ci].append(stability_ci)

        aggregated_concepts_with_avg_stability = []
        for concept_ci, stabilities in concept_stability_map.items():
            avg_stability = sum(stabilities) / len(stabilities) if stabilities else 0.0
            # Apply aggregation threshold if needed
            # if avg_stability >= self.aggregation_threshold: # Example
            aggregated_concepts_with_avg_stability.append((concept_ci, avg_stability))
        
        # The `compute_global_lattice` method from the original code was very complex
        # and its theoretical soundness was unclear. Replacing with a simpler unique concept collection for now.
        # A proper lattice merging algorithm should be implemented here if true merging is desired.
        self.global_lattice = aggregated_concepts_with_avg_stability
        
        # Sort for consistent output (optional)
        self.global_lattice.sort(key=lambda x: (len(x[0][0]), len(x[0][1])))


        # Compute average stability of the final global lattice
        if self.global_lattice:
            self.global_stability = sum(s for _, s in self.global_lattice) / len(self.global_lattice)
        else:
            self.global_stability = 0.0

        self.aggregation_runtimes['aggregation_logic'] = time.time() - start_time
        logging.info(f"Aggregation complete. Global lattice: {len(self.global_lattice)} concepts, Avg Stability: {self.global_stability:.4f}")
        return {"global_lattice": self.global_lattice, "global_stability": self.global_stability}


    def run_classical_fca_on_dataset(self, threshold=0.0, context_sensitivity=0.0, privacy_preserving=False):
        """
        Runs classical (centralized) FCA on the full dataset for comparison.
        The dataset_file_name_for_classical must be set prior to calling this.
        """
        if not self.dataset_file_name_for_classical:
            logging.error("Dataset file for classical FCA not set in Aggregator.")
            return None, 0.0, {} # concepts, stability, runtimes

        logging.info(f"Running classical FCA on: {self.dataset_file_name_for_classical}")
        classical_fca_analyzer = FasterFCA(threshold=threshold,
                                           context_sensitivity=context_sensitivity,
                                           privacy_preserving=privacy_preserving) # LDP can also be tested in classical
        
        output_file = f"{self.dataset_file_name_for_classical}_classical_concepts.json"
        classical_lattice = classical_fca_analyzer.run(self.dataset_file_name_for_classical, output_file)
        
        avg_stability_classical = 0.0
        if classical_lattice:
            avg_stability_classical = sum(s for _, s in classical_lattice) / len(classical_lattice)

        runtimes = classical_fca_analyzer.get_runtimes()
        logging.info(f"Classical FCA complete. Concepts: {len(classical_lattice)}, Avg Stability: {avg_stability_classical:.4f}")
        return classical_lattice, avg_stability_classical, runtimes

    def get_aggregation_runtimes(self):
        return self.aggregation_runtimes

# --- Simulation Engine ---
# Global parameters for simulation (can be moved to a config file)
# For demonstration, paths are relative or expect a certain structure.
# In a real setup, make these configurable.
SIM_BASE_PATH = "./sim_data" # Example base path for datasets
RESULTS_DIR = "./sim_results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Simulation parameters (example values)
# These were global in the original script. Encapsulating them or loading from config is better.
SIM_PARAMS = {
    "dataset_names": ["mushroom"], # "RecordLink", "chess" - ensure these datasets exist and are formatted
    "num_providers_list": [5], # 50, 100
    "splitting_types": ["IID"], # "Non-IID"
    "fractions_of_participation": [1.0], # 0.5, 0.75
    "context_sensitivity_values": np.arange(0.1, 0.5, 0.2), # Smaller range for faster demo
    "threshold_list": [0.1], # 0.0, 0.5
    "privacy_in_federated": [True, False], # Test federated with and without LDP
    "privacy_in_classical": False # Classical usually without LDP, but can be tested
}

def get_system_usage():
    """Gets current CPU and RAM usage."""
    cpu_percent = psutil.cpu_percent(interval=0.1)
    ram_info = psutil.virtual_memory()
    return cpu_percent, ram_info.percent, ram_info.used / (1024 ** 3) # GB

def simulate_one_federated_run(dataset_name, num_providers, splitting_type,
                               provider_fraction, threshold, context_sensitivity,
                               apply_ldp_federated, encryption_service):
    """Simulates one run of federated learning."""
    # Construct path to provider data. This assumes a pre-partitioned dataset structure.
    # e.g., SIM_BASE_PATH/mushroom/5/IID/provider_1.txt, provider_2.txt ...
    provider_data_dir = os.path.join(SIM_BASE_PATH, dataset_name, str(num_providers), splitting_type)
    if not os.path.isdir(provider_data_dir):
        logging.error(f"Provider data directory not found: {provider_data_dir}")
        return None, 0.0, {}, {} # No results if data dir is missing

    all_provider_files = [os.path.join(provider_data_dir, f) for f in os.listdir(provider_data_dir) if f.startswith("provider_") and f.endswith(".txt")]
    if not all_provider_files:
        logging.error(f"No provider data files found in {provider_data_dir}")
        return None, 0.0, {}, {}


    num_actual_providers = len(all_provider_files)
    num_participating = max(1, int(provider_fraction * num_actual_providers))
    
    # Ensure we don't try to sample more files than available
    participating_files = random.sample(all_provider_files, min(num_participating, num_actual_providers))
    
    logging.info(f"Simulating Federated: Dataset={dataset_name}, TotalProviders={num_actual_providers}, "
                 f"Participating={len(participating_files)}, Split={splitting_type}, LDP={apply_ldp_federated}, CS={context_sensitivity:.1f}, Thr={threshold:.1f}")

    providers = []
    total_provider_fca_runtimes = defaultdict(float)

    for i, file_path in enumerate(participating_files):
        provider = Provider(provider_id=i,
                            input_file_path=file_path,
                            threshold=threshold,
                            context_sensitivity=context_sensitivity,
                            privacy_preserving=apply_ldp_federated,
                            encryption_service=encryption_service)
        providers.append(provider)

    provider_payloads = []
    for provider in providers:
        payload = provider.compute_local_lattice()
        if payload: # Ensure payload is not None
            provider_payloads.append(payload)
        # Accumulate runtimes from each provider's FCA process
        p_runtimes = provider.get_fca_runtimes()
        for key, val in p_runtimes.items():
            total_provider_fca_runtimes[key] += val
    
    if not provider_payloads:
        logging.warning("No provider payloads generated for aggregation.")
        return [], 0.0, total_provider_fca_runtimes, {}


    aggregator = Aggregator(threshold=threshold, encryption_service=encryption_service)
    agg_results = aggregator.aggregate_provider_lattices(provider_payloads)
    
    return agg_results["global_lattice"], agg_results["global_stability"], total_provider_fca_runtimes, aggregator.get_aggregation_runtimes()


def run_all_simulations():
    """Runs all simulation configurations and stores results."""
    all_results_data = []
    encryption_service = Encryption() # Single encryption service for the simulation duration

    # Ensure SIM_BASE_PATH and a sample dataset directory exist for the simulation to run
    # Example: ./sim_data/mushroom/5/IID/
    # You'd need to create these and populate with `provider_X.txt` files.
    # And a `mushroom_full.data` (or similar) for classical comparison.
    if not os.path.exists(os.path.join(SIM_BASE_PATH, SIM_PARAMS["dataset_names"][0])):
        logging.error(f"Base simulation data path for dataset '{SIM_PARAMS['dataset_names'][0]}' not found at '{SIM_BASE_PATH}'. Please create it.")
        logging.error("Example structure: ./sim_data/mushroom/5/IID/provider_1.txt")
        logging.error("And for classical: ./sim_data/mushroom/mushroom_full.data")
        return pd.DataFrame() # Return empty dataframe


    for dataset_name in SIM_PARAMS["dataset_names"]:
        classical_results_cache = {} # Cache classical results for a dataset

        # Path to the full dataset file for classical comparison
        # Assumes a naming convention like 'dataset_name_full.data'
        full_dataset_file = os.path.join(SIM_BASE_PATH, dataset_name, f"{dataset_name}_full.data")

        for num_providers in SIM_PARAMS["num_providers_list"]:
            for splitting_type in SIM_PARAMS["splitting_types"]:
                for fraction in SIM_PARAMS["fractions_of_participation"]:
                    for threshold_val in SIM_PARAMS["threshold_list"]:
                        for cs_val in SIM_PARAMS["context_sensitivity_values"]:
                            for ldp_fed in SIM_PARAMS["privacy_in_federated"]:
                                
                                run_id = str(uuid.uuid4())[:8]
                                logging.info(f"--- Starting Run ID: {run_id} ---")
                                
                                # Reset encryption runtimes for this specific federated run
                                encryption_service.total_symmetric_encryption_runtime = 0.0
                                encryption_service.total_symmetric_decryption_runtime = 0.0

                                cpu_start, ram_start_pct, ram_used_start_gb = get_system_usage()
                                fed_start_time = time.time()

                                fed_lattice, fed_stability, prov_runtimes, agg_runtimes = \
                                    simulate_one_federated_run(dataset_name, num_providers, splitting_type,
                                                               fraction, threshold_val, cs_val,
                                                               ldp_fed, encryption_service)
                                fed_total_runtime = time.time() - fed_start_time
                                cpu_end, ram_end_pct, ram_used_end_gb = get_system_usage()

                                current_run_enc_runtimes = encryption_service.get_encryption_runtimes()
                                fed_computation_runtime = (prov_runtimes.get('total_fca_pipeline_runtime',0) +
                                                           agg_runtimes.get('aggregation_logic',0))
                                
                                # Run classical FCA for comparison (once per dataset config if not varying by LDP params for classical)
                                classical_cache_key = (dataset_name, threshold_val, SIM_PARAMS["privacy_in_classical"], cs_val if SIM_PARAMS["privacy_in_classical"] else 0.0)
                                if classical_cache_key not in classical_results_cache:
                                    if not os.path.exists(full_dataset_file):
                                        logging.warning(f"Full dataset file for classical comparison not found: {full_dataset_file}. Skipping classical.")
                                        classical_results_cache[classical_cache_key] = ([], 0.0, {}, 0.0) # Store empty
                                    else:
                                        logging.info(f"Running classical for {classical_cache_key}...")
                                        classical_agg = Aggregator() # No encryption for classical data itself
                                        classical_agg.set_dataset_for_classical_comparison(full_dataset_file)
                                        
                                        classical_cpu_start, _, _ = get_system_usage()
                                        classical_start_time = time.time()
                                        
                                        c_lattice, c_stability, c_fca_runtimes = classical_agg.run_classical_fca_on_dataset(
                                            threshold=threshold_val,
                                            context_sensitivity=cs_val, # Use same CS if LDP is on for classical
                                            privacy_preserving=SIM_PARAMS["privacy_in_classical"]
                                        )
                                        classical_total_runtime = time.time() - classical_start_time
                                        classical_cpu_end, _, _ = get_system_usage()
                                        
                                        classical_results_cache[classical_cache_key] = (
                                            c_lattice, c_stability, c_fca_runtimes, classical_total_runtime,
                                            (classical_cpu_start + classical_cpu_end) / 2.0
                                        )
                                
                                c_lattice_res, c_stability_res, c_fca_runtimes_res, c_total_runtime_res, c_cpu_avg_res = classical_results_cache[classical_cache_key]

                                result_entry = {
                                    "RunID": run_id,
                                    "Dataset": dataset_name,
                                    "NumProviders": num_providers,
                                    "Splitting": splitting_type,
                                    "ParticipationFraction": fraction,
                                    "Threshold": threshold_val,
                                    "ContextSensitivity": round(cs_val,2),
                                    "LDP_Federated": ldp_fed,
                                    "Federated_Concepts_Count": len(fed_lattice) if fed_lattice else 0,
                                    "Federated_Global_Stability": round(fed_stability, 4),
                                    "Federated_Total_Runtime_s": round(fed_total_runtime, 4),
                                    "Federated_Provider_FCA_Sum_Runtime_s": round(prov_runtimes.get('total_fca_pipeline_runtime',0),4),
                                    "Federated_Aggregator_Logic_Runtime_s": round(agg_runtimes.get('aggregation_logic',0),4),
                                    "Federated_Encryption_Runtime_s": round(current_run_enc_runtimes['symmetric_encryption_total_runtime'],4),
                                    "Federated_Decryption_Runtime_s": round(current_run_enc_runtimes['symmetric_decryption_total_runtime'],4),
                                    "Federated_CPU_Avg_Pct": round((cpu_start + cpu_end) / 2, 2),
                                    "Federated_RAM_Used_Start_GB": round(ram_used_start_gb, 3),
                                    "Federated_RAM_Used_End_GB": round(ram_used_end_gb, 3),
                                    "Classical_Concepts_Count": len(c_lattice_res) if c_lattice_res else 0,
                                    "Classical_Avg_Stability": round(c_stability_res, 4),
                                    "Classical_FCA_Runtime_s": round(c_fca_runtimes_res.get('total_fca_pipeline_runtime',0),4),
                                    "Classical_Total_Runtime_s": round(c_total_runtime_res, 4),
                                    "Classical_CPU_Avg_Pct": round(c_cpu_avg_res, 2),
                                    "LDP_Classical": SIM_PARAMS["privacy_in_classical"]
                                }
                                all_results_data.append(result_entry)
                                logging.info(f"--- Finished Run ID: {run_id} ---")
                                # Optionally save incrementally
                                # pd.DataFrame([result_entry]).to_csv(os.path.join(RESULTS_DIR,"fedfca_simulation_results.csv"), mode='a', header=not os.path.exists(os.path.join(RESULTS_DIR,"fedfca_simulation_results.csv")), index=False)


    results_df = pd.DataFrame(all_results_data)
    results_df.to_csv(os.path.join(RESULTS_DIR, "fedfca_simulation_results_final.csv"), index=False)
    results_df.to_json(os.path.join(RESULTS_DIR, "fedfca_simulation_results_final.json"), orient="records", indent=4)
    logging.info(f"All simulations complete. Results saved in '{RESULTS_DIR}' directory.")
    return results_df

if __name__ == '__main__':
    # --- Important Setup for Simulation ---
    # 1. Create the base simulation data directory, e.g., './sim_data'
    # 2. Inside it, create a directory for each dataset, e.g., './sim_data/mushroom'
    # 3. For classical comparison, place the full dataset file, e.g., './sim_data/mushroom/mushroom_full.data'
    #    (The format should be: obj_id: {attr1, attr2, ...})
    # 4. For federated simulation, create subdirectories for provider partitions:
    #    e.g., './sim_data/mushroom/5/IID/' for 5 providers, IID split.
    #    Populate this with 'provider_1.txt', 'provider_2.txt', etc.
    #    These files should also be in the format: obj_id: {attr1, attr2, ...}
    #    You can use the `loadData.py` script (if available and adapted) to generate these partitions.

    logging.info("Starting FedFCA Simulations...")
    # Example: Ensure the directory structure exists before running
    # This is a placeholder; you'll need to set up your data.
    sample_dataset_name = SIM_PARAMS["dataset_names"][0]
    sample_num_providers = SIM_PARAMS["num_providers_list"][0]
    sample_splitting_type = SIM_PARAMS["splitting_types"][0]
    
    # Create dummy directories and files for a minimal test run if they don't exist
    # This is just to make the script runnable without manual setup for a very basic test.
    # In a real scenario, you would have your actual partitioned data.
    
    # Create base path
    if not os.path.exists(SIM_BASE_PATH):
        os.makedirs(SIM_BASE_PATH)

    # Create dataset path
    dataset_path = os.path.join(SIM_BASE_PATH, sample_dataset_name)
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)

    # Create dummy full data file for classical
    dummy_full_data_path = os.path.join(dataset_path, f"{sample_dataset_name}_full.data")
    if not os.path.exists(dummy_full_data_path):
        with open(dummy_full_data_path, "w") as f:
            f.write("1: {1, 2}\n")
            f.write("2: {2, 3}\n")
            f.write("3: {1, 3}\n")
            f.write("4: {4}\n") # Add an object with a unique attribute for non-empty context
            logging.info(f"Created dummy full data file: {dummy_full_data_path}")
    
    # Create dummy provider data directory
    provider_dir_path = os.path.join(dataset_path, str(sample_num_providers), sample_splitting_type)
    if not os.path.exists(provider_dir_path):
        os.makedirs(provider_dir_path)
        logging.info(f"Created dummy provider directory: {provider_dir_path}")

    # Create dummy provider files
    for i in range(1, sample_num_providers + 1):
        dummy_provider_file_path = os.path.join(provider_dir_path, f"provider_{i}.txt")
        if not os.path.exists(dummy_provider_file_path):
            with open(dummy_provider_file_path, "w") as f:
                # Give each provider slightly different data to make aggregation non-trivial
                if i % 2 == 0:
                    f.write(f"{i*10+1}: {{1, {i}}}\n")
                    f.write(f"{i*10+2}: {{{i}, 3}}\n")
                else:
                    f.write(f"{i*10+1}: {{2, {i}}}\n")
                    f.write(f"{i*10+2}: {{{i}, 4}}\n") # Ensure attribute 4 is present
            logging.info(f"Created dummy provider file: {dummy_provider_file_path}")
            
    simulation_results_df = run_all_simulations()
    if not simulation_results_df.empty:
        print("\n--- Simulation Results Summary ---")
        print(simulation_results_df.head())
    else:
        print("Simulations did not produce results. Check logs for errors.")

