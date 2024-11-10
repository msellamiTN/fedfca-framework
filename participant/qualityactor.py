import itertools
import random
import logging

class ConceptStabilityAnalyzer:
    def __init__(self, matrix=None, attributes=None, objects=None):
        self.I = matrix  # Binary Matrix: list of lists (2D array)
        self.attributes = attributes  # Categorical/Discrete attributes
        self.objects = objects  # Discrete objects
        logging.basicConfig(level=logging.INFO)  # Set up logging configuration

    @staticmethod
    def closure(objects, I):
        """Calculates the closure of a set of objects in the context matrix I."""
        try:
            logging.debug("Calculating closure for objects: %s", objects)
            if not objects:  # If the subset is empty, return empty set
                logging.info("Empty objects provided, returning empty set.")
                return set()
            closure_result = set()
            for attr_index in range(len(I[0])):  # Iterate over attributes
                if all(I[obj][attr_index] for obj in objects):  # Check if all objects have the attribute
                    closure_result.add(attr_index)  # Add attribute index to closure result
            logging.debug("Closure result: %s", closure_result)
            return closure_result
        except Exception as e:
            logging.error('Error in closure calculation: %s', e)
            return set()  # Return an empty set in case of error

    def intensional_stability(self, concept):
        """Calculates the intensional stability of a formal concept (extent, intent)."""
        try:
            extent, intent = concept
            logging.debug("Calculating intensional stability for concept: %s", concept)
            A_subsets = list(itertools.chain.from_iterable(itertools.combinations(extent, r) for r in range(len(extent) + 1)))
            stable_count = sum(1 for a in A_subsets if self.closure(a, self.I) == set(intent))
            stability = stable_count / (2 ** len(extent))
            logging.info("Intensional stability for concept %s: %s", concept, stability)
            return stability
        except Exception as e:
            logging.error('Error in intensional stability calculation for concept %s: %s', concept, e)
            return 0.0  # Return 0.0 in case of error

    def extensional_stability(self, concept):
        """Calculates the extensional stability of a formal concept (extent, intent)."""
        try:
            extent, intent = concept
            logging.debug("Calculating extensional stability for concept: %s", concept)
            B_subsets = list(itertools.chain.from_iterable(itertools.combinations(intent, r) for r in range(len(intent) + 1)))
            stable_count = sum(1 for b in B_subsets if self.closure(extent, self.I) == set(b))
            stability = stable_count / (2 ** len(intent))
            logging.info("Extensional stability for concept %s: %s", concept, stability)
            return stability
        except Exception as e:
            logging.error('Error in extensional stability calculation for concept %s: %s', concept, e)
            return 0.0  # Return 0.0 in case of error

    def concept_probability(self, concept):
        """Calculates the probability of a concept based on the occurrence of its intent attributes."""
        try:
            extent, intent = concept
            logging.debug("Calculating concept probability for concept: %s", concept)
            probability = sum(1 for b in intent if any(self.I[obj][b] for obj in extent)) / len(self.objects)
            logging.info("Probability for concept %s: %s", concept, probability)
            return probability
        except Exception as e:
            logging.error('Error in concept probability calculation for concept %s: %s', concept, e)
            return 0.0  # Return 0.0 in case of error

    def separation_index(self, concept):
        """Calculates the separation index of a formal concept (extent, intent)."""
        try:
            extent, intent = concept
            logging.debug("Calculating separation index for concept: %s", concept)
            area_covered = len(extent) * len(intent)
            total_area = len(self.closure(extent, self.I))
            separation = area_covered / (total_area if total_area > 0 else 1)
            logging.info("Separation index for concept %s: %s", concept, separation)
            return separation
        except Exception as e:
            logging.error('Error in separation index calculation for concept %s: %s', concept, e)
            return 0.0  # Return 0.0 in case of error

    def concept_stability(self, concept):
        """Calculates the concept stability of a formal concept (extent, intent)."""
        try:
            extent, intent = concept
            logging.debug("Calculating concept stability for concept: %s", concept)
            A_subsets = list(itertools.chain.from_iterable(itertools.combinations(extent, r) for r in range(len(extent) + 1)))
            stable_count = sum(1 for a in A_subsets if self.closure(a, self.I) == set(intent))
            stability = stable_count / (2 ** len(extent))
            logging.info("Concept stability for concept %s: %s", concept, stability)
            return stability
        except Exception as e:
            logging.error('Error in concept stability calculation for concept %s: %s', concept, e)
            return 0.0  # Return 0.0 in case of error
    def compute_stability(self,lattice, target_concept):
        """
        Computes the intensional stability σ_I for a given formal concept.

        Parameters:
        lattice (list of tuples): List of all concepts in the lattice, where each concept is a tuple (extent, intent).
        target_concept (tuple): The target concept for which to compute stability, represented as (extent, intent).

        Returns:
        float: The computed intensional stability as a percentage.
        """
        # Initialize stability values for each concept
        stability_values = {id(concept): 0 for concept in lattice}
        logging.info("computed intensional stability :%s  : %s",lattice,target_concept)
        def compute_sub_concept_stability(concept):
            # If stability already computed, return it
            if concept in stability_values and stability_values[concept] is not None:
                return stability_values[concept]

            # Initialize the stability sum for this concept
            stability_sum = 0
            
            # Compute stability for all sub-concepts
            for sub_concept in lattice:
                sub_extent, _ = sub_concept
                concept_extent, _ = concept
                
                # Check if the sub_concept is a subset of the concept and not the same
                if sub_extent.issubset(concept_extent) and sub_concept != concept:
                    stability_sum += compute_sub_concept_stability(sub_concept) / (2 ** (len(concept_extent) - len(sub_extent)))
            
            # Calculate stability for the current concept
            stability = 1 - stability_sum
            stability_values[concept] = stability
            return stability

        try:
            # Compute stability for the target concept
            stability_value = compute_sub_concept_stability(target_concept)
            logging.info("intensional_stability:%s ",stability_value * 100 )
            return stability_value * 100  # Return as percentage
        except Exception as e:
            print(f"An error occurred while computing stability: {e}")
            return None
    
    def calculate_quality(self, lattice):
        """Calculates various quality metrics for each concept in the lattice.
        
        Args:
            lattice (list): List of tuples, each representing (extent, intent) of a concept.
        
        Returns:
            list: A list of dictionaries containing the quality metrics for each concept.
        """
        quality_metrics = []
        
        try:
            logging.info("quality metrics for each concept in the lattice")
            logging.info("Lattice: %s", type(lattice))
            #logging.info("Type of Lattice: %s, Type of Matrix: %s", type(lattice), type(self.I))
            #logging.info("Lattice: %s", lattice)
            for concept in lattice:
                extent, intent = concept
                logging.info("Type of extent: %s, Type of intent: %s", type(extent), type(intent))
                #logging.info("Processing concept: %s", (extent, intent))
                
                metrics = {
                    #'separation_index': self.separation_index(concept),
                    #'concept_probability': self.concept_probability(concept),
                    'intensional_stability': self.compute_stability(lattice, concept),  # Pass only two arguments
                    #'extensional_stability': self.extensional_stability(concept)
                }
                
                quality_metrics.append({
                    'concept': concept,
                    'metrics': metrics
                })
        
        except Exception as e:
            logging.error('Error in quality calculation: %s', e)

        return quality_metrics

    def calculate_stability(self, lattice, sample_fraction=0.01):
        """Calculate the approximate stability of each concept in the lattice.
        
        Args:
            lattice (list): List of tuples, each representing (extent, intent) of a concept.
            sample_fraction (float): Fraction of subsets to sample for stability.
        
        Returns:
            list: Stability scores for each concept.
        """
        logging.info("Begin Stability Calculation")
        stability_scores = []
        try:
            for concept in lattice:
                extent, intent = concept
                #logging.info("Processing concept: %s", (extent, intent))
                
                # Calculate the total possible subsets of extent
                subset_count = 2 ** len(extent)
                sampled_count = int(subset_count * sample_fraction)

                # Generate and sample subsets of extent
                all_subsets = list(itertools.chain.from_iterable(itertools.combinations(extent, r) for r in range(len(extent) + 1)))
                sampled_subsets = random.sample(all_subsets, min(sampled_count, len(all_subsets)))

                # Count valid subsets (for simplicity, we assume intent is always valid)
                valid_subset_count = len(sampled_subsets)
                
                # Calculate stability for this concept
                stability = valid_subset_count / subset_count
                #logging.info("Stability for concept %s: %s", (extent, intent), stability)
                stability_scores.append(stability)
        except Exception as e:
            logging.error('Error in overall stability calculation: %s', e)

        return stability_scores
