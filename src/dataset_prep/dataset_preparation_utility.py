import numpy as np
import os
import random
import shutil
import requests
import yaml
import ast
import time
import logging
from typing import Dict, List, Set, Tuple, Any
from scipy.stats import dirichlet

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatasetManager:
    """
    Manages downloading, processing, splitting, and saving datasets for the FedFCA framework.
    """
    def __init__(self, config_path: str = "config/config.yml", base_output_dir: str = "federated_data"):
        """
        Initializes the DatasetManager.

        Args:
            config_path (str): Path to the YAML configuration file for datasets.
            base_output_dir (str): Base directory where processed and split datasets will be saved.
        """
        self.config = self._load_config(config_path)
        self.base_output_dir = base_output_dir
        os.makedirs(self.base_output_dir, exist_ok=True)
        logging.info(f"DatasetManager initialized. Output will be saved to: {self.base_output_dir}")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Loads dataset configurations from a YAML file."""
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                logging.info(f"Successfully loaded dataset configuration from: {config_path}")
                return config_data
        except FileNotFoundError:
            logging.error(f"Configuration file not found: {config_path}. Please create it.")
            raise
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML configuration file {config_path}: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred while loading config {config_path}: {e}")
            raise

    def _download_file(self, url: str, download_path: str) -> bool:
        """Downloads a file from a URL to a specified path."""
        try:
            logging.info(f"Downloading dataset from {url} to {download_path}...")
            response = requests.get(url, stream=True, timeout=60) # Added timeout
            response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
            
            os.makedirs(os.path.dirname(download_path), exist_ok=True) # Ensure directory exists
            with open(download_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded {os.path.basename(download_path)}.")
            return True
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading file from {url}: {e}")
            return False
        except Exception as e:
            logging.error(f"An unexpected error occurred during download from {url}: {e}")
            return False

    def _process_fimi_to_formal_context(self, fimi_file_path: str) -> Dict[int, Set[int]]:
        """
        Processes a FIMI formatted dataset file into a formal context dictionary.
        FIMI format: each line contains space-separated integers (attributes) for one object.

        Args:
            fimi_file_path (str): Path to the FIMI formatted data file.

        Returns:
            Dict[int, Set[int]]: Formal context as {object_id: {attribute_set}}.
        """
        formal_context: Dict[int, Set[int]] = {}
        try:
            with open(fimi_file_path, 'r') as f:
                for i, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        # Object ID is 1-based (line number + 1)
                        object_id = i + 1
                        attributes = set(map(int, line.split()))
                        formal_context[object_id] = attributes
                    except ValueError:
                        logging.warning(f"Skipping malformed line {i+1} in {fimi_file_path}: '{line}'")
            logging.info(f"Processed {fimi_file_path} into formal context: {len(formal_context)} objects.")
        except FileNotFoundError:
            logging.error(f"FIMI data file not found for processing: {fimi_file_path}")
            return {} # Return empty if file not found after download attempt
        except Exception as e:
            logging.error(f"Error processing FIMI file {fimi_file_path}: {e}")
            return {}
        return formal_context
        
    def _save_formal_context_shard(self, context_shard: Dict[int, Set[int]], file_path: str):
        """Saves a shard of the formal context to a file."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
                for obj_id, attributes in sorted(context_shard.items()): # Sort for consistent output
                    # Format: object_id: {attr1, attr2, ...}
                    f.write(f"{obj_id}: {repr(attributes)}\n") # Use repr for set string representation
            logging.debug(f"Saved context shard to {file_path} with {len(context_shard)} objects.")
        except IOError as e:
            logging.error(f"Error saving context shard to {file_path}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while saving shard to {file_path}: {e}")


    def _split_formal_context(self,
                             full_formal_context: Dict[int, Set[int]],
                             num_providers: int,
                             splitting_strategy: str = "IID",
                             iid_ensure_all_data_used: bool = True,
                             non_iid_skew_factor: float = 0.5, # Lower means more skew
                             non_iid_type: str = "quantity" # "quantity" or "attribute"
                             ) -> List[Dict[int, Set[int]]]:
        """
        Splits the full formal context among a number of providers.

        Args:
            full_formal_context (Dict[int, Set[int]]): The complete dataset.
            num_providers (int): The number of providers to split data for.
            splitting_strategy (str): "IID" or "Non-IID".
            iid_ensure_all_data_used (bool): For IID, if True, distributes remainder objects.
            non_iid_skew_factor (float): For Non-IID, controls skewness (e.g., Dirichlet alpha).
            non_iid_type (str): For Non-IID, "quantity" (skew by number of objects) or
                                "attribute" (skew by attribute presence).

        Returns:
            List[Dict[int, Set[int]]]: A list of formal context shards, one for each provider.
        """
        if not full_formal_context:
            logging.warning("Full formal context is empty. Returning empty shards.")
            return [{} for _ in range(num_providers)]
        if num_providers <= 0:
            logging.error("Number of providers must be positive.")
            return []

        object_ids = list(full_formal_context.keys())
        random.shuffle(object_ids) # Shuffle for randomness in assignment
        
        provider_shards: List[Dict[int, Set[int]]] = [{} for _ in range(num_providers)]

        if splitting_strategy.upper() == "IID":
            objects_per_provider = len(object_ids) // num_providers
            remainder_objects = len(object_ids) % num_providers
            
            current_idx = 0
            for i in range(num_providers):
                num_to_assign = objects_per_provider
                if iid_ensure_all_data_used and remainder_objects > 0:
                    num_to_assign += 1
                    remainder_objects -= 1
                
                assigned_object_ids = object_ids[current_idx : current_idx + num_to_assign]
                for obj_id in assigned_object_ids:
                    provider_shards[i][obj_id] = full_formal_context[obj_id]
                current_idx += num_to_assign
            logging.info(f"Split data IID for {num_providers} providers.")

        elif splitting_strategy.upper() == "NON-IID":
            if non_iid_type == "quantity":
                # Dirichlet distribution for quantity skew
                # Alpha values for Dirichlet (smaller alpha = more skew)
                dirichlet_alphas = np.full(num_providers, non_iid_skew_factor)
                proportions = dirichlet.rvs(dirichlet_alphas, size=1)[0]
                
                assigned_counts = np.zeros(num_providers, dtype=int)
                # Distribute objects based on proportions
                for obj_idx, obj_id in enumerate(object_ids):
                    # Assign to provider based on weighted random choice according to proportions
                    # This ensures objects are distributed even if proportions are very skewed
                    chosen_provider_idx = random.choices(range(num_providers), weights=proportions, k=1)[0]
                    provider_shards[chosen_provider_idx][obj_id] = full_formal_context[obj_id]
                    assigned_counts[chosen_provider_idx] += 1
                logging.info(f"Split data Non-IID (quantity skew, factor={non_iid_skew_factor}) for {num_providers} providers. Counts: {assigned_counts.tolist()}")

            elif non_iid_type == "attribute":
                # This is more complex: requires identifying dominant attributes or classes
                # For FCA, one way is to sort objects by a primary attribute or cluster them
                # and then assign clusters to providers.
                # A simpler proxy: create skew based on object ID modulo N (less realistic but simple)
                # For a more realistic attribute skew, you'd need a specific strategy.
                # Here's a placeholder for a more advanced attribute-based skew.
                # For now, falls back to quantity skew if not implemented.
                logging.warning("Non-IID 'attribute' skew is complex and uses a simplified approach or falls back to quantity.")
                # Fallback to quantity skew for this example, or implement specific attribute logic
                # For demonstration, let's use a simple modulo assignment for attribute-like skew
                all_attributes_list = sorted(list(set.union(*full_formal_context.values()) if full_formal_context else set()))
                if not all_attributes_list:
                    logging.warning("No attributes found for Non-IID attribute skew. Falling back to quantity.")
                    return self._split_formal_context(full_formal_context, num_providers, "NON-IID", non_iid_type="quantity", non_iid_skew_factor=non_iid_skew_factor)

                # Example: Assign objects to providers based on presence of certain 'dominant' attributes
                # This is a simplified version.
                num_dominant_attrs_per_provider = max(1, len(all_attributes_list) // num_providers)
                obj_assignment_priority = [[] for _ in range(num_providers)]

                for obj_id in object_ids:
                    obj_attrs = full_formal_context[obj_id]
                    assigned = False
                    for i in range(num_providers):
                        # Define some rule, e.g., provider 'i' focuses on attributes i*k to (i+1)*k
                        start_attr_idx = i * num_dominant_attrs_per_provider
                        end_attr_idx = (i + 1) * num_dominant_attrs_per_provider
                        dominant_attrs_for_provider = set(all_attributes_list[start_attr_idx:end_attr_idx])
                        if obj_attrs.intersection(dominant_attrs_for_provider):
                            obj_assignment_priority[i].append(obj_id)
                            assigned = True
                            break # Assign to first matching provider for simplicity
                    if not assigned: # If no dominant attribute match, assign to a random provider
                        obj_assignment_priority[random.randint(0, num_providers - 1)].append(obj_id)
                
                # Distribute objects from priority lists, ensuring no provider is empty if possible
                temp_object_ids_pool = list(object_ids) # objects to be assigned
                for i in range(num_providers):
                    # Try to assign from priority list first
                    if obj_assignment_priority[i]:
                        obj_to_assign = obj_assignment_priority[i].pop(0)
                        if obj_to_assign in temp_object_ids_pool:
                             provider_shards[i][obj_to_assign] = full_formal_context[obj_to_assign]
                             temp_object_ids_pool.remove(obj_to_assign)
                # Distribute remaining objects somewhat evenly
                for obj_id_rem in temp_object_ids_pool:
                    provider_shards[random.randint(0, num_providers-1)][obj_id_rem] = full_formal_context[obj_id_rem]

                logging.info(f"Split data Non-IID (simplified attribute skew) for {num_providers} providers.")
            else:
                logging.error(f"Unknown Non-IID type: {non_iid_type}")
                return [{} for _ in range(num_providers)]
        else:
            logging.error(f"Unknown splitting strategy: {splitting_strategy}")
            return [{} for _ in range(num_providers)]
            
        # Ensure no provider is empty if there's data and multiple providers
        if len(object_ids) > 0 and num_providers > 0:
            for i in range(num_providers):
                if not provider_shards[i]:
                    # Find a provider with more than one object to steal from
                    non_empty_providers = [j for j, shard in enumerate(provider_shards) if len(shard) > 1]
                    if non_empty_providers:
                        steal_from_provider_idx = random.choice(non_empty_providers)
                        obj_id_to_move = random.choice(list(provider_shards[steal_from_provider_idx].keys()))
                        provider_shards[i][obj_id_to_move] = provider_shards[steal_from_provider_idx].pop(obj_id_to_move)
                        logging.debug(f"Moved object {obj_id_to_move} to empty provider {i} from provider {steal_from_provider_idx}")
                    elif any(provider_shards): # if some providers have single items
                         # find any non-empty provider
                        steal_from_provider_idx = next((j for j, shard in enumerate(provider_shards) if shard), -1)
                        if steal_from_provider_idx != -1 and steal_from_provider_idx != i: # ensure not stealing from self if it's the only one
                            obj_id_to_move = list(provider_shards[steal_from_provider_idx].keys())[0]
                            provider_shards[i][obj_id_to_move] = provider_shards[steal_from_provider_idx].pop(obj_id_to_move)
                            logging.debug(f"Moved object {obj_id_to_move} to empty provider {i} from provider {steal_from_provider_idx}")


        return provider_shards

    def prepare_all_datasets(self):
        """
        Orchestrates the preparation of all datasets defined in the configuration.
        Downloads, processes, splits, and saves each dataset.
        """
        if 'datasets' not in self.config or not self.config['datasets']:
            logging.error("No datasets defined in the configuration.")
            return

        for ds_config in self.config.get('datasets', []):
            dataset_name = ds_config.get('name')
            dataset_url = ds_config.get('url')
            dataset_format = ds_config.get('format', 'fimi') # Assume FIMI if not specified

            if not dataset_name or not dataset_url:
                logging.warning(f"Skipping dataset due to missing name or URL: {ds_config}")
                continue

            logging.info(f"--- Processing dataset: {dataset_name} ---")
            
            raw_data_dir = os.path.join(self.base_output_dir, "_raw_data")
            raw_file_path = os.path.join(raw_data_dir, f"{dataset_name}.dat")

            if not os.path.exists(raw_file_path):
                if not self._download_file(dataset_url, raw_file_path):
                    logging.error(f"Failed to download {dataset_name}. Skipping.")
                    continue
            else:
                logging.info(f"Raw data for {dataset_name} already exists at {raw_file_path}.")

            # Process raw data to formal context
            full_formal_context: Dict[int, Set[int]] = {}
            if dataset_format.lower() == 'fimi':
                full_formal_context = self._process_fimi_to_formal_context(raw_file_path)
            else:
                logging.warning(f"Unsupported dataset format '{dataset_format}' for {dataset_name}. Skipping processing.")
                continue
            
            if not full_formal_context:
                logging.error(f"Formal context for {dataset_name} is empty after processing. Skipping splits.")
                continue

            # Save the full processed context (optional)
            full_context_output_path = os.path.join(self.base_output_dir, dataset_name, f"{dataset_name}_full_formal_context.txt")
            self._save_formal_context_shard(full_formal_context, full_context_output_path)
            logging.info(f"Full formal context for {dataset_name} saved to {full_context_output_path}")

            # Splitting configurations
            splitting_configs = ds_config.get('splitting_setups', [])
            for split_setup in splitting_configs:
                num_providers = split_setup.get('num_providers')
                strategy = split_setup.get('strategy', 'IID')
                non_iid_type = split_setup.get('non_iid_type', 'quantity')
                non_iid_skew = split_setup.get('non_iid_skew_factor', 0.5)

                if not num_providers:
                    logging.warning(f"Skipping split for {dataset_name} due to missing 'num_providers'.")
                    continue
                
                logging.info(f"Splitting {dataset_name} for {num_providers} providers using {strategy} strategy "
                             f"(Non-IID type: {non_iid_type}, Skew: {non_iid_skew if strategy=='Non-IID' else 'N/A'}).")

                provider_shards = self._split_formal_context(
                    full_formal_context,
                    num_providers,
                    splitting_strategy=strategy,
                    non_iid_type=non_iid_type,
                    non_iid_skew_factor=non_iid_skew
                )

                # Save provider shards
                split_output_base = os.path.join(self.base_output_dir, dataset_name, f"providers_{num_providers}", strategy.lower())
                if strategy.upper() == 'NON-IID':
                    split_output_base = os.path.join(split_output_base, f"{non_iid_type}_skew{str(non_iid_skew).replace('.', 'p')}")
                
                for i, shard_data in enumerate(provider_shards):
                    provider_file_path = os.path.join(split_output_base, f"provider_{i+1}.txt")
                    if shard_data: # Only save if shard is not empty
                        self._save_formal_context_shard(shard_data, provider_file_path)
                    else:
                        logging.warning(f"Provider {i+1} for {dataset_name} (N={num_providers}, {strategy}) has an empty shard. Not saving file.")
                logging.info(f"Saved shards for {dataset_name} (N={num_providers}, {strategy}) to {split_output_base}")
        logging.info("--- All dataset preparation finished. ---")

if __name__ == '__main__':
    # Create a dummy config directory and file for the example
    if not os.path.exists("config"):
        os.makedirs("config")
    
    dummy_dataset_config = {
        "datasets": [
            # {
            #     "name": "mushroom_example",
            #     "url": "http://fimi.uantwerpen.be/data/mushroom.dat", # Standard FIMI dataset
            #     "format": "fimi",
            #     "splitting_setups": [
            #         {
            #             "num_providers": 5,
            #             "strategy": "IID"
            #         },
            #         {
            #             "num_providers": 5,
            #             "strategy": "Non-IID",
            #             "non_iid_type": "quantity", # "quantity" or "attribute"
            #             "non_iid_skew_factor": 0.3 # Smaller means more skew
            #         }
            #     ]
            # },
            {
                "name": "chess_example",
                "url": "http://fimi.uantwerpen.be/data/chess.dat",
                "format": "fimi",
                "splitting_setups": [
                    {
                        "num_providers": 5,
                        "strategy": "IID"
                    }
                ]
            }
        ]
    }
    with open("config/config.yml", "w") as f:
        yaml.dump(dummy_dataset_config, f, sort_keys=False)
    
    logging.info("Created dummy 'config/config.yml'. Starting dataset preparation...")
    
    manager = DatasetManager(config_path="config/config.yml", base_output_dir="federated_data_output")
    manager.prepare_all_datasets()

    logging.info("Dataset preparation script finished. Check the 'federated_data_output' directory.")
    logging.info("To use this with your actors:")
    logging.info("1. Ensure the 'federated_data_output' directory (or your chosen output) is accessible by ALM containers (e.g., via Docker volumes).")
    logging.info("2. When an AGM invites an ALM, it should tell the ALM the specific path to its data shard, e.g., "
                 "'federated_data_output/mushroom_example/providers_5/iid/provider_1.txt'.")
    logging.info("3. The ALMActor should then use this path to initialize its FedFCA_core.Provider instance.")

