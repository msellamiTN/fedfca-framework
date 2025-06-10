import numpy as np
import os
import random
import shutil
import requests
import time
import logging
from typing import Dict, List, Set, Tuple, Union, Any
from scipy.stats import dirichlet
from pathlib import Path

class DataLoader:
    def __init__(self, logger: logging.Logger = None):
        """
        Initialize the DataLoader.
        
        Args:
            logger: Optional logger instance. If not provided, a default logger will be created.
        """
        self.start_time = time.time()
        self.end_time = None
        self.stats = {}
        self.config = self._load_config()
        self.logger = logger or self._setup_default_logger()
        
    def _setup_default_logger(self) -> logging.Logger:
        """Set up a default logger if none is provided."""
        logger = logging.getLogger('DataLoader')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
        
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from config file or environment variables.
        
        Returns:
            Dict containing configuration parameters.
        """
        # Default configuration
        config = {
            'data_dir': os.getenv('DATA_DIR', 'data'),
            'cache_dir': os.getenv('CACHE_DIR', '.cache'),
            'max_retries': int(os.getenv('MAX_RETRIES', '3')),
            'timeout': int(os.getenv('TIMEOUT', '30'))
        }
        return config
    
    def load_from_file(self, file_path: Union[str, Path], format: str = 'auto') -> Dict[int, Set[int]]:
        """
        Load data from a file and convert it to a formal context dictionary.
        
        Args:
            file_path: Path to the input file.
            format: Format of the input file. Options: 'auto', 'fimi', 'json', 'txt'.
                   If 'auto', the format will be inferred from the file extension.
        
        Returns:
            Dict[int, Set[int]]: Formal context where keys are object IDs and values are sets of attribute IDs.
            
        Raises:
            FileNotFoundError: If the input file does not exist.
            ValueError: If the file format is not supported or the file is malformed.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")
            
        if format == 'auto':
            format = file_path.suffix[1:].lower()  # Remove the dot from extension
            
        self.logger.info(f"Loading data from {file_path} (format: {format})")
        
        try:
            if format in ('fimi', 'dat', 'data'):
                return self._load_fimi_format(file_path)
            elif format == 'json':
                return self._load_json_format(file_path)
            elif format == 'txt':
                return self._load_txt_format(file_path)
            else:
                raise ValueError(f"Unsupported file format: {format}")
        except Exception as e:
            self.logger.error(f"Error loading file {file_path}: {str(e)}")
            raise
    
    def _load_fimi_format(self, file_path: Path) -> Dict[int, Set[int]]:
        """
        Load data in FIMI format where each line contains space-separated integers.
        
        Args:
            file_path: Path to the input file.
            
        Returns:
            Dict[int, Set[int]]: Formal context dictionary.
        """
        formal_context = {}
        with open(file_path, 'r') as f:
            for obj_idx, line in enumerate(f, 1):
                try:
                    attributes = list(map(int, line.strip().split()))
                    formal_context[obj_idx] = set(attributes)
                except ValueError as e:
                    self.logger.warning(f"Skipping malformed line {obj_idx}: {line.strip()}")
        return formal_context
    
    def _load_json_format(self, file_path: Path) -> Dict[int, Set[int]]:
        """
        Load data from a JSON file.
        
        Args:
            file_path: Path to the JSON file.
            
        Returns:
            Dict[int, Set[int]]: Formal context dictionary.
        """
        import json
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Convert string keys to integers and values to sets
        return {int(k): set(v) if isinstance(v, list) else {v} for k, v in data.items()}
    
    def _load_txt_format(self, file_path: Path) -> Dict[int, Set[int]]:
        """
        Load data from a text file where each line is in the format "obj_id: {attr1,attr2,...}".
        
        Args:
            file_path: Path to the text file.
            
        Returns:
            Dict[int, Set[int]]: Formal context dictionary.
        """
        import ast
        formal_context = {}
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                    
                try:
                    obj_id, attrs_str = line.split(':', 1)
                    obj_id = int(obj_id.strip())
                    attrs = ast.literal_eval(attrs_str.strip())
                    formal_context[obj_id] = set(attrs) if isinstance(attrs, (list, set)) else {attrs}
                except (ValueError, SyntaxError) as e:
                    self.logger.warning(f"Skipping malformed line: {line}")
        return formal_context
    
    def download_file(self, url: str, filename: str = None, max_retries: int = None) -> str:
        """
        Download a file from a URL and save it locally.
        
        Args:
            url: URL of the file to download.
            filename: Local filename to save as. If None, will use the basename from the URL.
            max_retries: Maximum number of download retry attempts.
            
        Returns:
            str: Path to the downloaded file.
            
        Raises:
            requests.exceptions.RequestException: If the download fails after all retries.
        """
        if filename is None:
            filename = os.path.basename(url)
            
        max_retries = max_retries or self.config.get('max_retries', 3)
        timeout = self.config.get('timeout', 30)
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Downloading {url} (attempt {attempt + 1}/{max_retries})")
                response = requests.get(url, stream=True, timeout=timeout)
                response.raise_for_status()
                
                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                self.logger.info(f"Successfully downloaded {filename}")
                return filename
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Failed to download {url} after {max_retries} attempts")
                    raise
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def process_fimi_dataset(self, input_file: Union[str, Path]) -> Dict[int, Set[int]]:
        """
        Process a FIMI format dataset into a formal context dictionary.
        
        Args:
            input_file: Path to the input file in FIMI format.
            
        Returns:
            Dict[int, Set[int]]: Formal context where keys are object IDs and values are sets of attributes.
        """
        return self._load_fimi_format(Path(input_file))


    def save_formal_context(self, formal_context: Dict[int, Set[int]], output_path: Union[str, Path]) -> None:
        """
        Save a formal context to a file.
        
        Args:
            formal_context: Dictionary mapping object IDs to sets of attributes.
            output_path: Path where the output file will be saved.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            for obj_id, attrs in sorted(formal_context.items()):
                attrs_str = ','.join(map(str, sorted(attrs)))
                f.write(f"{obj_id}: {{{attrs_str}}}\\n")
        
        self.logger.info(f"Formal context saved to {output_path}")


        # Core Partitioning Function
        def split_dataset_into_providers_fca(self,
            formal_context_dict: Dict[int, Set[int]],
            num_providers: int,
            splitting_type: str = "IID",
            skew_factor: float = 1.0,
            attribute_distribution_skew: bool = False,
            quantity_skew: bool = False,
            max_objects_per_provider: int = None,
        ):
            """
            Enhanced splitting of formal contexts for FCA in federated learning.
            Addresses empty providers and imbalanced distributions.
            """
            num_objects = len(formal_context_dict)
            object_ids = np.array(list(formal_context_dict.keys()))
            all_attributes = list({attr for attrs in formal_context_dict.values() for attr in attrs})

            if splitting_type == "IID":
                # Equal distribution of objects
                shuffled_indices = np.random.permutation(object_ids)
                provider_indices = np.array_split(shuffled_indices, num_providers)

            elif splitting_type == "Non-IID":
                provider_indices = [[] for _ in range(num_providers)]

                if attribute_distribution_skew:
                    # Attribute-based skew
                    attribute_proportions = dirichlet.rvs([skew_factor] * num_providers, size=len(all_attributes))

                    for obj_idx, attributes in formal_context_dict.items():
                        provider_weights = np.zeros(num_providers)
                        for attr in attributes:
                            if attr in all_attributes:
                                attr_idx = all_attributes.index(attr)
                                provider_weights += attribute_proportions[attr_idx]

                        # Probabilistic assignment instead of argmax
                        provider_choice = np.random.choice(
                            range(num_providers),
                            p=provider_weights / provider_weights.sum()
                        )
                        provider_indices[provider_choice].append(obj_idx)

                elif quantity_skew:
                    # Quantity skew
                    proportions = dirichlet.rvs([skew_factor] * num_providers, size=1)[0]
                    shuffled_object_ids = np.random.permutation(object_ids)

                    for obj_idx in shuffled_object_ids:
                        provider_choice = np.random.choice(num_providers, p=proportions)
                        if max_objects_per_provider is not None and len(provider_indices[provider_choice]) >= max_objects_per_provider:
                            available_providers = [i for i in range(num_providers) if len(provider_indices[i]) < max_objects_per_provider]
                            if available_providers:
                                provider_choice = np.random.choice(available_providers)
                            else:
                                break
                        provider_indices[provider_choice].append(obj_idx)

                # Redistribute objects to empty providers
                for provider_idx, indices in enumerate(provider_indices):
                    if not indices:
                        fullest_provider = max(range(num_providers), key=lambda x: len(provider_indices[x]))
                        provider_indices[provider_idx].append(provider_indices[fullest_provider].pop())

            else:
                raise ValueError(f"Unknown splitting_type: {splitting_type}")

            # Create provider datasets
            provider_data = []
            for indices in provider_indices:
                provider_dict = {
                    obj_idx: formal_context_dict.get(obj_idx)
                    for obj_idx in indices
                    if obj_idx in formal_context_dict
                }
                if provider_dict:
                    provider_data.append(provider_dict)
                else:
                    print(f"Warning: Empty provider detected.")

            # Validate total distribution
            total_assigned_objects = sum(len(provider) for provider in provider_data)
            if total_assigned_objects != num_objects:
                raise ValueError("Mismatch in total objects distributed among providers.")

            return provider_data


        # Save Provider Data
        def save_provider_data(self, dataset_name, num_providers, provider_idx, provider_data, splitting_type):
            """
            Saves provider-specific data in the format required by FasterFCA.
            
            Args:
                dataset_name (str): Name of the dataset
                num_providers (int): Total number of providers
                provider_idx (int): Index of the current provider
                provider_data (dict): Dictionary containing object-attribute mappings
                splitting_type (str): Type of data splitting (IID/Non-IID)
                
            Returns:
                str: Path to the saved provider data file
                
            Example output format:
                1: {2, 3, 5}
                2: {4, 5, 6}
            """
            # Create directory if it doesn't exist
            output_dir = os.path.join("data", dataset_name, splitting_type, f"{num_providers}_providers")
            os.makedirs(output_dir, exist_ok=True)
            
            # Create output filename
            output_file = os.path.join(output_dir, f"provider_{provider_idx + 1}.data")
            
            try:
                with open(output_file, 'w') as f:
                    for obj_id, attrs in provider_data.items():
                        # Ensure attributes are sorted for consistency
                        if isinstance(attrs, (list, tuple, set)):
                            # Convert to set to remove duplicates, then sort
                            sorted_attrs = sorted(set(attrs))
                            # Format: "obj_id: {attr1,attr2,...}"
                            f.write(f"{obj_id}: {str(set(sorted_attrs)).replace(' ', '')}\n")
                        else:
                            # If attrs is not a collection, write it as is
                            f.write(f"{obj_id}: {attrs}\n")
                
                print(f"Successfully saved provider {provider_idx + 1} data to {output_file}")
                return output_file
                
            except Exception as e:
                print(f"Error saving provider {provider_idx + 1} data: {str(e)}")


        # Create Federated Datasets
        def create_federated_datasets(self,
            dataset_urls,
            num_providers_list,
            splitting_types,
            skew_factor=1.0,
            attribute_distribution_skew=False,
            quantity_skew=False,
            max_objects_per_provider=None,
        ):
            """Generates federated datasets with IID and Non-IID partitioning."""
            for dataset_name, url in dataset_urls.items():
                print(f"Fetching {dataset_name} dataset...")
                input_file = f"{dataset_name}.dat"
                if not os.path.exists(input_file):
                    downloaded_file = download_file(url, input_file)
                    if not downloaded_file:
                        print(f"Failed to download {dataset_name}. Please download it manually.")
                        continue

                formal_context_dict = process_fimi_dataset(input_file)
                if os.path.exists(dataset_name):
                    shutil.rmtree(dataset_name)
                os.makedirs(dataset_name, exist_ok=True)

                save_formal_context(formal_context_dict, os.path.join(dataset_name, f"{dataset_name}.data"))

                for num_providers in num_providers_list:
                    for splitting_type in splitting_types:
                        provider_data_list = split_dataset_into_providers_fca(
                            formal_context_dict,
                            num_providers,
                            splitting_type,
                            skew_factor,
                            attribute_distribution_skew,
                            quantity_skew,
                            max_objects_per_provider,
                        )

                        for provider_idx, provider_data in enumerate(provider_data_list, start=1):
                            save_provider_data(dataset_name, num_providers, provider_idx, provider_data, splitting_type)

    def run_sample(self):
        # Example Usage
        dataset_urls = {
            "mushroom" : "http://fimi.uantwerpen.be/data/mushroom.dat",
            "chess" : "http://fimi.uantwerpen.be/data/chess.dat",
            "Skin": "https://raw.githubusercontent.com/msellamiTN/fedfca-framework/refs/heads/main/data/fimi/Skin.dat",
            "RecordLink": "https://raw.githubusercontent.com/msellamiTN/fedfca-framework/refs/heads/main/data/fimi/RecordLink.dat",
            "connect": "http://fimi.uantwerpen.be/data/connect.dat",
            "T10I4D100K": "http://fimi.uantwerpen.be/data/T10I4D100K.dat",
        }
        num_providers_list = [50, 100,150,200,250]
        splitting_types = ["IID", "Non-IID"]
        skew_factor = 0.2
        max_objects_per_provider = 1000

        self.create_federated_datasets(
            dataset_urls=dataset_urls,
            num_providers_list=num_providers_list,
            splitting_types=splitting_types,
            skew_factor=skew_factor,
            attribute_distribution_skew=True,
            quantity_skew=True,
            max_objects_per_provider=max_objects_per_provider,
        )

    def load_config(self):
        """Load configuration from YAML file."""
        try:
            with open(self.config_file, 'r') as file:
                self.config = yaml.safe_load(file)
                return self.config
        except Exception as e:
            logging.error(f"Error loading config: {e}")
            self.config = {"default": "configuration"}
            return self.config
