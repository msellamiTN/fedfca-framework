"""
CryptoManager module for FedFCA framework.
Handles encryption, decryption, and key management using Redis KMS.
"""
import os
import uuid
import base64
import logging
from datetime import datetime
from typing import Optional, Dict, Any, Union, Tuple
from cryptography.fernet import Fernet, InvalidToken, InvalidSignature
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from .RedisKMS import RedisKMS
import threading

class CryptoManager:
    """
    CryptoManager handles encryption, decryption, and key management.
    It uses Fernet (symmetric encryption) for data encryption/decryption
    and Redis KMS for key storage and management.
    """
    
    def __init__(self, kms: RedisKMS, logger: Optional[logging.Logger] = None):
        """
        Initialize CryptoManager with a KMS instance.
        
        Args:
            kms: An instance of RedisKMS for key management
            logger: Optional logger instance
        """
        self.kms = kms
        self.redis_kms = kms  # Alias for backward compatibility
        self.logger = logger or logging.getLogger(__name__)
        self._fernet_instances: Dict[str, Fernet] = {}
        self._key_cache: Dict[str, bytes] = {}
        self._key_versions: Dict[str, int] = {}
        self._key_expirations: Dict[str, float] = {}
        self._key_lock = threading.Lock()
        self._cache_ttl = 3600  # 1 hour default cache TTL
        self.cipher_suites = {}  # Store Fernet instances
        self.local_keys = {}  # Store local key cache

    def _ensure_fernet(self, key_id: str) -> bool:
        """
        Ensure a Fernet instance exists for the given key ID.
        
        Args:
            key_id: The ID of the key to ensure Fernet initialization for
            
        Returns:
            bool: True if Fernet was successfully initialized, False otherwise
        """
        # Check if already initialized
        if key_id in self.cipher_suites:
            self.logger.debug(f"Fernet instance for key {key_id} already exists")
            return True
        
        # Try to get the key data
        key_data = self.get_key(key_id)
        if not key_data:
            self.logger.error(f"Key not found in Redis KMS: {key_id}")
            return False
        
        try:
            self.logger.debug(f"Initializing Fernet for key: {key_id}")
            
            # Handle different key data formats
            key_material = None
            if isinstance(key_data, dict):
                # Extract key material from dictionary
                key_material = key_data.get('key_material')
                if not key_material:
                    self.logger.error(f"No key_material found in key data for {key_id}")
                    return False
            else:
                # Handle raw key data (legacy format)
                key_material = key_data
            
            # Ensure key_material is in bytes format
            key_bytes = None
            if isinstance(key_material, str):
                try:
                    # Try to decode as base64 first (preferred format)
                    key_bytes = base64.urlsafe_b64decode(key_material)
                    self.logger.debug(f"Successfully decoded key {key_id} from base64")
                except Exception as e:
                    self.logger.error(f"Key {key_id} is not valid base64: {e}")
                    return False
            elif isinstance(key_material, bytes):
                key_bytes = key_material
            else:
                self.logger.error(f"Invalid key_material type for {key_id}: {type(key_material)}")
                return False
            
            # Ensure key is exactly 32 bytes
            if len(key_bytes) != 32:
                self.logger.error(f"Invalid key length for {key_id}: {len(key_bytes)} bytes (expected 32)")
                # Try to pad or truncate the key if it's close to 32 bytes
                if 28 <= len(key_bytes) < 32:
                    self.logger.warning("Key is close to 32 bytes, attempting to pad it")
                    key_bytes = key_bytes.ljust(32, b'\0')[:32]
                elif len(key_bytes) > 32:
                    self.logger.warning("Key is too long, truncating to 32 bytes")
                    key_bytes = key_bytes[:32]
                else:
                    return False
            
            # Initialize Fernet with the key
            try:
                # Ensure key is URL-safe base64 encoded
                fernet_key = base64.urlsafe_b64encode(key_bytes)
                self.cipher_suites[key_id] = Fernet(fernet_key)
                self.logger.info(f"Successfully initialized Fernet for key: {key_id}")
                
                # Store the key in local cache
                with self._key_lock:
                    self._key_cache[key_id] = key_bytes
                    self._key_versions[key_id] = self._key_versions.get(key_id, 0) + 1
                    self._key_expirations[key_id] = datetime.now().timestamp() + self._cache_ttl
                
                # Verify the Fernet instance works with a test encryption/decryption
                try:
                    test_data = b"test"
                    encrypted = self.cipher_suites[key_id].encrypt(test_data)
                    decrypted = self.cipher_suites[key_id].decrypt(encrypted)
                    
                    if decrypted != test_data:
                        self.logger.error(f"Fernet verification failed for key {key_id}")
                        del self.cipher_suites[key_id]
                        return False
                    
                    self.logger.debug(f"Successfully verified Fernet key {key_id} with test encryption/decryption")
                    return True
                    
                except Exception as verify_error:
                    self.logger.error(f"Fernet verification failed for key {key_id}: {str(verify_error)}")
                    if key_id in self.cipher_suites:
                        del self.cipher_suites[key_id]
                    return False
                
            except Exception as e:
                self.logger.error(f"Failed to initialize Fernet for key {key_id}: {str(e)}")
                if 'fernet_key' in locals():
                    key_sample = fernet_key[:10].decode('utf-8', errors='replace') if isinstance(fernet_key, bytes) else str(fernet_key)[:10]
                    self.logger.debug(f"Key material (first 10 chars): {key_sample}")
                return False
                
        except Exception as e:
            self.logger.error(
                f"Unexpected error initializing Fernet for key {key_id}: {e}",
                exc_info=True
            )
            return False
    
    def generate_key(self, key_id: Optional[str] = None, key_type: str = 'fernet', 
                    key_size: int = 32, expiration: Optional[int] = None,
                    federation_id: Optional[str] = None) -> str:
        """
        Generate a new encryption key and store it in Redis KMS.
        
        Args:
            key_id: Optional custom key ID. If None, a random ID will be generated.
            key_type: Type of key to generate ('fernet' or 'raw').
            key_size: Size of the key in bytes (for raw keys).
            expiration: Optional TTL in seconds for the key in Redis.
            federation_id: Optional federation ID this key belongs to.
            
        Returns:
            str: The generated key ID.
            
        Raises:
            ValueError: If key_type is invalid or key generation fails.
            RuntimeError: If key storage fails.
        """
        if key_id is None:
            key_id = f"key_{uuid.uuid4().hex[:8]}"
        
        self.logger.info(f"Generating new {key_type} key with ID: {key_id}")
        
        try:
            if key_type == 'fernet':
                key = Fernet.generate_key()
                key_data = {
                    'key_type': 'fernet',
                    'key_material': key.decode('utf-8'),  # Fernet key is already URL-safe base64
                    'created_at': datetime.utcnow().isoformat(),
                    'algorithm': 'AES-128-CBC',
                    'size': 32
                }
            elif key_type == 'raw':
                if key_size not in [16, 24, 32]:
                    raise ValueError("Key size must be 16, 24, or 32 bytes for raw keys")
                key = os.urandom(key_size)
                key_data = {
                    'key_type': 'raw',
                    'key_material': base64.urlsafe_b64encode(key).decode('utf-8'),
                    'created_at': datetime.utcnow().isoformat(),
                    'algorithm': 'AES-256-CBC',
                    'size': key_size
                }
            else:
                raise ValueError(f"Unsupported key type: {key_type}")
            
            # Add federation ID if provided
            if federation_id:
                key_data['federation_id'] = federation_id
            
            # Store in Redis KMS with validation
            if not self.redis_kms.store_key(key_id, key_data, expiration, federation_id):
                raise RuntimeError(f"Failed to store key {key_id} in Redis KMS")
            
            # Cache the key locally
            self.local_keys[key_id] = key_data
            
            # Ensure Fernet instance is created for fernet keys
            if key_type == 'fernet' and not self._ensure_fernet(key_id):
                self.logger.warning(f"Generated key {key_id} but failed to initialize Fernet")
            
            self.logger.info(f"Successfully generated and stored {key_type} key: {key_id}")
            return key_id
            
        except Exception as e:
            self.logger.error(f"Failed to generate key {key_id}: {str(e)}", exc_info=True)
            # Clean up if key was partially created
            if 'key_id' in locals():
                try:
                    self.redis_kms.delete_key(key_id)
                except:
                    pass
            raise
    
    def get_key(self, key_id: str) -> Optional[Union[bytes, Dict[str, Any]]]:
        """
        Get a key from Redis KMS or local cache.
        
        Args:
            key_id: The ID of the key to retrieve.
            
        Returns:
            The key data (bytes or dict) if found, None otherwise.
        """
        self.logger.debug(f"Getting key: {key_id}")
        
        # Check local cache first
        if key_id in self.local_keys:
            self.logger.debug(f"Found key {key_id} in local cache")
            return self.local_keys[key_id]
            
        # Try to get from Redis KMS
        try:
            key_data = self.kms.get_key(key_id)
            if key_data is None:
                self.logger.debug(f"Key {key_id} not found in Redis KMS")
                return None
                
            self.logger.debug(f"Retrieved key {key_id} from Redis KMS")
            
            # Ensure the key data has the expected structure
            if isinstance(key_data, dict):
                # If key_material is present, ensure it's in the correct format
                if 'key_material' in key_data:
                    key_material = key_data['key_material']
                    
                    # If key_material is bytes, ensure it's a valid Fernet key
                    if isinstance(key_material, bytes):
                        if len(key_material) == 32:
                            # Convert to base64 string for storage consistency
                            key_data['key_material'] = base64.urlsafe_b64encode(key_material).decode('utf-8')
                        else:
                            self.logger.error(f"Invalid key length for {key_id}: {len(key_material)} bytes (expected 32)")
                            return None
                            
                    # If key_material is a string, ensure it's valid base64
                    elif isinstance(key_material, str):
                        try:
                            # Try to decode to verify it's valid base64
                            decoded = base64.urlsafe_b64decode(key_material)
                            if len(decoded) != 32:
                                self.logger.error(f"Decoded key has invalid length for {key_id}: {len(decoded)} bytes (expected 32)")
                                return None
                        except Exception as e:
                            self.logger.error(f"Invalid base64 key material for {key_id}: {e}")
                            return None
                    
                    # Cache the key locally
                    self.local_keys[key_id] = key_data
                    return key_data
                
                self.logger.error(f"Key {key_id} is missing 'key_material' field")
                return None
            
            # Handle case where key_data is not a dict (legacy format)
            if isinstance(key_data, (str, bytes)):
                # Try to decode as base64
                try:
                    if isinstance(key_data, str):
                        key_bytes = base64.urlsafe_b64decode(key_data)
                    else:
                        key_bytes = key_data
                            
                    if len(key_bytes) == 32:
                        # Convert to new format
                        new_key_data = {
                            'key_type': 'fernet',
                            'key_material': base64.urlsafe_b64encode(key_bytes).decode('utf-8'),
                            'created_at': str(datetime.utcnow()),
                            'algorithm': 'AES-256-CBC',
                            'size': 32
                        }
                        # Update in Redis
                        if self.kms.store_key(key_id, new_key_data):
                            self.local_keys[key_id] = new_key_data
                            return new_key_data
                        self.logger.error(f"Failed to update legacy key {key_id} to new format")
                        return None
                        
                    self.logger.error(
                        f"Legacy key {key_id} has invalid length: {len(key_bytes)} bytes (expected 32)"
                    )
                    return None
                except Exception as e:
                    self.logger.error(f"Failed to process legacy key {key_id}: {e}")
                    return None
                
            return None
                
        except Exception as e:
            self.logger.error(f"Error getting key {key_id} from Redis KMS: {e}")
            return None

    def store_key(
        self,
        key_id: str,
        key_data: Union[bytes, Dict[str, Any]],
        expiration: Optional[int] = None
    ) -> bool:
        """
        Store a key in Redis KMS.
        
        Args:
            key_id: The ID for the key.
            key_data: The key data to store (bytes or dict).
            expiration: Optional TTL in seconds.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        if not key_id or key_data is None:
            raise ValueError("key_id and key_data are required")
            
        result = self.kms.store_key(key_id, key_data, expiration)
        if result:
            # Update local cache
            self.local_keys[key_id] = key_data
            # Clear Fernet cache to ensure it's recreated with new key
            self.cipher_suites.pop(key_id, None)
            self._ensure_fernet(key_id)
            
        return result
    
    def delete_key(self, key_id: str) -> bool:
        """
        Delete a key from Redis KMS.
        
        Args:
            key_id: The ID of the key to delete.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        # Remove from local caches
        self.local_keys.pop(key_id, None)
        self.cipher_suites.pop(key_id, None)
        self._key_cache.pop(key_id, None)
        self._fernet_instances.pop(key_id, None)
        
        # Delete from Redis
        return self.kms.delete_key(key_id)
    
    def encrypt(self, key_id: str, data: bytes) -> bytes:
        """
        Encrypt data using the specified key.
        
        Args:
            key_id: The ID of the key to use for encryption.
            data: The data to encrypt (bytes).
            
        Returns:
            bytes: The encrypted data.
            
        Raises:
            ValueError: If the key is not found or encryption fails.
        """
        if not self._ensure_fernet(key_id):
            raise ValueError(f"No valid key found for ID: {key_id}")
            
        try:
            return self.cipher_suites[key_id].encrypt(data)
        except Exception as e:
            self.logger.error(f"Encryption failed for key {key_id}: {e}")
            raise ValueError(f"Encryption failed: {e}")
    
    def decrypt(self, key_id: str, token: bytes) -> bytes:
        """
        Decrypt data using the specified key.
        
        Args:
            key_id: The ID of the key to use for decryption.
            token: The encrypted data to decrypt.
            
        Returns:
            bytes: The decrypted data.
            
        Raises:
            ValueError: If the key is not found or decryption fails.
        """
        if not self._ensure_fernet(key_id):
            raise ValueError(f"No valid key found for ID: {key_id}")
            
        try:
            return self.cipher_suites[key_id].decrypt(token)
        except InvalidToken:
            self.logger.error(f"Invalid token for key {key_id}")
            raise ValueError("Invalid or corrupted token")
        except Exception as e:
            self.logger.error(f"Decryption failed for key {key_id}: {e}")
            raise ValueError(f"Decryption failed: {e}")
    
    def key_exists(self, key_id: str) -> bool:
        """
        Check if a key exists in Redis KMS or local cache.
        
        Args:
            key_id: The ID of the key to check.
            
        Returns:
            bool: True if the key exists, False otherwise.
        """
        # Check local cache first
        if key_id in self.local_keys:
            return True
            
        # Check Redis KMS
        return self.kms.key_exists(key_id)

    def export_key(self, key_id: str) -> Optional[str]:
        """
        Export a key for sharing.
        
        Args:
            key_id: ID of the key to export.
            
        Returns:
            str: Base64 encoded key, or None if key not found.
        """
        key_data = self.get_key(key_id)
        if not key_data:
            return None
            
        if isinstance(key_data, dict):
            key_material = key_data.get('key_material')
            if key_material and isinstance(key_material, str):
                return key_material
            return None
            
        if isinstance(key_data, bytes):
            return base64.b64encode(key_data).decode('utf-8')
            
        return None
    
    def import_key(self, key_id: str, encoded_key: str) -> bool:
        """Import a key from a base64 encoded string.
        
        Args:
            key_id: ID for the imported key.
            encoded_key: Base64 encoded key.
            
        Returns:
            bool: True if import was successful, False otherwise.
            
        Raises:
            ValueError: If the key data is invalid.
        """
        try:
            key_bytes = base64.b64decode(encoded_key)
            key_data = {
                'key_type': 'fernet',
                'key_material': base64.urlsafe_b64encode(key_bytes).decode('utf-8'),
                'created_at': str(datetime.utcnow()),
                'algorithm': 'AES-256-CBC',
                'size': len(key_bytes)
            }
            return self.store_key(key_id, key_data)
        except Exception as e:
            self.logger.error(f"Failed to import key {key_id}: {e}")
            raise ValueError(f"Failed to import key: {e}")
