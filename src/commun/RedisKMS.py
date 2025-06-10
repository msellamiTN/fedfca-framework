"""
Redis-based Key Management Service for FedFCA framework.
Handles secure storage and retrieval of encryption keys.
"""
import json
import logging
import os
from typing import Optional, Dict, Any, Union
import redis
from datetime import datetime, timedelta
import base64
import time
class RedisKMS:
    def __init__(self, host: str = None, port: int = None, db: int = 0, 
                 key_prefix: str = 'fedfca:keys:', username: str = None, 
                 password: str = None):
        """
        Initialize Redis KMS client.
        
        Args:
            host: Redis server host (default: from env REDIS_HOST or 'datastore')
            port: Redis server port (default: from env REDIS_PORT or 6379)
            db: Redis database number
            key_prefix: Prefix for all keys stored in Redis
            username: Redis username (default: from env REDIS_USER or None)
            password: Redis password (default: from env REDIS_PASSWORD or None)
        """
        self.host = host or os.getenv('REDIS_HOST', 'datastore')
        self.port = port or int(os.getenv('REDIS_PORT', 6379))
        self.db = db
        self.key_prefix = key_prefix
        # self.username = username or os.getenv('REDIS_USER')
        # self.password = password or os.getenv('REDIS_PASSWORD')
        self.logger = logging.getLogger(__name__)
        self.redis = self._connect_redis()
        
    def _connect_redis(self) -> redis.Redis:
        """Establish connection to Redis server with retry logic"""
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                # Build connection parameters
                conn_params = {
                    'host': self.host,
                    'port': self.port,
                    'db': self.db,
                    'decode_responses': False,  # Keep bytes for binary data
                    'socket_connect_timeout': 5,
                    'socket_timeout': 5,
                    'retry_on_timeout': True
                }
                
                # # Add authentication if credentials are provided
                # if self.password:
                #     if self.username:
                #         conn_params['username'] = self.username
                #     conn_params['password'] = self.password
                
                self.logger.debug(f"Connecting to Redis with params: { {k: '***' if k in ('password',) else v for k, v in conn_params.items()} }")
                conn = redis.Redis(**conn_params)
                # Test the connection
                conn.ping()
                self.logger.info(f"Connected to Redis at {self.host}:{self.port}")
                return conn
            except (redis.ConnectionError, redis.TimeoutError) as e:
                self.logger.warning(
                    f"Failed to connect to Redis (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt < max_retries - 1:
 
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    self.logger.error("Max retries reached. Could not connect to Redis.")
                    raise
    
    def _get_full_key(self, key_id: str) -> str:
        """Get the full Redis key with prefix"""
        return f"{self.key_prefix}{key_id}"
    
    def store_key(
        self, 
        key_id: str, 
        key_data: Union[bytes, Dict[str, Any]],
        expiration: Optional[int] = None,
        federation_id: Optional[str] = None
    ) -> bool:
        """
        Store a key in Redis KMS.
        
        Args:
            key_id: Unique identifier for the key
            key_data: Key data (bytes or dict)
            expiration: Optional TTL in seconds
            federation_id: Optional federation ID this key belongs to
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            redis_key = self._get_full_key(key_id)
            
            # Ensure key_data is properly formatted
            if isinstance(key_data, dict):
                # Ensure required fields exist
                if 'key_material' not in key_data:
                    self.logger.error(f"Key data missing required 'key_material' field: {key_data}")
                    return False
                
                # Add federation ID if provided and not already set
                if federation_id and 'federation_id' not in key_data:
                    key_data['federation_id'] = federation_id
                
                # Ensure key_material is properly encoded
                key_material = key_data['key_material']
                key_bytes = None
                
                if isinstance(key_material, str):
                    try:
                        # Try URL-safe base64 first
                        key_bytes = base64.urlsafe_b64decode(key_material)
                        self.logger.debug("Successfully decoded key as URL-safe base64")
                    except Exception:
                        try:
                            # Try standard base64 if URL-safe fails
                            key_bytes = base64.b64decode(key_material)
                            self.logger.debug("Successfully decoded key as standard base64")
                            # Convert to URL-safe format for storage
                            key_material = base64.urlsafe_b64encode(key_bytes).decode('utf-8')
                            key_data['key_material'] = key_material
                        except Exception as e:
                            self.logger.error(f"Invalid base64 key material: {e}")
                            return False
                elif isinstance(key_material, bytes):
                    key_bytes = key_material
                    # Convert to URL-safe base64 string for consistent storage
                    key_material = base64.urlsafe_b64encode(key_bytes).decode('utf-8')
                    key_data['key_material'] = key_material
                else:
                    self.logger.error(f"Key material must be string or bytes, got {type(key_material)}")
                    return False
                
                # Verify key length
                if key_bytes and len(key_bytes) != 32:
                    self.logger.error(f"Invalid key length: {len(key_bytes)} bytes (expected 32)")
                    return False
                    
                # Ensure key material is in URL-safe base64 format
                if isinstance(key_data['key_material'], bytes):
                    key_data['key_material'] = base64.urlsafe_b64encode(key_data['key_material']).decode('utf-8')
                
                # Add/update metadata
                key_data.update({
                    'key_type': key_data.get('key_type', 'fernet'),
                    'created_at': key_data.get('created_at', datetime.utcnow().isoformat()),
                    'algorithm': key_data.get('algorithm', 'AES-128-CBC'),
                    'size': key_data.get('size', 32)
                })
                
                if federation_id and 'federation_id' not in key_data:
                    key_data['federation_id'] = federation_id
                
                value = json.dumps(key_data).encode('utf-8')
            else:
                # Convert bytes to proper key data structure
                try:
                    if len(key_data) != 32:
                        self.logger.error(f"Invalid key length: {len(key_data)} bytes (expected 32)")
                        return False
                        
                    key_data = {
                        'key_material': base64.urlsafe_b64encode(key_data).decode('utf-8'),
                        'key_type': 'fernet',
                        'created_at': datetime.utcnow().isoformat(),
                        'algorithm': 'AES-128-CBC',
                        'size': len(key_data),
                        'federation_id': federation_id
                    }
                    value = json.dumps(key_data).encode('utf-8')
                except Exception as e:
                    self.logger.error(f"Failed to encode key data: {e}")
                    return False
                
            # Store the key with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = self.redis.set(
                        name=redis_key,
                        value=value,
                        ex=expiration
                    )
                    
                    if result:
                        self.logger.info(f"Successfully stored key {key_id} in Redis KMS")
                        return True
                    else:
                        self.logger.warning(f"Failed to store key {key_id} in Redis KMS (attempt {attempt + 1}/{max_retries})")
                        
                except Exception as e:
                    self.logger.error(f"Error storing key {key_id} (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(1)  # Wait before retry
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to store key {key_id}: {e}", exc_info=True)
            return False
            
    def get_key(self, key_id: str, try_without_prefix: bool = True) -> Optional[Dict[str, Any]]:
        """
        Retrieve a key from Redis KMS.
        
        Args:
            key_id: Unique identifier for the key
            try_without_prefix: If True, will try to get the key without the prefix if not found with prefix
            
        Returns:
            Optional[Dict]: The key data if found, None otherwise
        """
        def _get_key(full_key: str) -> Optional[Dict[str, Any]]:
            """Helper function to get key data"""
            try:
                key_data = self.redis.get(full_key)
                if not key_data:
                    self.logger.debug(f"Key {full_key} not found in Redis KMS")
                    return None
                    
                # Deserialize the key data
                try:
                    key_dict = json.loads(key_data)
                    
                    # Handle legacy format (just key material as bytes)
                    if isinstance(key_dict, str):
                        key_dict = {'key_material': key_dict}
                    elif not isinstance(key_dict, dict):
                        raise ValueError(f"Unexpected key data format: {type(key_dict)}")
                    
                    # Ensure key_material exists
                    if 'key_material' not in key_dict:
                        raise ValueError("Key data missing 'key_material' field")
                    
                    # Handle base64 encoded key material
                    if isinstance(key_dict['key_material'], str):
                        try:
                            # Try to decode base64 if it's encoded
                            key_dict['key_material'] = base64.urlsafe_b64decode(key_dict['key_material'].encode())
                        except Exception as e:
                            self.logger.warning(f"Failed to decode key material as base64: {e}")
                            # Keep as is if it's not base64
                    
                    return key_dict
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode key data for {full_key}: {e}")
                    return None
                    
            except Exception as e:
                self.logger.error(f"Error retrieving key {full_key}: {e}", exc_info=True)
                return None
        
        # First try with the full key (with prefix)
        redis_key = self._get_full_key(key_id)
        key_data = _get_key(redis_key)
        
        # If not found and we should try without prefix
        if key_data is None and try_without_prefix and not key_id.startswith(self.key_prefix):
            self.logger.debug(f"Key {key_id} not found with prefix, trying without prefix...")
            key_data = _get_key(key_id)
        
        return key_data
            
    def delete_key(self, key_id: str) -> bool:
        """
        Delete a key from Redis KMS.
        
        Args:
            key_id: Unique identifier for the key
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            redis_key = self._get_full_key(key_id)
            result = self.redis.delete(redis_key)
            
            if result > 0:
                self.logger.debug(f"Deleted key {key_id} from Redis KMS")
                return True
            else:
                self.logger.warning(f"Key {key_id} not found in Redis KMS")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to delete key {key_id}: {e}", exc_info=True)
            return False
    
    def key_exists(self, key_id: str) -> bool:
        """
        Check if a key exists in Redis KMS.
        
        Args:
            key_id: Unique identifier for the key
            
        Returns:
            bool: True if key exists, False otherwise
        """
        try:
            return bool(self.redis.exists(self._get_full_key(key_id)))
        except Exception as e:
            self.logger.error(f"Failed to check if key {key_id} exists: {e}", exc_info=True)
            return False
            
    def set_key_metadata(self, key_id: str, metadata_updates: Dict[str, Any]) -> bool:
        """
        Update metadata fields for a key in Redis KMS.
        
        Args:
            key_id: Unique identifier for the key
            metadata_updates: Dictionary of metadata fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            redis_key = self._get_full_key(key_id)
            
            # Start a pipeline for atomic operations
            with self.redis.pipeline() as pipe:
                # Get the current key data
                current_data = self.get_key(key_id)
                
                if current_data is None:
                    self.logger.warning(f"Key {key_id} not found, cannot update metadata")
                    return False
                
                # If the current data is bytes, we can't update metadata
                if isinstance(current_data, bytes):
                    self.logger.warning(f"Cannot update metadata for binary key {key_id}")
                    return False
                
                # Update the metadata fields
                current_data.update(metadata_updates)
                
                # Store the updated data
                pipe.set(
                    name=redis_key,
                    value=json.dumps(current_data).encode('utf-8')
                )
                
                # Execute the pipeline
                pipe.execute()
                
                self.logger.debug(f"Updated metadata for key {key_id}: {metadata_updates}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to update metadata for key {key_id}: {e}", exc_info=True)
            return False
    
    def set_expiration(self, key_id: str, ttl_seconds: int) -> bool:
        """
        Set expiration time for a key.
        
        Args:
            key_id: Unique identifier for the key
            ttl_seconds: Time to live in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            redis_key = self._get_full_key(key_id)
            return bool(self.redis.expire(redis_key, ttl_seconds))
        except Exception as e:
            self.logger.error(f"Failed to set expiration for key {key_id}: {e}", exc_info=True)
            return False
