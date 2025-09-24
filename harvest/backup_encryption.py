"""
Encryption and compression utilities for backup operations.
Provides secure backup with encryption and compression support.
"""

import os
import gzip
import tarfile
import zipfile
import hashlib
import hmac
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from typing import Optional, Union, BinaryIO
from pathlib import Path

from .utils.logger import get_logger

logger = get_logger("harvest.backup_encryption")


class BackupEncryption:
    """
    Handles encryption and decryption of backup files.
    """
    
    def __init__(self, password: str = None):
        """
        Initialize encryption handler.
        
        Args:
            password: Password for encryption (if None, will use default)
        """
        self.password = password or "pixvault_default_password"
        self._key = None
    
    def _derive_key(self, salt: bytes) -> bytes:
        """Derive encryption key from password and salt."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(self.password.encode())
    
    def encrypt_file(self, input_path: str, output_path: str) -> bool:
        """
        Encrypt a file.
        
        Args:
            input_path: Path to input file
            output_path: Path to output encrypted file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate salt
            salt = os.urandom(16)
            
            # Derive key
            key = self._derive_key(salt)
            fernet = Fernet(base64.urlsafe_b64encode(key))
            
            # Read input file
            with open(input_path, 'rb') as f:
                data = f.read()
            
            # Encrypt data
            encrypted_data = fernet.encrypt(data)
            
            # Write encrypted file with salt
            with open(output_path, 'wb') as f:
                f.write(salt + encrypted_data)
            
            logger.info(f"File encrypted: {input_path} -> {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return False
    
    def decrypt_file(self, input_path: str, output_path: str) -> bool:
        """
        Decrypt a file.
        
        Args:
            input_path: Path to encrypted file
            output_path: Path to decrypted file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Read encrypted file
            with open(input_path, 'rb') as f:
                data = f.read()
            
            # Extract salt and encrypted data
            salt = data[:16]
            encrypted_data = data[16:]
            
            # Derive key
            key = self._derive_key(salt)
            fernet = Fernet(base64.urlsafe_b64encode(key))
            
            # Decrypt data
            decrypted_data = fernet.decrypt(encrypted_data)
            
            # Write decrypted file
            with open(output_path, 'wb') as f:
                f.write(decrypted_data)
            
            logger.info(f"File decrypted: {input_path} -> {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return False
    
    def encrypt_directory(self, input_dir: str, output_path: str) -> bool:
        """
        Encrypt entire directory.
        
        Args:
            input_dir: Path to input directory
            output_path: Path to output encrypted file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create tar.gz first
            temp_tar = output_path + ".temp.tar.gz"
            with tarfile.open(temp_tar, "w:gz") as tar:
                tar.add(input_dir, arcname=".")
            
            # Encrypt the tar file
            success = self.encrypt_file(temp_tar, output_path)
            
            # Clean up temp file
            if os.path.exists(temp_tar):
                os.remove(temp_tar)
            
            return success
            
        except Exception as e:
            logger.error(f"Directory encryption failed: {e}")
            return False
    
    def decrypt_directory(self, input_path: str, output_dir: str) -> bool:
        """
        Decrypt directory.
        
        Args:
            input_path: Path to encrypted file
            output_dir: Path to output directory
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create temp file
            temp_tar = input_path + ".temp.tar.gz"
            
            # Decrypt file
            success = self.decrypt_file(input_path, temp_tar)
            if not success:
                return False
            
            # Extract tar file
            with tarfile.open(temp_tar, "r:gz") as tar:
                tar.extractall(output_dir)
            
            # Clean up temp file
            if os.path.exists(temp_tar):
                os.remove(temp_tar)
            
            return True
            
        except Exception as e:
            logger.error(f"Directory decryption failed: {e}")
            return False


class BackupCompression:
    """
    Handles compression and decompression of backup files.
    """
    
    @staticmethod
    def compress_file(input_path: str, output_path: str, compression_level: int = 6) -> bool:
        """
        Compress a file using gzip.
        
        Args:
            input_path: Path to input file
            output_path: Path to output compressed file
            compression_level: Compression level (1-9)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(input_path, 'rb') as f_in:
                with gzip.open(output_path, 'wb', compresslevel=compression_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(f"File compressed: {input_path} -> {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Compression failed: {e}")
            return False
    
    @staticmethod
    def decompress_file(input_path: str, output_path: str) -> bool:
        """
        Decompress a gzip file.
        
        Args:
            input_path: Path to compressed file
            output_path: Path to output decompressed file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with gzip.open(input_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(f"File decompressed: {input_path} -> {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Decompression failed: {e}")
            return False
    
    @staticmethod
    def compress_directory(input_dir: str, output_path: str, compression_level: int = 6) -> bool:
        """
        Compress directory using tar.gz.
        
        Args:
            input_dir: Path to input directory
            output_path: Path to output compressed file
            compression_level: Compression level (1-9)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with tarfile.open(output_path, "w:gz", compresslevel=compression_level) as tar:
                tar.add(input_dir, arcname=".")
            
            logger.info(f"Directory compressed: {input_dir} -> {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Directory compression failed: {e}")
            return False
    
    @staticmethod
    def decompress_directory(input_path: str, output_dir: str) -> bool:
        """
        Decompress tar.gz directory.
        
        Args:
            input_path: Path to compressed file
            output_dir: Path to output directory
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with tarfile.open(input_path, "r:gz") as tar:
                tar.extractall(output_dir)
            
            logger.info(f"Directory decompressed: {input_path} -> {output_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Directory decompression failed: {e}")
            return False


class SecureBackup:
    """
    Handles secure backup with encryption and compression.
    """
    
    def __init__(self, password: str = None, compression_level: int = 6):
        """
        Initialize secure backup handler.
        
        Args:
            password: Password for encryption
            compression_level: Compression level (1-9)
        """
        self.encryption = BackupEncryption(password)
        self.compression = BackupCompression()
        self.compression_level = compression_level
    
    def create_secure_backup(self, source_path: str, dest_path: str) -> bool:
        """
        Create secure backup with encryption and compression.
        
        Args:
            source_path: Path to source directory
            dest_path: Path to destination backup file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Step 1: Compress directory
            temp_compressed = dest_path + ".temp.tar.gz"
            success = self.compression.compress_directory(
                source_path, temp_compressed, self.compression_level
            )
            if not success:
                return False
            
            # Step 2: Encrypt compressed file
            success = self.encryption.encrypt_file(temp_compressed, dest_path)
            
            # Clean up temp file
            if os.path.exists(temp_compressed):
                os.remove(temp_compressed)
            
            return success
            
        except Exception as e:
            logger.error(f"Secure backup creation failed: {e}")
            return False
    
    def restore_secure_backup(self, backup_path: str, dest_path: str) -> bool:
        """
        Restore secure backup.
        
        Args:
            backup_path: Path to backup file
            dest_path: Path to restore directory
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Step 1: Decrypt file
            temp_compressed = backup_path + ".temp.tar.gz"
            success = self.encryption.decrypt_file(backup_path, temp_compressed)
            if not success:
                return False
            
            # Step 2: Decompress directory
            success = self.compression.decompress_directory(temp_compressed, dest_path)
            
            # Clean up temp file
            if os.path.exists(temp_compressed):
                os.remove(temp_compressed)
            
            return success
            
        except Exception as e:
            logger.error(f"Secure backup restore failed: {e}")
            return False
    
    def verify_backup(self, backup_path: str) -> bool:
        """
        Verify backup integrity.
        
        Args:
            backup_path: Path to backup file
            
        Returns:
            True if backup is valid, False otherwise
        """
        try:
            # Try to decrypt and decompress
            temp_dir = backup_path + ".temp_verify"
            temp_compressed = backup_path + ".temp_verify.tar.gz"
            
            # Create temp directory
            Path(temp_dir).mkdir(parents=True, exist_ok=True)
            
            # Decrypt
            success = self.encryption.decrypt_file(backup_path, temp_compressed)
            if not success:
                return False
            
            # Decompress
            success = self.compression.decompress_directory(temp_compressed, temp_dir)
            if not success:
                return False
            
            # Clean up
            if os.path.exists(temp_compressed):
                os.remove(temp_compressed)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
            logger.info(f"Backup verification successful: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"Backup verification failed: {e}")
            return False


def create_secure_backup(source_path: str, dest_path: str, password: str = None) -> bool:
    """Create secure backup with encryption and compression."""
    secure_backup = SecureBackup(password)
    return secure_backup.create_secure_backup(source_path, dest_path)


def restore_secure_backup(backup_path: str, dest_path: str, password: str = None) -> bool:
    """Restore secure backup."""
    secure_backup = SecureBackup(password)
    return secure_backup.restore_secure_backup(backup_path, dest_path)


def verify_secure_backup(backup_path: str, password: str = None) -> bool:
    """Verify secure backup integrity."""
    secure_backup = SecureBackup(password)
    return secure_backup.verify_backup(backup_path)
