#!/usr/bin/env python3  
  
import os  
import sys  
import hashlib  
import struct  
import tempfile  
import shutil  
import zlib  
from typing import List, Optional, Tuple, Union  
import subprocess  
import bsdiff4
  
class FileContents:  
    """Represents file contents with SHA1 hash and metadata"""  
    def __init__(self, data: bytes = b'', file_path: str = ''):  
        self.data = data  
        self.sha1 = hashlib.sha1(data).digest() if data else b''  
        self.file_path = file_path  
        self.st_mode = 0o644  
        self.st_uid = os.getuid() if hasattr(os, 'getuid') else 0  
        self.st_gid = os.getgid() if hasattr(os, 'getgid') else 0  
          
        if file_path and os.path.exists(file_path):  
            stat = os.stat(file_path)  
            self.st_mode = stat.st_mode  
            self.st_uid = stat.st_uid  
            self.st_gid = stat.st_gid  
  
# Constants for ImgDiff  
CHUNK_NORMAL = 0  
CHUNK_GZIP = 1  
CHUNK_DEFLATE = 2  
CHUNK_RAW = 3  
HAS_BSDIFF4 = 1
  
def parse_sha1(sha1_str: str) -> bytes:  
    """Parse SHA1 string to bytes, handling format '<digest>:<anything>'"""  
    sha1_part = sha1_str.split(':')[0]  
    if len(sha1_part) != 40:  
        raise ValueError(f"Invalid SHA1 length: {len(sha1_part)}")  
      
    try:  
        return bytes.fromhex(sha1_part)  
    except ValueError:  
        raise ValueError(f"Invalid SHA1 format: {sha1_part}")  
  
def short_sha1(sha1_bytes: bytes) -> str:  
    """Convert SHA1 bytes to short hex string"""  
    return sha1_bytes.hex()[:8]  
  
def load_file_contents(filename: str) -> FileContents:  
    """Load file contents with SHA1 verification"""  
    if not os.path.exists(filename):  
        raise FileNotFoundError(f"File not found: {filename}")  
      
    with open(filename, 'rb') as f:  
        data = f.read()  
      
    return FileContents(data, filename)  
  
def find_matching_patch(file_sha1: bytes, patch_sha1_list: List[str]) -> int:  
    """Find matching patch index for given file SHA1"""  
    for i, patch_sha1_str in enumerate(patch_sha1_list):  
        try:  
            patch_sha1 = parse_sha1(patch_sha1_str)  
            if patch_sha1 == file_sha1:  
                return i  
        except ValueError:  
            continue  
    return -1  
  
def read_le_int32(data: bytes, offset: int) -> int:  
    """Read little-endian 32-bit integer"""  
    return struct.unpack('<I', data[offset:offset+4])[0]  
  
def read_le_int64(data: bytes, offset: int) -> int:  
    """Read little-endian 64-bit integer"""  
    return struct.unpack('<Q', data[offset:offset+8])[0]  
  
def apply_bsdiff_patch(source_data: bytes, patch_data: bytes) -> bytes:  
    """Apply BSDiff patch using bsdiff4 module only"""  
    if not patch_data.startswith(b'BSDIFF40'):  
        raise ValueError("Not a BSDiff patch")  
      
    # Use bsdiff4 module only  
    if HAS_BSDIFF4 == 1:
        return bsdiff4.patch(source_data, patch_data)  
    else:  
        raise RuntimeError("bsdiff4 module is required but not available. Install with: pip install bsdiff4")
    
def apply_imgdiff_patch(source_data: bytes, patch_data: bytes, bonus_data: bytes = None) -> bytes:  
    """Apply ImgDiff patch with proper integer range handling"""  
      
    if len(patch_data) < 12:  
        raise ValueError("Patch too short to contain header")  
      
    if not patch_data.startswith(b'IMGDIFF2'):  
        raise ValueError("Not an ImgDiff2 patch")  
      
    num_chunks = read_le_int32(patch_data, 8)  
    pos = 12  
      
    output_data = bytearray()  
      
    for chunk_idx in range(num_chunks):  
        if pos + 4 > len(patch_data):  
            raise ValueError(f"Failed to read chunk {chunk_idx} record")  
          
        chunk_type = read_le_int32(patch_data, pos)  
        pos += 4  
          
        if chunk_type == CHUNK_NORMAL:  
            # 添加范围检查  
            if pos + 24 > len(patch_data):  
                raise ValueError(f"Failed to read chunk {chunk_idx} normal header")  
              
            src_start = read_le_int64(patch_data, pos)  
            src_len = read_le_int64(patch_data, pos + 8)  
            patch_offset = read_le_int64(patch_data, pos + 16)  
            pos += 24  
              
            # 检查值是否在合理范围内  
            if src_start > len(source_data) or src_len > len(source_data):  
                raise ValueError(f"Source parameters out of range: start={src_start}, len={src_len}")  
              
            if patch_offset > len(patch_data):  
                raise ValueError(f"Patch offset out of range: {patch_offset}")  
              
            # 确保切片操作安全  
            try:  
                chunk_source = source_data[src_start:src_start + src_len]  
                chunk_patch = patch_data[patch_offset:]  
                patched_chunk = apply_bsdiff_patch(chunk_source, chunk_patch)  
                output_data.extend(patched_chunk)  
            except OverflowError as e:  
                raise ValueError(f"Integer overflow in chunk {chunk_idx}: {e}")

def generate_target(source_file: FileContents, patch_data: bytes,   
                   target_filename: str, target_sha1: bytes,   
                   target_size: int, bonus_data: bytes = None) -> bool:  
    """Generate target file by applying patch"""  
      
    # Determine patch type and apply  
    try:  
        if patch_data.startswith(b'BSDIFF40'):  
            patched_data = apply_bsdiff_patch(source_file.data, patch_data)  
        elif patch_data.startswith(b'IMGDIFF2'):  
            patched_data = apply_imgdiff_patch(source_file.data, patch_data, bonus_data)  
        else:  
            print("Unknown patch format")  
            return False  
    except Exception as e:  
        print(f"Patch application failed: {e}")  
        return False  
      
    # Verify target size  
    if len(patched_data) != target_size:  
        print(f"Size mismatch: expected {target_size}, got {len(patched_data)}")  
        return False  
      
    # Verify target SHA1  
    actual_sha1 = hashlib.sha1(patched_data).digest()  
    if actual_sha1 != target_sha1:  
        print(f"SHA1 mismatch: expected {target_sha1.hex()}, got {actual_sha1.hex()}")  
        return False  
      
    # Handle partition targets  
    if target_filename.startswith(('MTD:', 'EMMC:')):  
        return write_to_partition(patched_data, target_filename)  
      
    # Write to temporary file first, then rename (atomic operation)  
    temp_filename = target_filename + '.patch'  
    try:  
        with open(temp_filename, 'wb') as f:  
            f.write(patched_data)  
          
        # Set file permissions to match source  
        os.chmod(temp_filename, source_file.st_mode)  
        if hasattr(os, 'chown'):  
            try:  
                os.chown(temp_filename, source_file.st_uid, source_file.st_gid)  
            except (OSError, PermissionError):  
                pass  # Ignore permission errors  
          
        # Atomic rename to final target  
        os.rename(temp_filename, target_filename)  
        return True  
          
    except Exception as e:  
        print(f"Failed to write target file: {e}")  
        if os.path.exists(temp_filename):  
            os.unlink(temp_filename)  
        return False  
  
def write_to_partition(data: bytes, target: str) -> bool:  
    """Write data to partition - simplified implementation"""  
    print(f"Partition writing to {target} not fully implemented in Python version")  
    print("This would require low-level partition access")  
    return False  
  
def save_file_contents(filename: str, file_contents: FileContents) -> bool:  
    """Save file contents to disk with proper permissions"""  
    try:  
        with open(filename, 'wb') as f:  
            f.write(file_contents.data)  
          
        # Set file permissions  
        os.chmod(filename, file_contents.st_mode)  
        if hasattr(os, 'chown'):  
            try:  
                os.chown(filename, file_contents.st_uid, file_contents.st_gid)  
            except (OSError, PermissionError):  
                pass  # Ignore permission errors  
          
        return True  
    except Exception as e:  
        print(f"Failed to save file {filename}: {e}")  
        return False  
  
def applypatch_check(filename: str, patch_sha1_list: List[str]) -> bool:  
    """Check if file matches any of the expected SHA1 sums"""  
    try:  
        file_contents = load_file_contents(filename)  
        if not patch_sha1_list:  
            return True  # No SHA1s to check  
          
        return find_matching_patch(file_contents.sha1, patch_sha1_list) >= 0  
    except Exception:  
        return False  
  
def applypatch(source_filename: str, target_filename: str,   
               target_sha1_str: str, target_size: int,  
               patch_sha1_list: List[str], patch_files: List[str],  
               bonus_data: bytes = None) -> int:  
    """Main applypatch function - Python equivalent of C++ applypatch"""  
      
    print(f"patch {source_filename}:")  
      
    # Handle "-" target filename  
    if target_filename == "-":  
        target_filename = source_filename  
      
    # Parse target SHA1  
    try:  
        target_sha1 = parse_sha1(target_sha1_str)  
    except ValueError as e:  
        print(f"Failed to parse target SHA1: {e}")  
        return 1  
      
    # Check if target already has correct SHA1  
    try:  
        if os.path.exists(target_filename):  
            target_contents = load_file_contents(target_filename)  
            if target_contents.sha1 == target_sha1:  
                print(f"already {short_sha1(target_sha1)}")  
                return 0  
    except Exception:  
        pass  
      
    # Load source file  
    try:  
        source_file = load_file_contents(source_filename)  
    except Exception as e:  
        print(f"Failed to load source file: {e}")  
        return 1  
      
    # Find matching patch  
    patch_index = find_matching_patch(source_file.sha1, patch_sha1_list)  
    if patch_index < 0:  
        print("Source file doesn't match any expected SHA1")  
        return 1  
      
    # Load patch data  
    try:  
        with open(patch_files[patch_index], 'rb') as f:  
            patch_data = f.read()  
    except Exception as e:  
        print(f"Failed to load patch file: {e}")  
        return 1  
      
    # Apply patch  
    if generate_target(source_file, patch_data, target_filename,   
                      target_sha1, target_size, bonus_data):  
        print(f"now {short_sha1(target_sha1)}")  
        return 0  
    else:  
        return 1  
  
def main():  
    """Main function - equivalent to ApplyPatchFn in C++"""  
    if len(sys.argv) < 7:  
        print(f"usage: {sys.argv[0]} <file> <target> <tgt_sha1> <size> <init_sha1(1)> <patch(1)> [init_sha1(2)] [patch(2)] ... [bonus]")  
        print("\t<file> = source file from rom zip")  
        print("\t<target> = target file (use \"-\" to patch source file)")  
        print("\t<tgt_sha1> = target SHA1 Sum after patching")  
        print("\t<size> = file size")  
        print("\t<init_sha1> = file SHA1 sum")  
        print("\t<patch> = patch file (.p) from OTA zip")  
        print("\t<bonus> = bonus resource file")  
        return 1  
      
    source_filename = sys.argv[1]  
    target_filename = sys.argv[2]  
    target_sha1_str = sys.argv[3]  
    target_size_str = sys.argv[4]  
      
    try:  
        target_size = int(target_size_str)  
    except ValueError:  
        print(f"Invalid size: {target_size_str}")  
        return 1  
      
    # Parse patch arguments  
    remaining_args = sys.argv[5:]  
    bonus_data = None  
      
    # Check if there's a bonus file (odd number of remaining args)  
    if len(remaining_args) % 2 != 0:  
        bonus_filename = remaining_args[-1]  
        patch_args = remaining_args[:-1]  
          
        # Load bonus data  
        try:  
            with open(bonus_filename, 'rb') as f:  
                bonus_data = f.read()  
        except Exception as e:  
            print(f"Failed to load bonus file: {e}")  
            return 1  
    else:  
        patch_args = remaining_args  
      
    if len(patch_args) % 2 != 0:  
        print("Invalid number of patch arguments")  
        return 1  
      
    # Extract SHA1s and patch files  
    patch_sha1_list = []  
    patch_files = []  
    for i in range(0, len(patch_args), 2):  
        patch_sha1_list.append(patch_args[i])  
        patch_files.append(patch_args[i + 1])  
      
    # Create cache directory  
    cache_dir = "/tmp/applypatch_cache"  
    try:  
        os.makedirs(cache_dir, exist_ok=True)  
        print(f"cache dir: {cache_dir}")  
    except Exception as e:  
        print(f"Failed to create cache directory: {e}")  
        return 1  
      
    # Apply the patch  
    result = applypatch(source_filename, target_filename, target_sha1_str,   
                       target_size, patch_sha1_list, patch_files, bonus_data)  
      
    # Clean up cache directory  
    try:  
        if os.path.exists(cache_dir):  
            shutil.rmtree(cache_dir)  
    except Exception as e:  
        print(f"Failed to remove cache directory: {e}")  
      
    if result == 0:  
        print("Done")  
    else:  
        print(f"Done with error code: {result}")  
      
    return result  
  
if __name__ == "__main__":  
    sys.exit(main())