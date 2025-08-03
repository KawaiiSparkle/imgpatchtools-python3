#!/usr/bin/env python3  
  
import os  
import sys  
import hashlib  
import struct  
import tempfile  
import shutil  
import time
import zlib  
from typing import List, Optional, Tuple, Union, Iterator  
import bsdiff4
import asyncio  
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor  
import mmap  
from pathlib import Path  
import logging  
from contextlib import contextmanager  
import io  
  
# Set up logging  
logging.basicConfig(level=logging.INFO)  
logger = logging.getLogger(__name__)  
  
class FileContents:  
    """Represents file contents with SHA1 hash and metadata"""  
    __slots__ = ('data', 'sha1', 'file_path', 'st_mode', 'st_uid', 'st_gid')  
      
    def __init__(self, data: bytes, filename: str):  
        """Initialize FileContents with Windows-safe operations"""  
        self.data = data  
        self.filename = filename  
        self.sha1 = hashlib.sha1(data).digest()  
        
        # Windows-safe stat handling  
        if os.name == 'nt':  
            max_retries = 3  
            for attempt in range(max_retries):  
                try:  
                    self.st = Path(filename).stat()  
                    break  
                except OSError as e:  
                    if hasattr(e, 'winerror') and e.winerror == 6:  
                        logger.warning(f"WinError 6 getting file stats, attempt {attempt + 1}")  
                        if attempt < max_retries - 1:  
                            time.sleep(0.1 * (attempt + 1))  
                            continue  
                    raise  
        else:  
            self.st = Path(filename).stat()

  
# Constants for ImgDiff  
CHUNK_NORMAL = 0  
CHUNK_GZIP = 1  
CHUNK_DEFLATE = 2  
CHUNK_RAW = 3  
HAS_BSDIFF4 = 1  

def ensure_bytes(data):
    if isinstance(data, (bytes, bytearray)):
        return data
    elif hasattr(data, "tobytes"):
        return data.tobytes()
    elif hasattr(data, "__getitem__"):
        return bytes(data)
    else:
        raise TypeError(f"Unsupported data type for patch: {type(data)}")
    
# Cache path for cross-platform compatibility  
def get_cache_temp_source():  
    """Windows-compatible cache path generation with WinError 6 handling"""  
    if os.name == 'nt':  
        # Use Windows temp directory with proper error handling  
        temp_base = os.environ.get('TEMP', os.environ.get('TMP', 'C:\\temp'))  
        temp_dir = Path(temp_base) / "applypatch_cache"  
        # Ensure Windows path format  
        temp_dir = Path(str(temp_dir).replace('/', '\\'))  
    else:  
        temp_dir = Path("/tmp/applypatch_cache")  
      
    max_retries = 3  
    for attempt in range(max_retries):  
        try:  
            temp_dir.mkdir(parents=True, exist_ok=True)  
            break  
        except (PermissionError, OSError) as e:  
            if os.name == 'nt' and hasattr(e, 'winerror') and e.winerror == 6:  
                logger.warning(f"WinError 6 creating cache dir, attempt {attempt + 1}")  
                if attempt < max_retries - 1:  
                    time.sleep(0.2 * (attempt + 1))  
                    # Try alternative temp location  
                    import tempfile  
                    temp_dir = Path(tempfile.gettempdir()) / f"ap_cache_{attempt}"  
                    continue  
            elif attempt == max_retries - 1:  
                # Final fallback  
                import tempfile  
                temp_dir = Path(tempfile.gettempdir()) / "ap_cache_fallback"  
                temp_dir.mkdir(parents=True, exist_ok=True)  
            else:  
                time.sleep(0.1)  
      
    return temp_dir / "temp_source"

CACHE_TEMP_SOURCE = get_cache_temp_source()
  
@contextmanager  
def cache_manager():  
    """Enhanced cache manager with better cleanup"""  
    cache_dir = CACHE_TEMP_SOURCE.parent  
    cleanup_needed = False  
      
    try:  
        cache_dir.mkdir(parents=True, exist_ok=True)  
        cleanup_needed = True  
        yield cache_dir  
          
    finally:  
        if cleanup_needed:  
            max_retries = 3  
            for attempt in range(max_retries):  
                try:  
                    if cache_dir.exists():  
                        if CACHE_TEMP_SOURCE.exists():  
                            CACHE_TEMP_SOURCE.unlink()  
                        try:  
                            cache_dir.rmdir()  
                        except OSError:  
                            shutil.rmtree(cache_dir, ignore_errors=True)  
                    break  
                except Exception as e:  
                    if attempt < max_retries - 1:  
                        logger.warning(f"Cache cleanup retry {attempt + 1}: {e}")  
                        import time  
                        time.sleep(0.1)  
                    else:  
                        logger.error(f"Failed to clean cache after {max_retries} attempts: {e}", exc_info=True)
  
def parse_sha1(sha1_str: str) -> bytes:  
    """Parse SHA1 string to bytes, handling format '<digest>:<anything>'"""  
    if not isinstance(sha1_str, str):  
        raise TypeError(f"SHA1 string must be str, got {type(sha1_str)}")  
      
    sha1_part = sha1_str.split(':', 1)[0]  
    if len(sha1_part) != 40:  
        raise ValueError(f"Invalid SHA1 length: {len(sha1_part)}")  
      
    try:  
        return bytes.fromhex(sha1_part)  
    except ValueError as e:  
        raise ValueError(f"Invalid SHA1 format: {sha1_part}") from e  
  
def short_sha1(sha1_bytes: bytes) -> str:  
    """Convert SHA1 bytes to short hex string"""  
    if not isinstance(sha1_bytes, bytes):  
        raise TypeError(f"SHA1 must be bytes, got {type(sha1_bytes)}")  
    return sha1_bytes.hex()[:8]  
  
def load_file_contents_mmap(filename: str) -> FileContents:  
    """Enhanced file loading with comprehensive error handling"""  
    path = Path(filename)  
    if not path.exists():  
        raise FileNotFoundError(f"File not found: {filename}")  
      
    try:  
        file_size = path.stat().st_size  
          
        # 处理零长度文件  
        if file_size == 0:  
            logger.info(f"Loading empty file: {filename}")  
            return FileContents(b'', filename)  
          
        # 对于小文件直接读取  
        if file_size < 1024 * 1024:  # 1MB以下  
            with open(filename, 'rb') as f:  
                data = f.read()  
        else:  
            # 大文件使用mmap，处理可能的失败  
            try:  
                with open(filename, 'rb') as f:  
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:  
                        data = bytes(mm)  
            except (OSError, ValueError) as e:  
                logger.warning(f"mmap failed for {filename}, using regular read: {e}")  
                with open(filename, 'rb') as f:  
                    data = f.read()  
          
        return FileContents(data, filename)  
          
    except (OSError, IOError, MemoryError) as e:  
        raise FileNotFoundError(f"Failed to load file {filename}: {e}") from e
  
def load_file_contents(filename: str) -> FileContents:  
    """Load file contents with optimized path handling"""  
    return load_file_contents_mmap(filename)  
  
def find_matching_patch(file_sha1: bytes, patch_sha1_list: List[str]) -> int:  
    """Find matching patch index for given file SHA1 with early termination"""  
    if not isinstance(file_sha1, bytes):  
        raise TypeError("file_sha1 must be bytes")  
      
    for i, patch_sha1_str in enumerate(patch_sha1_list):  
        try:  
            patch_sha1 = parse_sha1(patch_sha1_str)  
            if patch_sha1 == file_sha1:  
                return i  
        except (ValueError, TypeError):  
            continue  
    return -1  
  
def read_le_int32(data: bytes, offset: int) -> int:  
    """Read little-endian 32-bit integer with bounds checking"""  
    if not isinstance(data, bytes):  
        raise TypeError("data must be bytes")  
    if offset < 0:  
        raise ValueError(f"Offset cannot be negative: {offset}")  
    if offset + 4 > len(data):  
        raise ValueError(f"Cannot read 32-bit int at offset {offset}, data length {len(data)}")  
    return struct.unpack_from('<I', data, offset)[0]  
  
def read_le_int64(data: bytes, offset: int) -> int:  
    """Read little-endian 64-bit integer with bounds checking"""  
    if not isinstance(data, bytes):  
        raise TypeError("data must be bytes")  
    if offset < 0:  
        raise ValueError(f"Offset cannot be negative: {offset}")  
    if offset + 8 > len(data):  
        raise ValueError(f"Cannot read 64-bit int at offset {offset}, data length {len(data)}")  
    return struct.unpack_from('<Q', data, offset)[0]  
  
def apply_bsdiff_patch(source_data: bytes, patch_data: bytes) -> bytes:  
    """Apply BSDiff patch using bsdiff4 module"""  
    if not isinstance(source_data, bytes) or not isinstance(patch_data, bytes):  
        raise TypeError("source_data and patch_data must be bytes")  
      
    if not patch_data.startswith(b'BSDIFF40'):  
        raise ValueError("Not a BSDiff patch")  
      
    if HAS_BSDIFF4 == 1:  
        try:  
            return bsdiff4.patch(source_data, patch_data)  
        except Exception as e:  
            raise RuntimeError(f"BSDiff patch application failed: {e}") from e  
    else:  
        raise RuntimeError("bsdiff4 module is required but not available. Install with: pip install bsdiff4")  

def patch_with_bonus(uncompressed_source, bonus_data, expanded_len, chunk_idx):
    if bonus_data and expanded_len > len(uncompressed_source):
        bonus_size = expanded_len - len(uncompressed_source)
        if bonus_size <= len(bonus_data):
            uncompressed_source += bonus_data[:bonus_size]
            logger.debug(f"Applied {bonus_size} bytes of bonus data to chunk {chunk_idx}")
    return uncompressed_source


def compress_deflate(data, level, window_bits, mem_level, strategy):
    compressor = zlib.compressobj(level=level, wbits=window_bits, memLevel=mem_level, strategy=strategy)
    return compressor.compress(data) + compressor.flush()

def apply_imgdiff_patch_streaming(src_data: bytes, patch_data: bytes, bonus_data: bytes = None) -> bytes:    
    """Apply IMGDIFF2 patch with exact C++ implementation matching"""    
    try:    
        src_data = ensure_bytes(src_data)    
        patch_data = ensure_bytes(patch_data)    
            
        if not patch_data.startswith(b'IMGDIFF2'):    
            raise ValueError("Not a valid IMGDIFF2 patch")    
            
        if len(patch_data) < 12:    
            raise ValueError("Patch too short")    
            
        # Read number of chunks (matches C++ Read4(header+8))    
        num_chunks = struct.unpack('<I', patch_data[8:12])[0]    
        pos = 12    
            
        result = bytearray()    
        logger.debug(f"IMGDIFF2: {num_chunks} chunks, patch size: {len(patch_data)}")    
            
        for i in range(num_chunks):    
            # Read chunk type (matches C++ Read4(patch->data + pos))    
            if pos + 4 > len(patch_data):    
                raise ValueError(f"Failed to read chunk {i} record")    
                
            chunk_type = struct.unpack('<I', patch_data[pos:pos+4])[0]    
            pos += 4    
                
            logger.debug(f"Chunk {i}: type={chunk_type} at pos={pos-4}")    
                
            if chunk_type == CHUNK_NORMAL:  # 0  
                chunk_data, pos = process_normal_chunk(i, src_data, patch_data, pos)  
                result.extend(chunk_data)  
                    
            elif chunk_type == CHUNK_DEFLATE:  # 2  
                chunk_data, pos = process_compressed_chunk(i, chunk_type, src_data, patch_data, pos, bonus_data)  
                result.extend(chunk_data)  
                    
            elif chunk_type == CHUNK_RAW:  # 3  
                chunk_data, pos = process_raw_chunk(i, patch_data, pos)  
                result.extend(chunk_data)  
                    
            else:    
                raise ValueError(f"patch chunk {i} is unknown type {chunk_type}")    
            
        logger.debug(f"IMGDIFF2 patch applied successfully. Final pos: {pos}, Result size: {len(result)}")    
        return bytes(result)    
            
    except Exception as e:    
        logger.error(f"IMGDIFF2 patch application failed: {e}")    
        raise  
  
def process_normal_chunk(chunk_idx: int, source_data: bytes, patch_data: bytes, pos: int) -> Tuple[bytes, int]:  
    """Process CHUNK_NORMAL with proper 64-bit integer handling"""  
    if pos + 24 > len(patch_data):  
        raise ValueError(f"Failed to read chunk {chunk_idx} normal header")  
      
    src_start = read_le_int64(patch_data, pos)  
    src_len = read_le_int64(patch_data, pos + 8)  
    patch_offset = read_le_int64(patch_data, pos + 16)  
      
    if src_start < 0 or src_len < 0 or patch_offset < 0:  
        raise ValueError(f"Negative values in chunk {chunk_idx} header")  
      
    if src_start + src_len > len(source_data):  
        raise ValueError(f"Source parameters out of range: start={src_start}, len={src_len}, source_len={len(source_data)}")  
      
    if patch_offset >= len(patch_data):  
        raise ValueError(f"Patch offset {patch_offset} out of range, patch_len={len(patch_data)}")  
      
    chunk_source = source_data[src_start:src_start + src_len]  
    chunk_patch = patch_data[patch_offset:]  
    result = apply_bsdiff_patch(chunk_source, chunk_patch)  
    return result, pos + 24  
  
def process_raw_chunk(chunk_idx: int, patch_data: bytes, pos: int) -> Tuple[bytes, int]:    
    """Process CHUNK_RAW with correct 4-byte length reading"""    
    if pos + 4 > len(patch_data):    
        raise ValueError(f"Failed to read chunk {chunk_idx} raw header")    
        
    # 使用4字节长度，匹配C++实现  
    data_len = struct.unpack('<I', patch_data[pos:pos+4])[0]  
    pos += 4    
        
    # 添加数值验证    
    if data_len < 0:    
        raise ValueError(f"Invalid RAW chunk data length: {data_len} (negative)")    
    if data_len > len(patch_data) - pos:    
        raise ValueError(f"Invalid RAW chunk data length: {data_len} (exceeds remaining data {len(patch_data) - pos})")    
    if data_len > 100 * 1024 * 1024:  # 100MB 限制    
        raise ValueError(f"RAW chunk data too large: {data_len} bytes")    
        
    return patch_data[pos:pos + data_len], pos + data_len
  
def process_compressed_chunk(chunk_idx: int, chunk_type: int, source_data: bytes,    
                           patch_data: bytes, pos: int, bonus_data: bytes = None) -> Tuple[bytes, int]:    
    """Process CHUNK_GZIP or CHUNK_DEFLATE with correct implementation"""    
        
    # DEFLATE chunk header: 32 bytes base + 16 bytes compression params = 48 bytes total  
    if chunk_type == CHUNK_DEFLATE:  
        header_size = 48  
    else:  
        header_size = 32  
          
    if pos + header_size > len(patch_data):    
        raise ValueError(f"Failed to read chunk {chunk_idx} compressed header")    
        
    # 读取基础参数 (32字节)  
    src_start = struct.unpack('<Q', patch_data[pos:pos+8])[0]  
    src_len = struct.unpack('<Q', patch_data[pos+8:pos+16])[0]    
    patch_offset = struct.unpack('<Q', patch_data[pos+16:pos+24])[0]  
    expanded_len = struct.unpack('<Q', patch_data[pos+24:pos+32])[0]  
        
    # 验证参数    
    if any(x < 0 for x in [src_start, src_len, patch_offset, expanded_len]):    
        raise ValueError(f"Negative values in chunk {chunk_idx} compressed header")    
        
    # 对于 CHUNK_DEFLATE，读取额外的压缩参数    
    compression_level = 6    
    window_bits = -15    
    mem_level = 8    
    strategy = 0    
      
    pos += 32  # 移动到压缩参数位置  
        
    if chunk_type == CHUNK_DEFLATE:    
        # 读取压缩参数 (16字节)  
        level = struct.unpack('<I', patch_data[pos:pos+4])[0]  
        method = struct.unpack('<I', patch_data[pos+4:pos+8])[0]  # 通常是Z_DEFLATED (8)  
        window_bits_raw = struct.unpack('<I', patch_data[pos+8:pos+12])[0]  
        mem_level_raw = struct.unpack('<I', patch_data[pos+12:pos+16])[0]  
        strategy_raw = struct.unpack('<I', patch_data[pos+16:pos+20])[0]  
        pos += 20  # 实际上C++代码读取了5个int32，所以是20字节  
            
        # 验证参数范围    
        if 0 <= level <= 9:    
            compression_level = level    
        if -15 <= window_bits_raw <= 15:    
            window_bits = window_bits_raw    
        if 1 <= mem_level_raw <= 9:  
            mem_level = mem_level_raw    
        if 0 <= strategy_raw <= 4:    
            strategy = strategy_raw    
        
    if src_start + src_len > len(source_data):    
        raise ValueError(f"Source parameters out of range: start={src_start}, len={src_len}")    
        
    if patch_offset >= len(patch_data):    
        raise ValueError(f"Patch offset {patch_offset} out of range")    
        
    compressed_source = source_data[src_start:src_start + src_len]    
        
    # 解压缩源数据    
    try:    
        if chunk_type == CHUNK_GZIP:    
            uncompressed_source = zlib.decompress(compressed_source, 16 + zlib.MAX_WBITS)    
        else:  # CHUNK_DEFLATE    
            try:    
                uncompressed_source = zlib.decompress(compressed_source, window_bits)    
            except zlib.error:    
                uncompressed_source = zlib.decompress(compressed_source, -zlib.MAX_WBITS)    
    except zlib.error as e:    
        raise ValueError(f"Failed to decompress chunk {chunk_idx}: {e}")    
        
    # 处理 bonus data (匹配C++实现)  
    if bonus_data and expanded_len > len(uncompressed_source):  
        bonus_size = expanded_len - len(uncompressed_source)  
        if bonus_size <= len(bonus_data):  
            uncompressed_source = bytearray(uncompressed_source)  
            uncompressed_source.extend(bonus_data[:bonus_size])  
            uncompressed_source = bytes(uncompressed_source)  
            logger.debug(f"Applied {bonus_size} bytes of bonus data to chunk {chunk_idx}")  
  
    # 应用 bsdiff 补丁    
    chunk_patch = patch_data[patch_offset:]    
    patched_uncompressed = apply_bsdiff_patch(uncompressed_source, chunk_patch)    
        
    # 重新压缩    
    try:    
        if chunk_type == CHUNK_GZIP:    
            result = zlib.compress(patched_uncompressed, level=compression_level, wbits=16 + zlib.MAX_WBITS)    
        else:  # CHUNK_DEFLATE    
            # 修复：使用正确的zlib.compressobj调用  
            compressor = zlib.compressobj(level=compression_level, wbits=window_bits,    
                                        memLevel=mem_level, strategy=strategy)    
            result = compressor.compress(patched_uncompressed) + compressor.flush()    
    except zlib.error as e:    
        raise ValueError(f"Failed to recompress chunk {chunk_idx}: {e}")    
        
    return result, pos  
  
def sanitize_filename(filename: str) -> str:  
    """Sanitize filename for cross-platform compatibility"""  
    # 使用Path对象处理路径分隔符  
    path = Path(filename)  
    # 替换不安全字符  
    safe_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-')  
    sanitized = ''.join(c if c in safe_chars else '_' for c in path.name)  
    return sanitized  
  
async def generate_target_async(source_file: FileContents, patch_data: bytes,  
                               target_filename: str, target_sha1: bytes,  
                               target_size: int, bonus_data: bytes = None) -> bool:  
    """Async version with proper event loop handling"""  
      
    # 检查是否为分区目标，转换为文件写入  
    if target_filename.startswith(('MTD:', 'EMMC:')):  
        parts = target_filename.split(':')  
        if len(parts) >= 2:  
            partition_name = sanitize_filename(parts[1])  
            target_filename = f"patched_{partition_name}.img"  
            print(f"Converting partition target to file: {target_filename}")  
      
    retry_count = 2  
    for attempt in range(retry_count):  
        try:  
            # 使用 get_running_loop() 而不是 get_event_loop()  
            try:  
                loop = asyncio.get_running_loop()  
            except RuntimeError:  
                loop = asyncio.new_event_loop()  
                asyncio.set_event_loop(loop)  
              
            # 异步应用补丁
            patch_data = ensure_bytes(patch_data)  
            if patch_data.startswith(b'BSDIFF40'):  
                if len(source_file.data) > 10 * 1024 * 1024:  # 10MB以上  
                    try:  
                        with ProcessPoolExecutor() as executor:  
                            patched_data = await loop.run_in_executor(  
                                executor, apply_bsdiff_patch, source_file.data, patch_data  
                            )  
                    except (OSError, MemoryError) as e:  
                        logger.warning(f"Process pool execution failed: {e}")  
                        if attempt < retry_count - 1:  
                            patched_data = apply_bsdiff_patch(source_file.data, patch_data)  
                        else:  
                            return False  
                else:  
                    patched_data = apply_bsdiff_patch(source_file.data, patch_data)  
            elif patch_data.startswith(b'IMGDIFF2'):  
                try:  
                    patched_data = apply_imgdiff_patch_streaming(source_file.data, patch_data, bonus_data)  
                except (ValueError, zlib.error) as e:  
                    logger.error(f"ImgDiff patch failed: {e}", exc_info=True)  
                    if attempt < retry_count - 1:  
                        continue  
                    return False  
            else:  
                logger.error("Unknown patch format")  
                return False  
              
            # 验证大小和SHA1  
            if len(patched_data) != target_size:  
                logger.error(f"Size mismatch: expected {target_size}, got {len(patched_data)}")  
                if attempt < retry_count - 1:  
                    continue  
                return False  
              
            actual_sha1 = await loop.run_in_executor(  
                None, lambda: hashlib.sha1(patched_data, usedforsecurity=False).digest()  
            )  
              
            if actual_sha1 != target_sha1:  
                logger.error(f"SHA1 mismatch: expected {target_sha1.hex()}, got {actual_sha1.hex()}")  
                if attempt < retry_count - 1:  
                    continue  
                return False  
              
            # 原子写入文件  
            await loop.run_in_executor(None, write_file_atomic, target_filename + '.patch', patched_data, source_file)  
            return True  
              
        except Exception as e:  
            logger.error(f"Error during patch application: {e}", exc_info=True)  
            if attempt < retry_count - 1:  
                continue  
            return False  
      
    return False
  
def write_file_atomic(temp_filename: str, data: bytes, source_file: FileContents):  
    """Enhanced atomic file write with proper error handling"""  
    try:  
        # 确保目录存在  
        Path(temp_filename).parent.mkdir(parents=True, exist_ok=True)  
          
        # 写入临时文件  
        with open(temp_filename, 'wb') as f:  
            f.write(data)  
            f.flush()  
            os.fsync(f.fileno())  
          
        # 设置文件权限（在重命名前完成）  
        try:  
            os.chmod(temp_filename, source_file.st_mode)  
            if hasattr(os, 'chown') and os.name != 'nt':  
                os.chown(temp_filename, source_file.st_uid, source_file.st_gid)  
        except (OSError, PermissionError) as e:  
            logger.warning(f"Failed to set file permissions: {e}")  
          
        # 原子重命名  
        target_filename = temp_filename.replace('.patch', '')  
        if os.name == 'nt':  
            if os.path.exists(target_filename):  
                try:  
                    os.unlink(target_filename)  
                except OSError as e:  
                    backup_name = target_filename + '.backup'  
                    os.rename(target_filename, backup_name)  
                    logger.info(f"Backed up existing file to {backup_name}")  
          
        os.rename(temp_filename, target_filename)  
          
    except Exception as e:  
        # 清理临时文件  
        if os.path.exists(temp_filename):  
            try:  
                os.unlink(temp_filename)  
            except OSError:  
                pass  
        raise e

  
def generate_target_with_retry(source_file: FileContents, patch_data: bytes,  
                              target_filename: str, target_sha1: bytes,  
                              target_size: int, bonus_data: bytes = None) -> bool:  
    """Synchronous wrapper for async target generation"""  
    try:  
        return asyncio.run(generate_target_async(source_file, patch_data, target_filename,  
                                               target_sha1, target_size, bonus_data))  
    except Exception as e:  
        logger.warning(f"Async execution failed, falling back to sync: {e}")  
        # 回退到同步版本  
        return generate_target_sync(source_file, patch_data, target_filename,  
                                  target_sha1, target_size, bonus_data)  
  
def generate_target_sync(source_file: FileContents, patch_data: bytes,  
                        target_filename: str, target_sha1: bytes,  
                        target_size: int, bonus_data: bytes = None) -> bool:  
    """Synchronous fallback version with enhanced error handling"""  
    # 检查是否为分区目标，转换为文件写入  
    if target_filename.startswith(('MTD:', 'EMMC:')):  
        parts = target_filename.split(':')  
        if len(parts) >= 2:  
            partition_name = sanitize_filename(parts[1])  
            target_filename = f"patched_{partition_name}.img"  
            print(f"Converting partition target to file: {target_filename}")  
      
    retry_count = 2  
    for attempt in range(retry_count):  
        try:  
            # 应用补丁
            patch_data = ensure_bytes(patch_data)  
            if patch_data.startswith(b'BSDIFF40'):  
                patched_data = apply_bsdiff_patch(source_file.data, patch_data)  
            elif patch_data.startswith(b'IMGDIFF2'):  
                patched_data = apply_imgdiff_patch_streaming(source_file.data, patch_data, bonus_data)  
            else:  
                logger.error("Unknown patch format")  
                return False  
              
            # 验证大小  
            if len(patched_data) != target_size:  
                logger.error(f"Size mismatch: expected {target_size}, got {len(patched_data)}")  
                if attempt < retry_count - 1:  
                    logger.info("Retrying...")  
                    continue  
                return False  
              
            # 验证SHA1  
            actual_sha1 = hashlib.sha1(patched_data, usedforsecurity=False).digest()  
            if actual_sha1 != target_sha1:  
                logger.error(f"SHA1 mismatch: expected {target_sha1.hex()}, got {actual_sha1.hex()}")  
                if attempt < retry_count - 1:  
                    logger.info("Retrying...")  
                    continue  
                return False  
              
            # 原子写入文件  
            try:  
                write_file_atomic(target_filename, patched_data, source_file)  
                return True  
              
            except (OSError, PermissionError, IOError) as e:  
                logger.error(f"Failed to write target file: {e}", exc_info=True)  
                if attempt < retry_count - 1:  
                    logger.info("Retrying...")  
                    continue  
                return False  
          
        except (ValueError, RuntimeError) as e:  
            logger.error(f"Patch application failed: {e}", exc_info=True)  
            if attempt < retry_count - 1:  
                logger.info("Retrying...")  
                continue  
            return False  
        except Exception as e:  
            logger.error(f"Unexpected error: {e}", exc_info=True)  
            if attempt < retry_count - 1:  
                logger.info("Retrying...")  
                continue  
            return False  
      
    return False  
  
def save_file_contents(filename: str, file_contents: FileContents) -> bool:
    try:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'wb') as f:
            f.write(file_contents.data)
        try:
            if os.name != 'nt':
                os.chmod(filename, file_contents.st_mode)
                if hasattr(os, 'chown'):
                    os.chown(filename, file_contents.st_uid, file_contents.st_gid)
        except Exception as e:
            logger.warning(f"Failed to set file permissions for {filename}: {e}")
        return True
    except Exception as e:
        logger.error(f"Failed to save file {filename}: {e}", exc_info=True)
        return False
  
def applypatch_check(filename: str, patch_sha1_list: List[str]) -> bool:  
    """Check if file matches any of the expected SHA1 sums"""  
    try:  
        file_contents = load_file_contents(filename)  
        if not patch_sha1_list:  
            return True  # No SHA1s to check  
          
        return find_matching_patch(file_contents.sha1, patch_sha1_list) >= 0  
    except (FileNotFoundError, OSError, IOError):  
        return False  
  
def applypatch_with_cache(source_filename: str, target_filename: str,  
                         target_sha1_str: str, target_size: int,  
                         patch_sha1_list: List[str], patch_files: List[str],  
                         bonus_data: bytes = None) -> int:  
    """Enhanced applypatch with robust error handling"""  
      
    with cache_manager() as cache_dir:  
        print(f"patch {source_filename}:")  
          
        if target_filename == "-":  
            target_filename = source_filename  
          
        try:  
            target_sha1 = parse_sha1(target_sha1_str)  
        except (ValueError, TypeError) as e:  
            logger.error(f"Failed to parse target SHA1: {e}", exc_info=True)  
            return 1  
          
        # 检查目标文件是否已经正确  
        try:  
            if os.path.exists(target_filename):  
                target_contents = load_file_contents(target_filename)  
                if target_contents.sha1 == target_sha1:  
                    print(f"already {short_sha1(target_sha1)}")  
                    return 0  
        except (FileNotFoundError, OSError, IOError):  
            pass  
          
        # 加载源文件  
        source_file = None  
        try:  
            source_file = load_file_contents(source_filename)  
        except (FileNotFoundError, OSError, IOError) as e:  
            logger.error(f"Failed to load source file: {e}", exc_info=True)  
          
        # 查找匹配的补丁  
        patch_index = -1  
        if source_file:  
            try:  
                patch_index = find_matching_patch(source_file.sha1, patch_sha1_list)  
            except (TypeError, ValueError) as e:  
                logger.error(f"Error finding matching patch: {e}", exc_info=True)  
          
        # 尝试从缓存加载  
        if patch_index < 0:  
            print("source file is bad; trying copy")  
            try:  
                if CACHE_TEMP_SOURCE.exists():  
                    copy_file = load_file_contents(str(CACHE_TEMP_SOURCE))  
                    patch_index = find_matching_patch(copy_file.sha1, patch_sha1_list)  
                    if patch_index >= 0:  
                        source_file = copy_file  
                        print("using cached source file")  
            except (FileNotFoundError, OSError, IOError, TypeError, ValueError):  
                pass  
          
        if patch_index < 0:  
            print("copy file doesn't match source SHA-1s either")  
            return 1  
          
        # 备份源文件到缓存  
        try:  
            CACHE_TEMP_SOURCE.parent.mkdir(parents=True, exist_ok=True)  
            save_file_contents(str(CACHE_TEMP_SOURCE), source_file)  
        except (OSError, IOError) as e:  
            logger.warning(f"Warning: failed to back up source file: {e}")  
          
        # 加载补丁数据（确保二进制模式）  
        try:  
            with open(patch_files[patch_index], 'rb') as f:  
                patch_data = f.read()  
        except (FileNotFoundError, OSError, IOError) as e:  
            logger.error(f"Failed to load patch file: {e}", exc_info=True)  
            return 1  
          
        # 应用补丁  
        if generate_target_with_retry(source_file, patch_data, target_filename,  
                                     target_sha1, target_size, bonus_data):  
            print(f"now {short_sha1(target_sha1)}")  
            return 0  
        else:  
            return 1
  
def check_python_version():
    if sys.version_info < (3, 8):
        print("Warning: This script is designed for Python 3.8+")
        print(f"Current version: {sys.version}")
    elif sys.version_info >= (3, 13):
        print(f"Running on Python {sys.version_info.major}.{sys.version_info.minor}")

  
def main():  
    """Main function with enhanced error handling and resource management"""  
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
          
        # Load bonus data - 确保使用二进制模式  
        try:  
            with open(bonus_filename, 'rb') as f:  
                bonus_data = f.read()  
        except (FileNotFoundError, OSError, IOError) as e:  
            logger.error(f"Failed to load bonus file: {e}", exc_info=True)  
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
      
    # Apply the patch using the enhanced function with cache management  
    result = applypatch_with_cache(source_filename, target_filename, target_sha1_str,  
                                  target_size, patch_sha1_list, patch_files, bonus_data)  
      
    if result == 0:  
        print("Done")  
    else:  
        print(f"Done with error code: {result}")  
      
    return result  
  
if __name__ == "__main__":  
    check_python_version()  
    sys.exit(main())

